package com.github.nepyh.rooter.module.storage.impl.s3

import aws.sdk.kotlin.services.s3.S3Client
import aws.sdk.kotlin.services.s3.model.DeleteObjectRequest
import aws.sdk.kotlin.services.s3.model.GetObjectRequest
import aws.sdk.kotlin.services.s3.model.HeadObjectRequest
import aws.sdk.kotlin.services.s3.model.NoSuchKey
import aws.sdk.kotlin.services.s3.model.NotFound
import aws.sdk.kotlin.services.s3.model.PutObjectRequest
import aws.sdk.kotlin.services.s3.presigners.presignGetObject
import aws.smithy.kotlin.runtime.content.asByteStream
import aws.smithy.kotlin.runtime.content.toInputStream as byteStreamToInputStream
import com.github.nepyh.rooter.module.storage.FileStorage
import io.ktor.http.Headers
import io.ktor.http.HttpHeaders
import io.ktor.http.content.PartData
import io.ktor.utils.io.jvm.javaio.toByteReadChannel
import io.ktor.utils.io.jvm.javaio.toInputStream
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.withContext
import java.util.UUID
import kotlin.time.Duration
import kotlin.time.Duration.Companion.minutes


class S3FileStorage(
    region: String,
    val bucket: String,
    val presignDuration: Duration = 15.minutes,
) : FileStorage {
    val s3 = S3Client {
        this.region = region
    }

    override suspend fun upload(file: PartData.FileItem, directory: String): String =
        withContext(Dispatchers.IO) {
            val originalName = file.originalFileName ?: "unknown_file"
            val fileExtension = originalName.substringAfterLast('.', "")
            val uniqueFileName = if (fileExtension.isNotEmpty()) {
                "${UUID.randomUUID()}.$fileExtension"
            } else {
                UUID.randomUUID().toString()
            }
            val key = if (directory.isEmpty()) uniqueFileName else "$directory/$uniqueFileName"

            val contentLength = file.headers[HttpHeaders.ContentLength]?.toLongOrNull()

            file.provider().toInputStream().use { inputStream ->
                s3.putObject(
                    PutObjectRequest {
                        bucket = this@S3FileStorage.bucket
                        this.key = key
                        body = inputStream.asByteStream(contentLength)
                        contentType = file.headers[HttpHeaders.ContentType]
                    }
                )
            }

            key
        }

    override suspend fun getFile(fileKey: String): PartData.FileItem? =
        withContext(Dispatchers.IO) {
            try {
                s3.getObject(
                    GetObjectRequest {
                        bucket = this@S3FileStorage.bucket
                        key = fileKey
                    }
                ) { response ->
                    val body = response.body
                    if (body == null) {
                        null
                    } else {
                        PartData.FileItem(
                            provider = { body.byteStreamToInputStream().toByteReadChannel() },
                            dispose = {},
                            partHeaders = Headers.build {
                                response.contentType?.let { append(HttpHeaders.ContentType, it) }
                                response.contentLength?.let { append(HttpHeaders.ContentLength, it.toString()) }
                            }
                        )
                    }
                }
            } catch (_: NoSuchKey) {
                null
            }
        }

    override suspend fun getUrl(fileKey: String): String? =
        withContext(Dispatchers.IO) {
            if (!exists(fileKey)) return@withContext null

            val presigned = s3.presignGetObject(
                GetObjectRequest {
                    bucket = this@S3FileStorage.bucket
                    key = fileKey
                },
                presignDuration,
            )
            presigned.url.toString()
        }

    override suspend fun delete(fileKey: String): Boolean =
        withContext(Dispatchers.IO) {
            if (!exists(fileKey)) return@withContext false

            s3.deleteObject(
                DeleteObjectRequest {
                    bucket = this@S3FileStorage.bucket
                    key = fileKey
                }
            )
            true
        }

    private suspend fun exists(fileKey: String): Boolean =
        try {
            s3.headObject(
                HeadObjectRequest {
                    bucket = this@S3FileStorage.bucket
                    key = fileKey
                }
            )
            true
        } catch (_: NoSuchKey) {
            false
        } catch (_: NotFound) {
            false
        }

    fun close() {
        s3.close()
    }
}
