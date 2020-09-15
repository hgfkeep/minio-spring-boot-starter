package win.hgfdodo.minio.service;

import io.minio.*;
import io.minio.errors.*;
import io.minio.http.Method;
import io.minio.messages.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import win.hgfdodo.minio.connection.MinioConnectionFactory;
import win.hgfdodo.minio.exception.MinioBadRequestException;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.security.InvalidKeyException;
import java.security.NoSuchAlgorithmException;
import java.time.ZonedDateTime;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

/**
 * object operator
 *
 * @author Guangfu He
 * @date 2020/9/14 14:19
 * @email hgfkeep@gmail.com
 */
public class ObjectOps {
    private final static Logger log = LoggerFactory.getLogger(ObjectOps.class);

    private final MinioConnectionFactory minioConnectionFactory;

    private String region;
    private Map<String, String> headers;
    private Map<String, String> extraHeaders;
    private Map<String, String> extraQueryParams;
    private Map<String, String> userMetadata;
    private ServerSideEncryption serverSideEncryption;
    private Tags tags;
    private ServerSideEncryptionCustomerKey ssec;

    /**
     * Set bucket server side encryption to operate
     *
     * @param serverSideEncryption
     * @return
     */
    public ObjectOps sse(ServerSideEncryption serverSideEncryption) {
        this.serverSideEncryption = serverSideEncryption;
        return this;
    }

    /**
     * Set bucket region to operate
     *
     * @param region
     * @return
     */
    public ObjectOps region(String region) {
        this.region = region;
        return this;
    }

    public ObjectOps headers(Map<String, String> headers) {
        this.headers = headers;
        return this;
    }

    public ObjectOps extraHeaders(Map<String, String> extraHeaders) {
        this.extraHeaders = extraHeaders;
        return this;
    }

    public ObjectOps extraQueryParams(Map<String, String> extraQueryParams) {
        this.extraQueryParams = extraQueryParams;
        return this;
    }

    public ObjectOps userMetadata(Map<String, String> userMetadata) {
        this.userMetadata = userMetadata;
        return this;
    }

    public ObjectOps(MinioConnectionFactory minioConnectionFactory) {
        this.minioConnectionFactory = minioConnectionFactory;
    }

    public MinioClient connection() {
        return minioConnectionFactory.getConnection();
    }

    /**
     * Creates an object by combining data from different source objects using server-side copy.
     */
    public ObjectWriteResponse composeObject(String bucketName, String mergedObjectName, List<ComposeSource> composeSources) throws IOException, InvalidKeyException, InvalidResponseException, InsufficientDataException, NoSuchAlgorithmException, ServerException, InternalException, XmlParserException, InvalidBucketNameException, ErrorResponseException {
        return connection().composeObject(
                ComposeObjectArgs.builder()
                        .bucket(bucketName)
                        .region(region)
                        .headers(headers)
                        .extraHeaders(extraHeaders)
                        .extraQueryParams(extraQueryParams)
                        .tags(tags)
                        .sse(serverSideEncryption)
                        .userMetadata(userMetadata)
                        .sources(composeSources)
                        .object(mergedObjectName)
                        .build());
    }

    public void copyObject(String destBucket, String destObjectName, String srcBucket, String srcObjectName, Directive taggingDirective, Directive metadataDirective) throws IOException, InvalidKeyException, InvalidResponseException, InsufficientDataException, NoSuchAlgorithmException, ServerException, InternalException, XmlParserException, InvalidBucketNameException, ErrorResponseException {
        CopySource copySource = CopySource.builder().bucket(srcBucket).object(srcObjectName).build();
        copyObject(destBucket, destObjectName, copySource, taggingDirective, metadataDirective);
    }

    public void copyObject(String destBucket, String destObjectName, CopySource copySource, Directive taggingDirective, Directive metadataDirective) throws IOException, InvalidKeyException, InvalidResponseException, InsufficientDataException, NoSuchAlgorithmException, ServerException, InternalException, XmlParserException, InvalidBucketNameException, ErrorResponseException {
        connection().copyObject(
                CopyObjectArgs.builder()
                        .bucket(destBucket)
                        .object(destObjectName)
                        .region(region)
                        .headers(headers)
                        .extraHeaders(extraHeaders)
                        .extraQueryParams(extraQueryParams)
                        .tags(tags)
                        .sse(serverSideEncryption)
                        .userMetadata(userMetadata)
                        .source(copySource)
                        .taggingDirective(taggingDirective)
                        .metadataDirective(metadataDirective)
                        .build());
    }

    /**
     * Deletes tags of an object.
     */
    public void deleteObjectTags(String bucket, String objectName) throws IOException, InvalidKeyException, InvalidResponseException, InsufficientDataException, NoSuchAlgorithmException, ServerException, InternalException, XmlParserException, InvalidBucketNameException, ErrorResponseException {
        connection().deleteObjectTags(DeleteObjectTagsArgs.builder()
                .bucket(bucket)
                .region(region)
                .extraHeaders(extraHeaders)
                .extraQueryParams(extraQueryParams)
                .object(objectName)
                .build());
    }

    /**
     * Disables legal hold on an object.
     */
    public void disableObjectLegalHold(String bucket, String objectName) throws IOException, InvalidKeyException, InvalidResponseException, InsufficientDataException, NoSuchAlgorithmException, ServerException, InternalException, XmlParserException, InvalidBucketNameException, ErrorResponseException {
        connection().disableObjectLegalHold(DisableObjectLegalHoldArgs.builder()
                .bucket(bucket)
                .region(region)
                .extraHeaders(extraHeaders)
                .extraQueryParams(extraQueryParams)
                .object(objectName)
                .build());
    }

    public void downloadObject(String bucket, String objectName, String filename) throws IOException, InvalidKeyException, InvalidResponseException, InsufficientDataException, NoSuchAlgorithmException, ServerException, InternalException, XmlParserException, InvalidBucketNameException, ErrorResponseException {
        connection().downloadObject(DownloadObjectArgs.builder()
                .bucket(bucket)
                .object(objectName)
                .region(region)
                .extraHeaders(extraHeaders)
                .extraQueryParams(extraQueryParams)
                .ssec(ssec)
                .filename(filename)
                .build());
    }

    public void downloadVersionedObject(String bucket, String objectName, String filename, String versionId) throws IOException, InvalidKeyException, InvalidResponseException, InsufficientDataException, NoSuchAlgorithmException, ServerException, InternalException, XmlParserException, InvalidBucketNameException, ErrorResponseException {
        connection().downloadObject(DownloadObjectArgs.builder()
                .bucket(bucket)
                .object(objectName)
                .region(region)
                .extraHeaders(extraHeaders)
                .extraQueryParams(extraQueryParams)
                .ssec(ssec)
                .versionId(versionId)
                .filename(filename)
                .build());
    }

    public void enableVersionedObjectLegalHold(String bucket, String objectName, String versionId) throws IOException, InvalidKeyException, InvalidResponseException, InsufficientDataException, NoSuchAlgorithmException, ServerException, InternalException, XmlParserException, InvalidBucketNameException, ErrorResponseException {
        connection().enableObjectLegalHold(EnableObjectLegalHoldArgs.builder()
                .bucket(bucket)
                .object(objectName)
                .region(region)
                .extraHeaders(extraHeaders)
                .extraQueryParams(extraQueryParams)
                .versionId(versionId)
                .build());
    }

    public void enableObjectLegalHold(String bucket, String objectName) throws IOException, InvalidKeyException, InvalidResponseException, InsufficientDataException, NoSuchAlgorithmException, ServerException, InternalException, XmlParserException, InvalidBucketNameException, ErrorResponseException {
        connection().enableObjectLegalHold(EnableObjectLegalHoldArgs.builder()
                .bucket(bucket)
                .object(objectName)
                .region(region)
                .extraHeaders(extraHeaders)
                .extraQueryParams(extraQueryParams)
                .build());
    }

    /**
     * Gets data of an object.
     * Returned InputStream must be closed after use to release network resources.
     */
    public InputStream getObject(String bucket, String objectName) throws IOException, InvalidKeyException, InvalidResponseException, InsufficientDataException, NoSuchAlgorithmException, ServerException, InternalException, XmlParserException, InvalidBucketNameException, ErrorResponseException {
        return connection().getObject(GetObjectArgs.builder()
                .bucket(bucket)
                .object(objectName)
                .region(region)
                .extraHeaders(extraHeaders)
                .extraQueryParams(extraQueryParams)
                .ssec(ssec)
                .build());
    }


    /**
     * Gets data of an object.
     * Returned InputStream must be closed after use to release network resources.
     */
    public InputStream getObject(String bucket, String objectName, String versionId) throws IOException, InvalidKeyException, InvalidResponseException, InsufficientDataException, NoSuchAlgorithmException, ServerException, InternalException, XmlParserException, InvalidBucketNameException, ErrorResponseException {
        return connection().getObject(GetObjectArgs.builder()
                .bucket(bucket)
                .object(objectName)
                .versionId(versionId)
                .region(region)
                .extraHeaders(extraHeaders)
                .extraQueryParams(extraQueryParams)
                .ssec(ssec)
                .build());
    }

    /**
     * Gets data of an object from offset
     * Returned InputStream must be closed after use to release network resources.
     */
    public InputStream getObject(String bucket, String objectName, long offset) throws IOException, InvalidKeyException, InvalidResponseException, InsufficientDataException, NoSuchAlgorithmException, ServerException, InternalException, XmlParserException, InvalidBucketNameException, ErrorResponseException {
        return connection().getObject(GetObjectArgs.builder()
                .bucket(bucket)
                .object(objectName)
                .region(region)
                .extraHeaders(extraHeaders)
                .extraQueryParams(extraQueryParams)
                .ssec(ssec)
                .offset(offset)
                .build());
    }


    /**
     * Gets some part data of an object from offset with length
     * Returned InputStream must be closed after use to release network resources.
     */
    public InputStream getObjectPart(String bucket, String objectName, long offset, long length) throws IOException, InvalidKeyException, InvalidResponseException, InsufficientDataException, NoSuchAlgorithmException, ServerException, InternalException, XmlParserException, InvalidBucketNameException, ErrorResponseException {
        return connection().getObject(GetObjectArgs.builder()
                .bucket(bucket)
                .object(objectName)
                .region(region)
                .extraHeaders(extraHeaders)
                .extraQueryParams(extraQueryParams)
                .ssec(ssec)
                .offset(offset)
                .length(length)
                .build());
    }

    /**
     * Gets some part data of an object from offset with length
     * Returned InputStream must be closed after use to release network resources.
     */
    public InputStream getVersionedObjectPart(String bucket, String objectName, String versionId, long offset, long length) throws IOException, InvalidKeyException, InvalidResponseException, InsufficientDataException, NoSuchAlgorithmException, ServerException, InternalException, XmlParserException, InvalidBucketNameException, ErrorResponseException {
        return connection().getObject(GetObjectArgs.builder()
                .bucket(bucket)
                .object(objectName)
                .region(region)
                .extraHeaders(extraHeaders)
                .extraQueryParams(extraQueryParams)
                .ssec(ssec)
                .versionId(versionId)
                .offset(offset)
                .length(length)
                .build());
    }

    public Retention getObjectRetention(String bucket, String objectName) throws IOException, InvalidKeyException, InvalidResponseException, InsufficientDataException, NoSuchAlgorithmException, ServerException, InternalException, XmlParserException, InvalidBucketNameException, ErrorResponseException {
        return connection().getObjectRetention(GetObjectRetentionArgs.builder()
                .bucket(bucket)
                .object(objectName)
                .region(region)
                .extraHeaders(extraHeaders)
                .extraQueryParams(extraQueryParams)
                .build());
    }

    public Retention getVersionedObjectRetention(String bucket, String objectName, String versionId) throws IOException, InvalidKeyException, InvalidResponseException, InsufficientDataException, NoSuchAlgorithmException, ServerException, InternalException, XmlParserException, InvalidBucketNameException, ErrorResponseException {
        return connection().getObjectRetention(GetObjectRetentionArgs.builder()
                .bucket(bucket)
                .object(objectName)
                .versionId(versionId)
                .region(region)
                .extraHeaders(extraHeaders)
                .extraQueryParams(extraQueryParams)
                .build());
    }

    public Tags getObjectTags(String bucket, String objectName) throws IOException, InvalidKeyException, InvalidResponseException, InsufficientDataException, NoSuchAlgorithmException, ServerException, InternalException, XmlParserException, InvalidBucketNameException, ErrorResponseException {
        return connection().getObjectTags(GetObjectTagsArgs.builder()
                .bucket(bucket)
                .object(objectName)
                .region(region)
                .extraHeaders(extraHeaders)
                .extraQueryParams(extraQueryParams)
                .build());
    }

    public Tags getObjectTags(String bucket, String objectName, String versionId) throws IOException, InvalidKeyException, InvalidResponseException, InsufficientDataException, NoSuchAlgorithmException, ServerException, InternalException, XmlParserException, InvalidBucketNameException, ErrorResponseException {
        return connection().getObjectTags(GetObjectTagsArgs.builder()
                .bucket(bucket)
                .object(objectName)
                .versionId(versionId)
                .region(region)
                .extraHeaders(extraHeaders)
                .extraQueryParams(extraQueryParams)
                .build());
    }

    /**
     * Gets URL of an object, useful when this object has public read access.
     */
    public String getObjectUrl(String bucket, String objectName) throws IOException, InvalidKeyException, InvalidResponseException, InsufficientDataException, NoSuchAlgorithmException, ServerException, InternalException, XmlParserException, InvalidBucketNameException, ErrorResponseException {
        return connection().getObjectUrl(bucket, objectName);
    }


    /**
     * Gets presigned URL of an object for HTTP method, expiry time and custom request parameters.
     *
     * @return
     */
    public String getPresignedObjectUrl(String bucket, String objectName, Method method, int expirySeconds) throws IOException, InvalidKeyException, InvalidResponseException, InsufficientDataException, InvalidExpiresRangeException, ServerException, InternalException, NoSuchAlgorithmException, XmlParserException, InvalidBucketNameException, ErrorResponseException {
        return connection().getPresignedObjectUrl(GetPresignedObjectUrlArgs.builder()
                .bucket(bucket)
                .object(objectName)
                .region(region)
                .extraHeaders(extraHeaders)
                .extraQueryParams(extraQueryParams)
                .method(method)
                .expiry(expirySeconds)
                .build());
    }

    /**
     * Gets presigned URL of an object for HTTP method, expiry time and custom request parameters.
     *
     * @return
     */
    public String getVersionedPresignedObjectUrl(String bucket, String objectName, String versionId, Method method, int expirySeconds) throws IOException, InvalidKeyException, InvalidResponseException, InsufficientDataException, InvalidExpiresRangeException, ServerException, InternalException, NoSuchAlgorithmException, XmlParserException, InvalidBucketNameException, ErrorResponseException {
        return connection().getPresignedObjectUrl(GetPresignedObjectUrlArgs.builder()
                .bucket(bucket)
                .object(objectName)
                .region(region)
                .extraHeaders(extraHeaders)
                .extraQueryParams(extraQueryParams)
                .versionId(versionId)
                .method(method)
                .expiry(expirySeconds)
                .build());
    }

    public boolean isObjectLegalHoldEnabled(String bucket, String objectName) throws IOException, InvalidKeyException, InvalidResponseException, InsufficientDataException, NoSuchAlgorithmException, ServerException, InternalException, XmlParserException, InvalidBucketNameException, ErrorResponseException {
        return connection().isObjectLegalHoldEnabled(IsObjectLegalHoldEnabledArgs.builder()
                .bucket(bucket)
                .object(objectName)
                .region(region)
                .extraHeaders(extraHeaders)
                .extraQueryParams(extraQueryParams)
                .build());
    }

    public boolean isVersionedObjectLegalHoldEnabled(String bucket, String objectName, String versionId) throws IOException, InvalidKeyException, InvalidResponseException, InsufficientDataException, NoSuchAlgorithmException, ServerException, InternalException, XmlParserException, InvalidBucketNameException, ErrorResponseException {
        return connection().isObjectLegalHoldEnabled(IsObjectLegalHoldEnabledArgs.builder()
                .bucket(bucket)
                .object(objectName)
                .versionId(versionId)
                .region(region)
                .extraHeaders(extraHeaders)
                .extraQueryParams(extraQueryParams)
                .build());
    }

    public List<Item> listAllObjects(String bucket) throws IOException, InvalidKeyException, InvalidResponseException, InsufficientDataException, NoSuchAlgorithmException, ServerException, InternalException, XmlParserException, InvalidBucketNameException, ErrorResponseException {
        List<Item> items = new ArrayList<>();
        Iterable<Result<Item>> iterable = connection().listObjects(ListObjectsArgs.builder()
                .bucket(bucket)
                .region(region)
                .extraHeaders(extraHeaders)
                .extraQueryParams(extraQueryParams)
                .build());
        for (Result<Item> itemResult : iterable) {
            items.add(itemResult.get());
        }
        return items;
    }

    public List<Item> listAllObjectsExtra(String bucket, boolean fetchOwner, boolean includeUserMetadata, boolean includeVersions) throws IOException, InvalidKeyException, InvalidResponseException, InsufficientDataException, NoSuchAlgorithmException, ServerException, InternalException, XmlParserException, InvalidBucketNameException, ErrorResponseException {
        List<Item> items = new ArrayList<>();
        Iterable<Result<Item>> iterable = connection().listObjects(ListObjectsArgs.builder()
                .bucket(bucket)
                .region(region)
                .extraHeaders(extraHeaders)
                .extraQueryParams(extraQueryParams)
                .fetchOwner(fetchOwner)
                .includeUserMetadata(includeUserMetadata)
                .includeVersions(includeVersions)
                .build());
        for (Result<Item> itemResult : iterable) {
            items.add(itemResult.get());
        }
        return items;
    }

    public List<Item> listAllObjectsRecursive(String bucket) throws IOException, InvalidKeyException, InvalidResponseException, InsufficientDataException, NoSuchAlgorithmException, ServerException, InternalException, XmlParserException, InvalidBucketNameException, ErrorResponseException {
        List<Item> items = new ArrayList<>();
        Iterable<Result<Item>> iterable = connection().listObjects(ListObjectsArgs.builder()
                .bucket(bucket)
                .region(region)
                .extraHeaders(extraHeaders)
                .extraQueryParams(extraQueryParams)
                .fetchOwner(true)
                .includeUserMetadata(true)
                .includeVersions(true)
                .build());
        for (Result<Item> itemResult : iterable) {
            items.add(itemResult.get());
        }
        return items;
    }

    public void presignedPostPolicy() {
    }

    public ObjectWriteResponse putObject(String bucketName, String objectName, InputStream stream, long objectSize, long partSize, String contentType) throws IOException, InvalidKeyException, InvalidResponseException, InsufficientDataException, NoSuchAlgorithmException, ServerException, InternalException, XmlParserException, InvalidBucketNameException, ErrorResponseException {
        return connection().putObject(PutObjectArgs.builder()
                .bucket(bucketName)
                .object(objectName)
                .sse(serverSideEncryption)
                .userMetadata(userMetadata)
                .headers(headers)
                .contentType(contentType)
                .stream(stream, objectSize, partSize)
                .region(region)
                .extraHeaders(extraHeaders)
                .extraQueryParams(extraQueryParams)
                .build());
    }

    public ObjectWriteResponse mkdir(String bucketName, String objectName, InputStream stream, long objectSize, long partSize, String contentType) throws IOException, InvalidKeyException, InvalidResponseException, InsufficientDataException, NoSuchAlgorithmException, ServerException, InternalException, XmlParserException, InvalidBucketNameException, ErrorResponseException, MinioBadRequestException {
        if (!objectName.endsWith("/")) {
            throw new MinioBadRequestException("folder or directory object name MUST end with '/'");
        }
        return putObject(bucketName, objectName, new ByteArrayInputStream(new byte[]{}), 0, -1, null);
    }

    public void putKnownSizeObject(String bucketName, String objectName, InputStream stream, long objectSize, String contentType) throws IOException, InvalidResponseException, InvalidKeyException, NoSuchAlgorithmException, ServerException, ErrorResponseException, XmlParserException, InvalidBucketNameException, InsufficientDataException, InternalException {
        putObject(bucketName, objectName, stream, objectSize, -1, contentType);
    }

    /**
     * save unknown size object
     */
    public void putUnknownSizeObject(String bucketName, String objectName, InputStream stream, long partSize, String contentType) throws IOException, InvalidResponseException, InvalidKeyException, NoSuchAlgorithmException, ServerException, ErrorResponseException, XmlParserException, InvalidBucketNameException, InsufficientDataException, InternalException {
        putObject(bucketName, objectName, stream, -1, partSize, contentType);
    }

    public void removeObject(String bucketName, String objectName) throws IOException, InvalidKeyException, InvalidResponseException, InsufficientDataException, NoSuchAlgorithmException, ServerException, InternalException, XmlParserException, InvalidBucketNameException, ErrorResponseException {
        connection().removeObject(RemoveObjectArgs.builder()
                .bucket(bucketName)
                .object(objectName)
                .region(region)
                .extraHeaders(extraHeaders)
                .extraQueryParams(extraQueryParams)
                .build());
    }

    public void removeVersionedObject(String bucketName, String objectName, String versionId) throws IOException, InvalidKeyException, InvalidResponseException, InsufficientDataException, NoSuchAlgorithmException, ServerException, InternalException, XmlParserException, InvalidBucketNameException, ErrorResponseException {
        connection().removeObject(RemoveObjectArgs.builder()
                .bucket(bucketName)
                .object(objectName)
                .region(region)
                .extraHeaders(extraHeaders)
                .extraQueryParams(extraQueryParams)
                .versionId(versionId)
                .build());
    }

    public void removeObject(String bucketName, String objectName, boolean bypassGovernanceMode) throws IOException, InvalidKeyException, InvalidResponseException, InsufficientDataException, NoSuchAlgorithmException, ServerException, InternalException, XmlParserException, InvalidBucketNameException, ErrorResponseException {
        connection().removeObject(RemoveObjectArgs.builder()
                .bucket(bucketName)
                .object(objectName)
                .region(region)
                .extraHeaders(extraHeaders)
                .extraQueryParams(extraQueryParams)
                .bypassGovernanceMode(bypassGovernanceMode)
                .build());
    }

    /**
     * remove objects in bucket
     *
     * @param bucketName
     * @param objectNames
     * @return failed remove objectNames
     */
    public List<String> removeObjects(String bucketName, List<String> objectNames) throws IOException, InvalidKeyException, InvalidResponseException, InsufficientDataException, NoSuchAlgorithmException, ServerException, InternalException, XmlParserException, InvalidBucketNameException, ErrorResponseException {
        List<DeleteObject> objects = objectNames.stream().map(DeleteObject::new).collect(Collectors.toList());
        Iterable<Result<DeleteError>> results = minioConnectionFactory.getConnection().removeObjects(RemoveObjectsArgs.builder().bucket(bucketName).objects(objects).build());
        List<String> errorDeleteObjects = new ArrayList<>();
        for (Result<DeleteError> result : results) {
            errorDeleteObjects.add(result.get().objectName());
            log.error("Error in deleting object {}:{}, code={}, message={}", bucketName, result.get().objectName(), result.get().errorCode(), result.get().message());
        }
        return errorDeleteObjects;
    }

    /**
     * Selects content of a object by SQL expression.
     */
    public SelectResponseStream selectObjectContent(String bucketName, String objectName, String sql, InputSerialization is, OutputSerialization os) throws IOException, InvalidKeyException, InvalidResponseException, InsufficientDataException, NoSuchAlgorithmException, ServerException, InternalException, XmlParserException, InvalidBucketNameException, ErrorResponseException {
        return connection().selectObjectContent(
                SelectObjectContentArgs.builder()
                        .bucket(bucketName)
                        .object(objectName)
                        .sqlExpression(sql)
                        .inputSerialization(is)
                        .outputSerialization(os)
                        .requestProgress(true)
                        .build());
    }

    public void setObjectRetention(String bucketName, String objectName, RetentionMode retentionMode, ZonedDateTime retainUntilDate) throws IOException, InvalidKeyException, InvalidResponseException, InsufficientDataException, NoSuchAlgorithmException, ServerException, InternalException, XmlParserException, InvalidBucketNameException, ErrorResponseException {
        Retention retention = new Retention(retentionMode, retainUntilDate);
        connection().setObjectRetention(SetObjectRetentionArgs.builder()
                .bucket(bucketName)
                .object(objectName)
                .config(retention)
                .build());
    }

    public void setObjectTags(String bucketName, String objectName, Tags tags) throws IOException, InvalidKeyException, InvalidResponseException, InsufficientDataException, NoSuchAlgorithmException, ServerException, InternalException, XmlParserException, InvalidBucketNameException, ErrorResponseException {
        connection().setObjectTags(SetObjectTagsArgs.builder()
                .bucket(bucketName)
                .object(objectName)
                .region(region)
                .extraHeaders(extraHeaders)
                .extraQueryParams(extraQueryParams)
                .tags(tags)
                .build());
    }

    public void setObjectTags(String bucketName, String objectName, Map<String, String> tags) throws IOException, InvalidKeyException, InvalidResponseException, InsufficientDataException, NoSuchAlgorithmException, ServerException, InternalException, XmlParserException, InvalidBucketNameException, ErrorResponseException {
        connection().setObjectTags(SetObjectTagsArgs.builder()
                .bucket(bucketName)
                .object(objectName)
                .region(region)
                .extraHeaders(extraHeaders)
                .extraQueryParams(extraQueryParams)
                .tags(tags)
                .build());
    }

    public ObjectStat statObject(String bucketName, String objectName) throws IOException, InvalidKeyException, InvalidResponseException, InsufficientDataException, NoSuchAlgorithmException, ServerException, InternalException, XmlParserException, InvalidBucketNameException, ErrorResponseException {
        return connection().statObject(StatObjectArgs.builder()
                .bucket(bucketName)
                .object(objectName)
                .region(region)
                .extraHeaders(extraHeaders)
                .extraQueryParams(extraQueryParams)
                .ssec(ssec)
                .build());
    }

    public ObjectStat statVersionedObject(String bucketName, String objectName, String versionId) throws IOException, InvalidKeyException, InvalidResponseException, InsufficientDataException, NoSuchAlgorithmException, ServerException, InternalException, XmlParserException, InvalidBucketNameException, ErrorResponseException {
        return connection().statObject(StatObjectArgs.builder()
                .bucket(bucketName)
                .object(objectName)
                .region(region)
                .extraHeaders(extraHeaders)
                .extraQueryParams(extraQueryParams)
                .ssec(ssec)
                .versionId(versionId)
                .build());
    }

    public ObjectWriteResponse uploadObject(String bucketName, String objectName, String filename, String contentType) throws IOException, InvalidKeyException, InvalidResponseException, InsufficientDataException, NoSuchAlgorithmException, ServerException, InternalException, XmlParserException, InvalidBucketNameException, ErrorResponseException {
        return connection().uploadObject(UploadObjectArgs.builder()
                .bucket(bucketName)
                .object(objectName)
                .region(region)
                .extraHeaders(extraHeaders)
                .extraQueryParams(extraQueryParams)
                .sse(serverSideEncryption)
                .contentType(contentType)
                .filename(filename)
                .build());
    }
}
