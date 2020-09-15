package win.hgfdodo.minio.service;

import io.minio.*;
import io.minio.errors.*;
import io.minio.messages.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import win.hgfdodo.minio.connection.MinioConnectionFactory;

import java.io.IOException;
import java.security.InvalidKeyException;
import java.security.NoSuchAlgorithmException;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

/**
 * Bucket operator
 *
 * @author Guangfu He
 */
public class BucketOps {
    private Logger log = LoggerFactory.getLogger(BucketOps.class);

    private final MinioConnectionFactory minioConnectionFactory;

    private String bucket;
    private ServerSideEncryption serverSideEncryption;
    private String region;

    private Map<String, String> extraHeaders;

    private Map<String, String> extraQueryParams;

    public BucketOps(MinioConnectionFactory minioConnectionFactory) {
        this.minioConnectionFactory = minioConnectionFactory;
    }

    private BucketOps bucket(String bucketName) {
        this.bucket = bucketName;
        return this;
    }

    /**
     * Set bucket server side encryption to operate
     *
     * @param serverSideEncryption
     * @return
     */
    public BucketOps sse(ServerSideEncryption serverSideEncryption) {
        this.serverSideEncryption = serverSideEncryption;
        return this;
    }

    /**
     * Set bucket region to operate
     *
     * @param region
     * @return
     */
    public BucketOps region(String region) {
        this.region = region;
        return this;
    }

    private MinioClient connection() {
        return minioConnectionFactory.getConnection();
    }

    /**
     * check bucket exists
     */
    public boolean bucketExists() throws IOException, InvalidKeyException, InvalidResponseException, InsufficientDataException, NoSuchAlgorithmException, ServerException, InternalException, XmlParserException, InvalidBucketNameException, ErrorResponseException {
        return connection().bucketExists(BucketExistsArgs.builder()
                .bucket(this.bucket)
                .region(this.region)
                .extraHeaders(this.extraHeaders)
                .extraQueryParams(this.extraQueryParams)
                .build());
    }

    /**
     * Deletes encryption configuration of a bucket.
     */
    public void deleteBucketEncryption() throws IOException, InvalidKeyException, InvalidResponseException, InsufficientDataException, NoSuchAlgorithmException, ServerException, InternalException, XmlParserException, InvalidBucketNameException, ErrorResponseException {
        connection().deleteBucketEncryption(DeleteBucketEncryptionArgs.builder()
                .bucket(this.bucket)
                .region(region)
                .extraHeaders(this.extraHeaders)
                .extraQueryParams(this.extraQueryParams)
                .build());
    }

    /**
     * Deletes life-cycle configuration of a bucket.
     */
    public void deleteBucketLifeCycle() throws IOException, InvalidKeyException, InvalidResponseException, InsufficientDataException, NoSuchAlgorithmException, ServerException, InternalException, XmlParserException, InvalidBucketNameException, ErrorResponseException {
        connection().getBucketLifeCycle(GetBucketLifeCycleArgs.builder()
                .bucket(this.bucket)
                .region(this.region)
                .extraHeaders(this.extraHeaders)
                .extraQueryParams(this.extraQueryParams)
                .build());
    }

    /**
     * Deletes notification configuration of a bucket.
     */
    public void deleteBucketNotification() throws IOException, InvalidResponseException, InvalidKeyException, NoSuchAlgorithmException, ServerException, ErrorResponseException, XmlParserException, InvalidBucketNameException, InsufficientDataException, InternalException {
        connection().deleteBucketNotification(DeleteBucketNotificationArgs.builder()
                .bucket(this.bucket)
                .region(this.region)
                .extraHeaders(this.extraHeaders)
                .extraQueryParams(this.extraQueryParams)
                .build());
    }

    /**
     * Deletes bucket policy configuration of a bucket.
     */
    public void deleteBucketPolicy() throws IOException, InvalidKeyException, InvalidResponseException, InsufficientDataException, NoSuchAlgorithmException, ServerException, InternalException, XmlParserException, InvalidBucketNameException, ErrorResponseException {
        connection().deleteBucketPolicy(DeleteBucketPolicyArgs.builder()
                .bucket(this.bucket)
                .region(this.region)
                .extraHeaders(this.extraHeaders)
                .extraQueryParams(this.extraQueryParams)
                .build());
    }

    /**
     * Deletes tags of a bucket.
     */
    public void deleteBucketTags() throws IOException, InvalidKeyException, InvalidResponseException, InsufficientDataException, NoSuchAlgorithmException, ServerException, InternalException, XmlParserException, InvalidBucketNameException, ErrorResponseException {
        connection().deleteBucketTags(DeleteBucketTagsArgs.builder()
                .bucket(this.bucket)
                .region(this.region)
                .extraHeaders(this.extraHeaders)
                .extraQueryParams(this.extraQueryParams)
                .build());
    }

    /**
     * Deletes default object retention in a bucket.
     */
    public void deleteDefaultRetention() throws IOException, InvalidKeyException, InvalidResponseException, InsufficientDataException, NoSuchAlgorithmException, ServerException, InternalException, XmlParserException, InvalidBucketNameException, ErrorResponseException {
        connection().deleteDefaultRetention(DeleteDefaultRetentionArgs.builder()
                .bucket(this.bucket)
                .region(this.region)
                .extraHeaders(this.extraHeaders)
                .extraQueryParams(this.extraQueryParams)
                .build());
    }

    /**
     * Disables object versioning feature in a bucket.
     */
    public void disableVersioning() throws IOException, InvalidKeyException, InvalidResponseException, InsufficientDataException, NoSuchAlgorithmException, ServerException, InternalException, XmlParserException, InvalidBucketNameException, ErrorResponseException {
        connection().disableVersioning(DisableVersioningArgs.builder()
                .bucket(this.bucket)
                .region(this.region)
                .extraHeaders(this.extraHeaders)
                .extraQueryParams(this.extraQueryParams)
                .build());
    }

    /**
     * Enables object versioning feature in a bucket.
     */
    public void enableVersioning() throws IOException, InvalidKeyException, InvalidResponseException, InsufficientDataException, NoSuchAlgorithmException, ServerException, InternalException, XmlParserException, InvalidBucketNameException, ErrorResponseException {
        connection().enableVersioning(EnableVersioningArgs.builder()
                .bucket(this.bucket)
                .region(this.region)
                .extraHeaders(this.extraHeaders)
                .extraQueryParams(this.extraQueryParams)
                .build());
    }

    /**
     * Gets encryption configuration of a bucket.
     */
    public SseConfiguration getBucketEncryption() throws IOException, InvalidKeyException, InvalidResponseException, InsufficientDataException, NoSuchAlgorithmException, ServerException, InternalException, XmlParserException, InvalidBucketNameException, ErrorResponseException {
        return connection().getBucketEncryption(GetBucketEncryptionArgs.builder()
                .bucket(this.bucket)
                .region(this.region)
                .extraHeaders(this.extraHeaders)
                .extraQueryParams(this.extraQueryParams)
                .build());
    }

    /**
     * Gets life-cycle configuration of a bucket.
     */
    public String getBucketLifeCycle() throws IOException, InvalidKeyException, InvalidResponseException, InsufficientDataException, NoSuchAlgorithmException, ServerException, InternalException, XmlParserException, InvalidBucketNameException, ErrorResponseException {
        return connection().getBucketLifeCycle(GetBucketLifeCycleArgs.builder()
                .bucket(this.bucket)
                .region(this.region)
                .extraHeaders(this.extraHeaders)
                .extraQueryParams(this.extraQueryParams)
                .build());
    }

    /**
     * Gets notification configuration of a bucket.
     */
    public NotificationConfiguration getBucketNotification() throws IOException, InvalidKeyException, InvalidResponseException, InsufficientDataException, NoSuchAlgorithmException, ServerException, InternalException, XmlParserException, InvalidBucketNameException, ErrorResponseException {
        return connection().getBucketNotification(GetBucketNotificationArgs.builder()
                .bucket(this.bucket)
                .region(this.region)
                .extraHeaders(this.extraHeaders)
                .extraQueryParams(this.extraQueryParams)
                .build());
    }

    /**
     * Gets bucket policy configuration of a bucket.
     */
    public String getBucketPolicy() throws IOException, InvalidKeyException, InvalidResponseException, BucketPolicyTooLargeException, NoSuchAlgorithmException, ServerException, InternalException, XmlParserException, InvalidBucketNameException, InsufficientDataException, ErrorResponseException {
        return connection().getBucketPolicy(GetBucketPolicyArgs.builder()
                .bucket(this.bucket)
                .region(this.region)
                .extraHeaders(this.extraHeaders)
                .extraQueryParams(this.extraQueryParams)
                .build());
    }

    /**
     * Gets tags of a bucket.
     */
    public Tags getBucketTags() throws IOException, InvalidKeyException, InvalidResponseException, InsufficientDataException, NoSuchAlgorithmException, ServerException, InternalException, XmlParserException, InvalidBucketNameException, ErrorResponseException {
        return connection().getBucketTags(GetBucketTagsArgs.builder()
                .bucket(this.bucket)
                .region(this.region)
                .extraHeaders(this.extraHeaders)
                .extraQueryParams(this.extraQueryParams)
                .build());
    }

    /**
     * Gets default object retention in a bucket.
     */
    public ObjectLockConfiguration getDefaultRetention() throws IOException, InvalidKeyException, InvalidResponseException, InsufficientDataException, NoSuchAlgorithmException, ServerException, InternalException, XmlParserException, InvalidBucketNameException, ErrorResponseException {
        return connection().getDefaultRetention(GetDefaultRetentionArgs.builder()
                .bucket(this.bucket)
                .region(this.region)
                .extraHeaders(this.extraHeaders)
                .extraQueryParams(this.extraQueryParams)
                .build());
    }

    public boolean isVersioningEnabled() throws IOException, InvalidKeyException, InvalidResponseException, InsufficientDataException, NoSuchAlgorithmException, ServerException, InternalException, XmlParserException, InvalidBucketNameException, ErrorResponseException {
        return connection().isVersioningEnabled(IsVersioningEnabledArgs.builder()
                .bucket(this.bucket)
                .region(this.region)
                .extraHeaders(this.extraHeaders)
                .extraQueryParams(this.extraQueryParams)
                .build());
    }

    /**
     * Lists bucket information of all buckets.
     *
     * @return
     */
    public List<Bucket> listBuckets() throws IOException, InvalidKeyException, InvalidResponseException, InsufficientDataException, NoSuchAlgorithmException, ServerException, InternalException, XmlParserException, InvalidBucketNameException, ErrorResponseException {
        return connection().listBuckets();
    }

    /**
     * Listens events of object prefix and suffix of a bucket.
     * <p>
     * The returned closable iterator is lazily evaluated
     * hence its required to iterate to get new records
     * and must be used with try-with-resource to release
     * underneath network resources.
     *
     * @return
     */
    public CloseableIterator<Result<NotificationRecords>> listenBucketNotification(String objectPrefix, String objectSuffix, String... events) throws IOException, InvalidKeyException, InvalidResponseException, InsufficientDataException, NoSuchAlgorithmException, ServerException, InternalException, XmlParserException, InvalidBucketNameException, ErrorResponseException {
        if (objectPrefix == null) {
            objectPrefix = "";
        }
        if (objectSuffix == null) {
            objectSuffix = "";
        }
        return connection().listenBucketNotification(ListenBucketNotificationArgs.builder()
                .bucket(this.bucket)
                .region(this.region)
                .extraHeaders(this.extraHeaders)
                .extraQueryParams(this.extraQueryParams)
                .prefix(objectPrefix)
                .suffix(objectSuffix)
                .events(events)
                .build());
    }

    /**
     * Lists incomplete object upload information of a bucket.
     */
    public void listIncompleteUploads() {
        connection().listIncompleteUploads(ListIncompleteUploadsArgs.builder()
                .bucket(this.bucket)
                .region(this.region)
                .extraHeaders(this.extraHeaders)
                .extraQueryParams(this.extraQueryParams)
                .build());
    }

    /**
     * Lists incomplete object upload information of a bucket recursively.
     */
    public void listIncompleteUploads(boolean recursive) {
        connection().listIncompleteUploads(ListIncompleteUploadsArgs.builder()
                .bucket(this.bucket)
                .region(this.region)
                .extraHeaders(this.extraHeaders)
                .recursive(recursive)
                .extraQueryParams(this.extraQueryParams)
                .build());
    }

    /**
     * Lists incomplete object upload information of a bucket.
     */
    public void listIncompleteUploadsWithPrefix(String prefix) {
        connection().listIncompleteUploads(ListIncompleteUploadsArgs.builder()
                .bucket(this.bucket)
                .region(this.region)
                .extraHeaders(this.extraHeaders)
                .extraQueryParams(this.extraQueryParams)
                .prefix(prefix)
                .build());
    }
    //TODO: uploadIdMarker and maxUpload

    /**
     * Creates a bucket with given region and object lock feature enabled.
     */
    public void makeBucket() throws IOException, InvalidKeyException, InvalidResponseException, RegionConflictException, InsufficientDataException, NoSuchAlgorithmException, ServerException, InternalException, XmlParserException, InvalidBucketNameException, ErrorResponseException {
        connection().makeBucket(MakeBucketArgs.builder()
                .bucket(this.bucket)
                .region(this.region)
                .extraHeaders(this.extraHeaders)
                .extraQueryParams(this.extraQueryParams)
                .build());
    }

    /**
     * Creates a object-lock enabled bucket with given region and object lock feature enabled.
     */
    public void makeObjectLockEnabledBucket() throws IOException, InvalidKeyException, InvalidResponseException, RegionConflictException, InsufficientDataException, NoSuchAlgorithmException, ServerException, InternalException, XmlParserException, InvalidBucketNameException, ErrorResponseException {
        connection().makeBucket(MakeBucketArgs.builder()
                .bucket(this.bucket)
                .region(this.region)
                .extraHeaders(this.extraHeaders)
                .objectLock(true)
                .extraQueryParams(this.extraQueryParams)
                .build());
    }

    /**
     * Removes an empty bucket.
     */
    public void removeBucket() throws IOException, InvalidKeyException, InvalidResponseException, InsufficientDataException, NoSuchAlgorithmException, ServerException, InternalException, XmlParserException, InvalidBucketNameException, ErrorResponseException {
        connection().removeBucket(RemoveBucketArgs.builder()
                .bucket(this.bucket)
                .region(this.region)
                .extraHeaders(this.extraHeaders)
                .extraQueryParams(this.extraQueryParams)
                .build());
    }

    /**
     * Removes incomplete uploads of an object.
     */
    public void removeIncompleteUpload(String objectName) throws IOException, InvalidKeyException, InvalidResponseException, InsufficientDataException, NoSuchAlgorithmException, ServerException, InternalException, XmlParserException, InvalidBucketNameException, ErrorResponseException {
        connection().removeIncompleteUpload(RemoveIncompleteUploadArgs.builder()
                .bucket(this.bucket)
                .region(this.region)
                .extraHeaders(this.extraHeaders)
                .object(objectName)
                .extraQueryParams(this.extraQueryParams)
                .build());
    }

    /**
     * Removes incomplete uploads of an object.
     */
    public void removeIncompleteVersionedUpload(String objectName, String versionId) throws IOException, InvalidKeyException, InvalidResponseException, InsufficientDataException, NoSuchAlgorithmException, ServerException, InternalException, XmlParserException, InvalidBucketNameException, ErrorResponseException {
        connection().removeIncompleteUpload(RemoveIncompleteUploadArgs.builder()
                .bucket(this.bucket)
                .region(this.region)
                .extraHeaders(this.extraHeaders)
                .object(objectName)
                .versionId(versionId)
                .extraQueryParams(this.extraQueryParams)
                .build());
    }

    /**
     * Sets encryption configuration of a bucket.
     */
    public void setBucketEncryption(SseConfiguration sseConfiguration) throws IOException, InvalidKeyException, InvalidResponseException, InsufficientDataException, NoSuchAlgorithmException, ServerException, InternalException, XmlParserException, InvalidBucketNameException, ErrorResponseException {
        connection().setBucketEncryption(SetBucketEncryptionArgs.builder()
                .bucket(this.bucket)
                .region(this.region)
                .extraHeaders(this.extraHeaders)
                .extraQueryParams(this.extraQueryParams)
                .config(sseConfiguration)
                .build());
    }

    /**
     * Sets life-cycle configuration to a bucket.
     * <p>
     * for example:
     * <LifecycleConfiguration>
     * <Rule>
     * <ID>expire-bucket</ID>
     * <Prefix></Prefix>
     * <Status>Enabled</Status>
     * <Expiration>
     * <Days>365</Days>
     * </Expiration>
     * </Rule>
     * </LifecycleConfiguration>
     */
    public void setBucketLifeCycle(String lifeCycleXml) throws IOException, InvalidKeyException, InvalidResponseException, InsufficientDataException, NoSuchAlgorithmException, ServerException, InternalException, XmlParserException, InvalidBucketNameException, ErrorResponseException {
        connection().setBucketLifeCycle(SetBucketLifeCycleArgs.builder()
                .bucket(this.bucket)
                .region(this.region)
                .extraHeaders(this.extraHeaders)
                .extraQueryParams(this.extraQueryParams)
                .config(lifeCycleXml)
                .build());
    }

    /**
     * Sets notification configuration to a bucket.
     */
    public void setBucketNotification(NotificationConfiguration notification) throws IOException, InvalidKeyException, InvalidResponseException, InsufficientDataException, NoSuchAlgorithmException, ServerException, InternalException, XmlParserException, InvalidBucketNameException, ErrorResponseException {
        connection().setBucketNotification(SetBucketNotificationArgs.builder()
                .bucket(this.bucket)
                .region(this.region)
                .extraHeaders(this.extraHeaders)
                .extraQueryParams(this.extraQueryParams)
                .config(notification)
                .build());
    }

    /**
     * Sets notification configuration to a bucket.
     */
    public void setBucketNotification(List<EventType> eventTypes, String queue, String prefixRule, String suffixRule) throws IOException, InvalidKeyException, InvalidResponseException, InsufficientDataException, NoSuchAlgorithmException, ServerException, InternalException, XmlParserException, InvalidBucketNameException, ErrorResponseException {
        QueueConfiguration queueConfiguration = new QueueConfiguration();
        queueConfiguration.setQueue(queue);
        queueConfiguration.setEvents(eventTypes);
        queueConfiguration.setPrefixRule(prefixRule);
        queueConfiguration.setSuffixRule(suffixRule);

        NotificationConfiguration notificationConfiguration = new NotificationConfiguration();
        notificationConfiguration.setQueueConfigurationList(Arrays.asList(queueConfiguration));
        setBucketNotification(notificationConfiguration);
        setBucketNotification(notificationConfiguration);
    }

    /**
     * set bucket policy, for exmaple
     * <p>
     * {
     * "Statement": [
     * {
     * "Action": [
     * "s3:GetBucketLocation",
     * "s3:ListBucket"
     * ],
     * "Effect": "Allow",
     * "Principal": "*",
     * "Resource": "arn:aws:s3:::my-bucketname"
     * },
     * {
     * "Action": "s3:GetObject",
     * "Effect": "Allow",
     * "Principal": "*",
     * "Resource": "arn:aws:s3:::my-bucketname/myobject*"
     * }
     * ],
     * "Version": "2012-10-17"
     * }
     */
    public void setBucketPolicy(String policyConfig) throws IOException, InvalidKeyException, InvalidResponseException, InsufficientDataException, NoSuchAlgorithmException, ServerException, InternalException, XmlParserException, InvalidBucketNameException, ErrorResponseException {
        connection().setBucketPolicy(SetBucketPolicyArgs.builder()
                .bucket(this.bucket)
                .region(this.region)
                .extraHeaders(this.extraHeaders)
                .extraQueryParams(this.extraQueryParams)
                .config(policyConfig)
                .build());
    }

    /**
     * Sets tags to a bucket.
     */
    public void setBucketTags(Map<String, String> tags) throws IOException, InvalidKeyException, InvalidResponseException, InsufficientDataException, NoSuchAlgorithmException, ServerException, InternalException, XmlParserException, InvalidBucketNameException, ErrorResponseException {
        connection().setBucketTags(SetBucketTagsArgs.builder()
                .bucket(this.bucket)
                .region(this.region)
                .extraHeaders(this.extraHeaders)
                .extraQueryParams(this.extraQueryParams)
                .tags(tags)
                .build());
    }

    /**
     * Sets default object retention in a bucket.
     */
    public void setDefaultRetention(ObjectLockConfiguration objectLockConfiguration) throws IOException, InvalidKeyException, InvalidResponseException, InsufficientDataException, NoSuchAlgorithmException, ServerException, InternalException, XmlParserException, InvalidBucketNameException, ErrorResponseException {
        connection().setDefaultRetention(SetDefaultRetentionArgs.builder()
                .bucket(this.bucket)
                .region(this.region)
                .extraHeaders(this.extraHeaders)
                .extraQueryParams(this.extraQueryParams)
                .config(objectLockConfiguration)
                .build());
    }

    /**
     * Sets default object retention in a bucket.
     */
    public void setDefaultRetention(RetentionMode retentionMode, RetentionDuration retentionDuration) throws IOException, InvalidKeyException, InvalidResponseException, InsufficientDataException, NoSuchAlgorithmException, ServerException, InternalException, XmlParserException, InvalidBucketNameException, ErrorResponseException {
        ObjectLockConfiguration objectLockConfiguration = new ObjectLockConfiguration(retentionMode, retentionDuration);
        connection().setDefaultRetention(SetDefaultRetentionArgs.builder()
                .bucket(this.bucket)
                .region(this.region)
                .extraHeaders(this.extraHeaders)
                .extraQueryParams(this.extraQueryParams)
                .config(objectLockConfiguration)
                .build());
    }
}
