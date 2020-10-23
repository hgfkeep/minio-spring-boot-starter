package win.hgfdodo.minio.service;

import io.minio.errors.*;
import io.minio.messages.Bucket;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.security.InvalidKeyException;
import java.security.NoSuchAlgorithmException;
import java.util.List;
import java.util.Optional;

import static org.junit.jupiter.api.Assertions.assertTrue;

@SpringBootTest
class MinioTemplateTest {
    private final static Logger log = LoggerFactory.getLogger(MinioTemplateTest.class);

    @Autowired
    MinioTemplate minioTemplate;
    private final static String BUCKET_NAME = "hgf";

    @BeforeEach
    void setUp() {
    }

    @Test
    void createBucket() throws IOException, InvalidResponseException, RegionConflictException, InvalidKeyException, NoSuchAlgorithmException, ServerException, ErrorResponseException, XmlParserException, InvalidBucketNameException, InsufficientDataException, InternalException {
        minioTemplate.createBucket(BUCKET_NAME);
        List<Bucket> bucketList = minioTemplate.getAllBuckets();
        log.info("bucket list :{}", bucketList);
        log.info("list containers {}? {}", BUCKET_NAME, bucketList.contains(BUCKET_NAME));
        assertTrue(bucketList.contains(BUCKET_NAME));
    }

    @Test
    void getBucket() throws IOException, InvalidResponseException, InvalidKeyException, NoSuchAlgorithmException, ServerException, ErrorResponseException, XmlParserException, InvalidBucketNameException, InsufficientDataException, InternalException {
        Optional<Bucket> bucket = minioTemplate.getBucket(BUCKET_NAME);
        if (bucket.isPresent()) {
            Bucket b = bucket.get();
            log.info("bucket info: name={}, createTime={}", b.name(), b.creationDate());
        }
        assertTrue(bucket.isPresent());
    }

    @Test
    void removeBucket() {
    }

    @Test
    void getAllObjectsByPrefix() {
    }

    @Test
    void getObjectURL() {
    }

    @Test
    void saveKnownSizeObject() {
    }

    @Test
    void saveUnknownSizeObject() {
    }

    @Test
    void mkdir() {
    }

    @Test
    void saveObject() throws IOException, InvalidResponseException, InvalidKeyException, NoSuchAlgorithmException, ServerException, InternalException, XmlParserException, InvalidBucketNameException, InsufficientDataException, ErrorResponseException {
        FileInputStream fileInputStream = new FileInputStream(new File("D:\\Projects\\minio-spring-boot\\minio-spring-boot-starter\\src\\test\\resources\\a.jpg"));
        minioTemplate.saveKnownSizeObject("test", "2020-09-10/x.jpg", fileInputStream, fileInputStream.available(), "image/jpeg");
    }

    @Test
    void saveObjectExtra() {
    }

    @Test
    void saveObjectExtraWithSSE() {
    }

    @Test
    void getObjectInfo() {
    }

    @Test
    void getVersionedObjectInfo() {
    }

    @Test
    void removeObject() {
    }

    @Test
    void removeVersionedObject() {
    }

    @Test
    void removeObjects() {
    }
}