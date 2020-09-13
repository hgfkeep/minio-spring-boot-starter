package win.hgfdodo.minio.service;

import io.minio.errors.*;
import io.minio.messages.Bucket;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;

import java.io.IOException;
import java.security.InvalidKeyException;
import java.security.NoSuchAlgorithmException;
import java.util.List;

@SpringBootTest
class MinioTemplateTest {

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
        System.out.println(bucketList);
        System.out.println(bucketList.contains(BUCKET_NAME));
        Assertions.assertTrue(bucketList.contains(BUCKET_NAME));
    }

    @Test
    void getBucket() {
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
    void saveObject() {
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