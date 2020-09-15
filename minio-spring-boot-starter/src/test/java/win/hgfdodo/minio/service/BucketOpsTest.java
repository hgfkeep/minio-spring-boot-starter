package win.hgfdodo.minio.service;

import io.minio.errors.*;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import win.hgfdodo.minio.connection.MinioConnectionFactory;

import java.io.IOException;
import java.security.InvalidKeyException;
import java.security.NoSuchAlgorithmException;

/**
 * @author Guangfu He
 * @date 2020/9/14 14:49
 * @email hgfkeep@gmail.com
 */
@SpringBootTest
class BucketOpsTest {

    @Autowired
    MinioConnectionFactory minioConnectionFactory;
    private BucketOps bucketOps;

    @BeforeEach
    public void setup() {
        this.bucketOps = new BucketOps(minioConnectionFactory);
    }

    @Test
    void bucketExists() throws IOException, InvalidResponseException, InvalidKeyException, NoSuchAlgorithmException, ServerException, ErrorResponseException, XmlParserException, InvalidBucketNameException, InsufficientDataException, InternalException {
    }
}