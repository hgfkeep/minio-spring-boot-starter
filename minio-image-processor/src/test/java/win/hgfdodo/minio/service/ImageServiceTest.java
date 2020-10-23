package win.hgfdodo.minio.service;

import io.minio.errors.*;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;

import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.security.InvalidKeyException;
import java.security.NoSuchAlgorithmException;

@SpringBootTest
class ImageServiceTest {

    @Autowired
    ImageService imageService;

    private final static String bucketName = "test";
    private final static String objectName = "2020-09-10/1599726705195_Koala.jpg";

    @Test
    void resizeImage() throws IOException, InvalidResponseException, InvalidKeyException, NoSuchAlgorithmException, ServerException, InternalException, XmlParserException, InvalidBucketNameException, InsufficientDataException, ErrorResponseException {
        InputStream inputStream = imageService.resizeImage(bucketName, objectName, 2 * 1000 * 1000, 640, 320);
        int len = inputStream.available();
        byte[] bytes = new byte[len];
        inputStream.read(bytes);
        Files.write(Paths.get("D:\\Projects\\minio-spring-boot\\image-processor\\src\\test", "a.jpg"), bytes);
    }

    @Test
    void testResizeImage() {
    }

    @Test
    void testResizeImage1() {
    }

    @Test
    void resizeImageAndSave() throws IOException, InvalidResponseException, InvalidKeyException, NoSuchAlgorithmException, ServerException, ErrorResponseException, XmlParserException, InvalidBucketNameException, InsufficientDataException, InternalException {
        String scaledObjectName = imageService.resizeImageAndSave(bucketName, objectName, 2 * 1000 * 1000, 640, 320);
        System.out.println("objectName:"+ scaledObjectName);
    }

    @Test
    void testResizeImageAndSave() {
    }

    @Test
    void getPresignedScaleImage() {
    }

    @Test
    void testGetPresignedScaleImage() {
    }
}