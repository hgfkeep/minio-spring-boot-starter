package win.hgfdodo.minio.service;

import io.minio.ObjectStat;
import io.minio.ObjectWriteResponse;
import io.minio.errors.*;
import net.coobird.thumbnailator.Thumbnails;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;
import win.hgfdodo.minio.domain.FileNameAndType;
import win.hgfdodo.minio.exception.NotImageException;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.security.InvalidKeyException;
import java.security.NoSuchAlgorithmException;

@Service
public class ImageService {
    private final static Logger log = LoggerFactory.getLogger(ImageService.class);

    private final MinioTemplate minioTemplate;

    public ImageService(MinioTemplate minioTemplate) {
        this.minioTemplate = minioTemplate;
    }

    /**
     * 获取minio 中的图片信息，转为wx兼容的图片格式和图片size
     *
     * @param bucketName
     * @param objectName
     * @return
     */
    public InputStream resizeImage(String bucketName, String objectName, int maxSize, int width, int hight) throws IOException, InvalidResponseException, InvalidKeyException, NoSuchAlgorithmException, ServerException, ErrorResponseException, XmlParserException, InvalidBucketNameException, InsufficientDataException, InternalException {
        log.debug("Request to resize image: {}:://{}", bucketName, objectName);
        FileNameAndType fileNameAndType = new FileNameAndType(objectName);
        if (!fileNameAndType.isPicture()) {
            throw new NotImageException(objectName);
        }
        ObjectStat objectStat = minioTemplate.getObjectInfo(bucketName, objectName);
        float len = (float) objectStat.length();
        float quality = maxSize / len;
        if (quality > 1.0F) {
            quality = 1.0F;
        }
        InputStream inputStream = minioTemplate.getObject(bucketName, objectName);
        return resizeImage(inputStream, fileNameAndType.getType(), quality, width, hight);
    }

    /**
     * 获取minio 中的图片信息，转为wx兼容的图片size, 用户指定的图片格式
     *
     * @param bucketName
     * @param objectName
     * @param outputType
     * @return
     */
    public InputStream resizeImage(String bucketName, String objectName, String outputType, int maxSize, int width, int hight) throws IOException, InvalidResponseException, InvalidKeyException, NoSuchAlgorithmException, ServerException, ErrorResponseException, XmlParserException, InvalidBucketNameException, InsufficientDataException, InternalException {
        log.debug("Request to resize image: {}:://{}", bucketName, objectName);
        FileNameAndType fileNameAndType = new FileNameAndType(objectName);
        if (!fileNameAndType.isPicture()) {
            throw new NotImageException(objectName);
        }
        ObjectStat objectStat = minioTemplate.getObjectInfo(bucketName, objectName);
        float len = (float) objectStat.length();
        float quality = maxSize / len;
        if (quality > 1.0F) {
            quality = 1.0F;
        }
        InputStream inputStream = minioTemplate.getObject(bucketName, objectName);
        return resizeImage(inputStream, outputType, quality, width, hight);
    }

    /**
     * 获取图片流，转为wx兼容的图片size, 用户指定的图片格式
     *
     * @param inputStream 图片输入流
     * @param outputType  输出格式
     * @param quality     缩放的图像质量， 介于0.0 ~1.0
     * @param width       缩放到的图片宽度，保持原图比例
     * @param hight       缩放到的图片高度，保持原图比例
     * @return
     */
    public InputStream resizeImage(InputStream inputStream, String outputType, float quality, int width, int hight) throws IOException, InvalidResponseException, InvalidKeyException, NoSuchAlgorithmException, ServerException, ErrorResponseException, XmlParserException, InvalidBucketNameException, InsufficientDataException, InternalException {
        ByteArrayOutputStream byteArrayOutputStream = new ByteArrayOutputStream();
        Thumbnails.of(inputStream).size(width, hight).outputQuality(quality).outputFormat(outputType).toOutputStream(byteArrayOutputStream);
        ByteArrayInputStream byteArrayInputStream = new ByteArrayInputStream(byteArrayOutputStream.toByteArray());
        return byteArrayInputStream;
    }

    /**
     * @param bucketName
     * @param objectName
     * @param maxSize
     * @param width
     * @param hight
     * @return 返回objectName
     */
    public String resizeImageAndSave(String bucketName, String objectName, int maxSize, int width, int hight) throws IOException, InvalidResponseException, InvalidKeyException, NoSuchAlgorithmException, ServerException, InternalException, XmlParserException, InvalidBucketNameException, InsufficientDataException, ErrorResponseException {
        log.debug("Request to resize and save image: {}:://{}", bucketName, objectName);
        FileNameAndType fileNameAndType = new FileNameAndType(objectName);
        ObjectStat objectStat = minioTemplate.getObjectInfo(bucketName, objectName);
        float len = (float) objectStat.length();
        float quality = maxSize / len;
        if (quality > 1.0F) {
            quality = 1.0F;
        }
        InputStream inputStream = resizeImage(bucketName, objectName, maxSize, width, hight);
        ObjectWriteResponse response = minioTemplate.saveKnownSizeObject(bucketName, fileNameAndType.getScaledFileName(), inputStream, inputStream.available(), fileNameAndType.getImageContentType());
        System.out.println(response);
        return fileNameAndType.getScaledFileName();
    }

    /**
     * @param bucketName
     * @param objectName
     * @param outputType 输出的文件类型
     * @param maxSize
     * @param width
     * @param hight
     * @return 返回缩放后的objectName
     */
    public String resizeImageAndSave(String bucketName, String objectName, String outputType, int maxSize, int width, int hight) throws IOException, InvalidResponseException, InvalidKeyException, NoSuchAlgorithmException, ServerException, InternalException, XmlParserException, InvalidBucketNameException, InsufficientDataException, ErrorResponseException {
        log.debug("Request to resize and save image: {}:://{}, outputType={}", bucketName, objectName, outputType);
        FileNameAndType fileNameAndType = new FileNameAndType(objectName);
        InputStream inputStream = resizeImage(bucketName, objectName, outputType, maxSize, width, hight);
        minioTemplate.saveKnownSizeObject(bucketName, fileNameAndType.getScaledFileName(), inputStream, inputStream.available(), fileNameAndType.getImageContentType());
        return fileNameAndType.getScaledFileName();
    }

    public String getPresignedScaleImage(String bucketName, String objectName, int maxSize, int width, int hight) throws IOException, InvalidResponseException, InvalidKeyException, NoSuchAlgorithmException, ServerException, ErrorResponseException, XmlParserException, InvalidBucketNameException, InsufficientDataException, InternalException, InvalidExpiresRangeException {
        log.debug("Request to resize image and get presigned image url: {}://{}, max size:{}, expect width:{}, hight:{}", bucketName, objectName, maxSize, width, hight);
        String resizedImageName = resizeImageAndSave(bucketName, objectName, maxSize, width, hight);
        return minioTemplate.getObjectURL(bucketName, resizedImageName);
    }

    public String getPresignedScaleImage(String bucketName, String objectName, Integer expireInSeconds, int maxSize, int width, int hight) throws IOException, InvalidResponseException, InvalidKeyException, NoSuchAlgorithmException, ServerException, ErrorResponseException, XmlParserException, InvalidBucketNameException, InsufficientDataException, InternalException, InvalidExpiresRangeException {
        log.debug("Request to resize image and get presigned image url: {}://{}, max size:{}, expect width:{}, hight:{}, expired in {}s", bucketName, objectName, maxSize, width, hight, expireInSeconds);
        String resizedImageName = resizeImageAndSave(bucketName, objectName, maxSize, width, hight);
        return minioTemplate.getObjectURL(bucketName, resizedImageName, expireInSeconds);
    }
}
