package win.hgfdodo.minio.endpoint;


import io.minio.ObjectStat;
import io.minio.errors.*;
import io.minio.messages.Bucket;
import io.minio.messages.Item;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.http.HttpStatus;
import org.springframework.web.bind.annotation.*;
import org.springframework.web.multipart.MultipartFile;
import win.hgfdodo.minio.service.MinioTemplate;
import win.hgfdodo.minio.vo.MinioItem;

import java.io.IOException;
import java.security.InvalidKeyException;
import java.security.NoSuchAlgorithmException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

/**
 * Minio common controller
 *
 * @author Guangfu He
 */
@ConditionalOnProperty(name = "spring.minio.endpoint.enable", havingValue = "true")
@RestController
@RequestMapping("${spring.minio.endpoint.name:/minio}")
public class MinioEndpoint {

    private final MinioTemplate template;

    public MinioEndpoint(MinioTemplate template) {
        this.template = template;
    }

    /**
     * Bucket Endpoints
     */
    @PostMapping("/bucket/{bucketName}")
    public Bucket createBucker(@PathVariable String bucketName) throws IOException, InvalidResponseException, RegionConflictException, InvalidKeyException, NoSuchAlgorithmException, ServerException, ErrorResponseException, XmlParserException, InvalidBucketNameException, InsufficientDataException, InternalException {

        template.createBucket(bucketName);
        return template.getBucket(bucketName).get();

    }

    @GetMapping("/bucket")
    public List<Bucket> getBuckets() throws IOException, InvalidResponseException, InvalidKeyException, NoSuchAlgorithmException, ServerException, ErrorResponseException, XmlParserException, InvalidBucketNameException, InsufficientDataException, InternalException {
        return template.getAllBuckets();
    }

    @GetMapping("/bucket/{bucketName}")
    public Bucket getBucket(@PathVariable String bucketName) throws IOException, InvalidResponseException, InvalidKeyException, NoSuchAlgorithmException, ServerException, ErrorResponseException, XmlParserException, InvalidBucketNameException, InsufficientDataException, InternalException {
        return template.getBucket(bucketName).orElseThrow(() -> new IllegalArgumentException("Bucket Name not found!"));
    }

    @DeleteMapping("/bucket/{bucketName}")
    @ResponseStatus(HttpStatus.ACCEPTED)
    public void deleteBucket(@PathVariable String bucketName) throws IOException, InvalidResponseException, InvalidKeyException, NoSuchAlgorithmException, ServerException, ErrorResponseException, XmlParserException, InvalidBucketNameException, InsufficientDataException, InternalException {

        template.removeBucket(bucketName);
    }

    /**
     * Object Endpoints
     *
     * @return
     */

    @PostMapping("/object/{bucketName}")
    public ObjectStat createObject(@RequestBody MultipartFile object, @PathVariable String bucketName) throws IOException, InvalidResponseException, InvalidKeyException, NoSuchAlgorithmException, ServerException, InternalException, XmlParserException, InvalidBucketNameException, InsufficientDataException, ErrorResponseException {
        String name = object.getOriginalFilename();
        template.saveKnownSizeObject(bucketName, name, object.getInputStream(), object.getSize(), object.getContentType());
        return template.getObjectInfo(bucketName, name);
    }

    @PostMapping("/object/{bucketName}/{objectName}")
    public ObjectStat createObject(@RequestBody MultipartFile object, @PathVariable String bucketName, @PathVariable String objectName) throws IOException, InvalidResponseException, InvalidKeyException, NoSuchAlgorithmException, ServerException, InternalException, XmlParserException, InvalidBucketNameException, InsufficientDataException, ErrorResponseException {
        template.saveKnownSizeObject(bucketName, objectName, object.getInputStream(), object.getSize(), object.getContentType());
        return template.getObjectInfo(bucketName, objectName);

    }

    @GetMapping("/object/{bucketName}/{objectName}")
    public List<MinioItem> filterObject(@PathVariable String bucketName, @PathVariable String objectName) throws IOException, InvalidResponseException, InvalidKeyException, NoSuchAlgorithmException, ServerException, ErrorResponseException, XmlParserException, InvalidBucketNameException, InsufficientDataException, InternalException {
        List<Item> items = template.getAllObjectsByPrefix(bucketName, objectName, true);
        return items.stream().map(MinioItem::new).collect(Collectors.toList());
    }

    @GetMapping("/object/{bucketName}/{objectName}/{expires}")
    public Map<String, Object> getObject(@PathVariable String bucketName, @PathVariable String objectName, @PathVariable Integer expires) throws IOException, InvalidResponseException, InvalidKeyException, InvalidExpiresRangeException, ServerException, ErrorResponseException, NoSuchAlgorithmException, XmlParserException, InvalidBucketNameException, InsufficientDataException, InternalException {
        Map<String, Object> responseBody = new HashMap<>();
        // Put Object info
        responseBody.put("bucket", bucketName);
        responseBody.put("object", objectName);
        responseBody.put("url", template.getObjectURL(bucketName, objectName, expires));
        responseBody.put("expires", expires);
        return responseBody;
    }

    @DeleteMapping("/object/{bucketName}/{objectName}/")
    @ResponseStatus(HttpStatus.ACCEPTED)
    public void deleteObject(@PathVariable String bucketName, @PathVariable String objectName) throws IOException, InvalidResponseException, InvalidKeyException, NoSuchAlgorithmException, ServerException, ErrorResponseException, XmlParserException, InvalidBucketNameException, InsufficientDataException, InternalException {
        template.removeObject(bucketName, objectName);
    }

}
