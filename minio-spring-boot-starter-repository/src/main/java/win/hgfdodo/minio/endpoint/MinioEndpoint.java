package win.hgfdodo.minio.endpoint;


import io.minio.ObjectStat;
import io.minio.errors.*;
import io.minio.messages.Bucket;
import io.minio.messages.Item;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.core.io.UrlResource;
import org.springframework.http.*;
import org.springframework.util.AntPathMatcher;
import org.springframework.web.bind.annotation.*;
import org.springframework.web.multipart.MultipartFile;
import org.springframework.web.servlet.HandlerMapping;
import win.hgfdodo.minio.endpoint.message.MinioResourceRegion;
import win.hgfdodo.minio.service.MinioTemplate;
import win.hgfdodo.minio.vo.MinioItem;

import javax.servlet.http.HttpServletRequest;
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
    private final Logger log = LoggerFactory.getLogger(MinioEndpoint.class);

    private final MinioTemplate template;
    public final static int MAX_SLICE_DATA = 16 * 1024 * 1024;

    public MinioEndpoint(MinioTemplate template) {
        this.template = template;
    }

    /**
     * Bucket Endpoints
     */
    @PostMapping("/bucket/{bucketName}")
    public Bucket createBucket(@PathVariable String bucketName) throws IOException, InvalidResponseException, RegionConflictException, InvalidKeyException, NoSuchAlgorithmException, ServerException, ErrorResponseException, XmlParserException, InvalidBucketNameException, InsufficientDataException, InternalException {

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

    /**
     *
     * @param bucketName
     * @param objectName
     * @param expires  url 过期时间，单位s
     */
    @GetMapping("/object/url/{bucketName}/{objectName}")
    public Map<String, Object> getPresignedObjectUrl(@PathVariable String bucketName, @PathVariable String objectName, @RequestParam Integer expires) throws IOException, InvalidResponseException, InvalidKeyException, InvalidExpiresRangeException, ServerException, ErrorResponseException, NoSuchAlgorithmException, XmlParserException, InvalidBucketNameException, InsufficientDataException, InternalException {
        Map<String, Object> responseBody = new HashMap<>();
        // Put Object info
        responseBody.put("bucket", bucketName);
        responseBody.put("object", objectName);
        responseBody.put("url", template.getObjectURL(bucketName, objectName, expires));
        responseBody.put("expires", expires);
        return responseBody;
    }

    @GetMapping(value = "/object/partial/{bucketName}/**")
    public ResponseEntity<MinioResourceRegion> getObject(@PathVariable String bucketName, @RequestHeader HttpHeaders headers, HttpServletRequest request) throws IOException, InvalidResponseException, InvalidKeyException, NoSuchAlgorithmException, ServerException, ErrorResponseException, XmlParserException, InvalidBucketNameException, InsufficientDataException, InternalException, InvalidExpiresRangeException {
        final String path = request.getAttribute(HandlerMapping.PATH_WITHIN_HANDLER_MAPPING_ATTRIBUTE).toString();
        final String bestMatchPattern = request.getAttribute(HandlerMapping.BEST_MATCHING_PATTERN_ATTRIBUTE).toString();
        String objectName = new AntPathMatcher().extractPathWithinPattern(bestMatchPattern, path);

        ObjectStat stat = template.getObjectInfo(bucketName, objectName);
        log.debug("object stat: {}", stat);
        String url = template.getObjectURL(bucketName, objectName);
        UrlResource urlResource = new UrlResource(url);

        HttpRange range = null;
        if (headers.getRange().size() > 0) {
            range = headers.getRange().get(0);
        }
        if (range != null) {
            long start = range.getRangeStart(stat.length());
            long end = range.getRangeEnd(stat.length());
            long rangeLength = Math.min(MAX_SLICE_DATA, end - start + 1);
            MinioResourceRegion region = new MinioResourceRegion(urlResource, start, rangeLength, stat.length(), MediaTypeFactory.getMediaType(stat.name()));
            return ResponseEntity.status(HttpStatus.PARTIAL_CONTENT).contentType(region.getMediaType().get()).body(region);
        } else {
            long rangeLength = Math.min(MAX_SLICE_DATA, stat.length());
            MinioResourceRegion region = new MinioResourceRegion(urlResource, 0, rangeLength, stat.length(), MediaTypeFactory.getMediaType(stat.name()));
            return ResponseEntity.status(HttpStatus.PARTIAL_CONTENT).contentType(region.getMediaType().get()).body(region);
        }
    }

    @DeleteMapping("/object/{bucketName}/{objectName}/")
    @ResponseStatus(HttpStatus.ACCEPTED)
    public void deleteObject(@PathVariable String bucketName, @PathVariable String objectName) throws IOException, InvalidResponseException, InvalidKeyException, NoSuchAlgorithmException, ServerException, ErrorResponseException, XmlParserException, InvalidBucketNameException, InsufficientDataException, InternalException {
        template.removeObject(bucketName, objectName);
    }

}
