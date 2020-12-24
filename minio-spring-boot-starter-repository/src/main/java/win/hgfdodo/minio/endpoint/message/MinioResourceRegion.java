package win.hgfdodo.minio.endpoint.message;

import org.springframework.core.io.Resource;
import org.springframework.http.MediaType;
import org.springframework.http.MediaTypeFactory;
import org.springframework.util.Assert;

import java.util.Optional;

/**
 * @author Guangfu He
 * @date 2020/12/23 10:06
 * @email hgfkeep@gmail.com
 */
public class MinioResourceRegion {

    private final Resource resource;

    private final long position;

    private final long count;

    private final long contentLength;

    private Optional<MediaType> mediaType;


    /**
     * Create a new {@code ResourceRegion} from a given {@link Resource}.
     * This region of a resource is represented by a start {@code position}
     * and a byte {@code count} within the given {@code Resource}.
     *
     * @param resource      a Resource
     * @param position      the start position of the region in that resource
     * @param count         the byte count of the region in that resource
     * @param contentLength
     */
    public MinioResourceRegion(Resource resource, long position, long count, long contentLength, Optional<MediaType> mediaType) {
        Assert.notNull(resource, "Resource must not be null");
        Assert.isTrue(position >= 0, "'position' must be larger than or equal to 0");
        Assert.isTrue(count >= 0, "'count' must be larger than or equal to 0");
        Assert.isTrue(contentLength >= 0, "'contentLength' must be larger than or equal to 0");
        this.resource = resource;
        this.position = position;
        this.count = count;
        this.contentLength = contentLength;
        this.mediaType = mediaType;
        if (!this.mediaType.isPresent()) {
            this.mediaType = MediaTypeFactory.getMediaType(this.resource);
        }
    }


    /**
     * Return the underlying {@link Resource} for this {@code ResourceRegion}.
     */
    public Resource getResource() {
        return this.resource;
    }

    /**
     * Return the start position of this region in the underlying {@link Resource}.
     */
    public long getPosition() {
        return this.position;
    }

    /**
     * Return the byte count of this region in the underlying {@link Resource}.
     */
    public long getCount() {
        return this.count;
    }

    /**
     * Return the content length in the underlying {@link Resource}.
     */
    public long getContentLength() {
        return contentLength;
    }

    /**
     * Return the mediaType in the underlying {@link Resource}.
     */
    public Optional<MediaType> getMediaType() {
        return mediaType;
    }
}
