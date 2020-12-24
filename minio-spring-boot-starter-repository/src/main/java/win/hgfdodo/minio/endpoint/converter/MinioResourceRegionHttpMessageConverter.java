package win.hgfdodo.minio.endpoint.converter;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.core.io.Resource;
import org.springframework.http.HttpHeaders;
import org.springframework.http.HttpInputMessage;
import org.springframework.http.HttpOutputMessage;
import org.springframework.http.MediaType;
import org.springframework.http.converter.AbstractGenericHttpMessageConverter;
import org.springframework.http.converter.HttpMessageNotReadableException;
import org.springframework.http.converter.HttpMessageNotWritableException;
import org.springframework.util.Assert;
import org.springframework.util.MimeTypeUtils;
import org.springframework.util.StreamUtils;
import win.hgfdodo.minio.endpoint.message.MinioResourceRegion;
import win.hgfdodo.minio.service.MinioTemplate;

import javax.annotation.Nullable;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.lang.reflect.ParameterizedType;
import java.lang.reflect.Type;
import java.nio.charset.StandardCharsets;
import java.util.Collection;

/**
 * @author Guangfu He
 * @date 2020/12/23 10:06
 * @email hgfkeep@gmail.com
 */
public class MinioResourceRegionHttpMessageConverter extends AbstractGenericHttpMessageConverter<Object> {
    private final static Logger log = LoggerFactory.getLogger(MinioResourceRegionHttpMessageConverter.class);

    private final MinioTemplate minioTemplate;

    public MinioResourceRegionHttpMessageConverter(MinioTemplate minioTemplate) {
        super(MediaType.ALL);
        this.minioTemplate = minioTemplate;
    }

    /**
     * get default http content type of minio resource region
     */
    @Override
    protected MediaType getDefaultContentType(Object object) throws IOException {
        MinioResourceRegion region = null;
        if (object instanceof MinioResourceRegion) {
            region = ((MinioResourceRegion) object);
        } else {
            Collection<MinioResourceRegion> regions = (Collection<MinioResourceRegion>) object;
            if (!regions.isEmpty()) {
                region = regions.iterator().next();
            }
        }
        log.debug("default content type: {}", region.getMediaType().get());
        return region.getMediaType().orElse(MediaType.APPLICATION_OCTET_STREAM);
    }

    @Override
    public boolean canWrite(Class<?> clazz, @Nullable MediaType mediaType) {
        return canWrite(clazz, null, mediaType);
    }

    @Override
    protected boolean canWrite(MediaType mediaType) {
        return super.canWrite(mediaType);
    }

    @Override
    public boolean canWrite(Type type, Class<?> clazz, MediaType mediaType) {
        if (!(type instanceof ParameterizedType)) { // type 不是参数化类型， 直接判断类型
            return (type instanceof Class && MinioResourceRegion.class.isAssignableFrom((Class<?>) type));
        }

        ParameterizedType parameterizedType = (ParameterizedType) type;
        if (!(parameterizedType.getRawType() instanceof Class)) {
            return false;
        }
        Class<?> rawType = (Class<?>) parameterizedType.getRawType();
        if (!(Collection.class.isAssignableFrom(rawType))) {
            return false;
        }
        if (parameterizedType.getActualTypeArguments().length != 1) {
            return false;
        }
        Type typeArgument = parameterizedType.getActualTypeArguments()[0];
        if (!(typeArgument instanceof Class)) {
            return false;
        }

        Class<?> typeArgumentClass = (Class<?>) typeArgument;
        return MinioResourceRegion.class.isAssignableFrom(typeArgumentClass);
    }


    @Override
    protected void writeInternal(Object object, Type type, HttpOutputMessage outputMessage) throws IOException, HttpMessageNotWritableException {
        if (object instanceof MinioResourceRegion) {
            writeMinioResourceRegion((MinioResourceRegion) object, outputMessage);
        } else {
            Collection<MinioResourceRegion> regions = (Collection<MinioResourceRegion>) object;
            if (regions.size() == 1) {
                writeMinioResourceRegion(regions.iterator().next(), outputMessage);
            } else {
                writeResourceRegionCollection((Collection<MinioResourceRegion>) object, outputMessage);
            }
        }
    }

    protected void writeMinioResourceRegion(MinioResourceRegion region, HttpOutputMessage httpOutputMessage) throws IOException, HttpMessageNotWritableException {
        Assert.notNull(region, "ResourceRegion must not be null");
        HttpHeaders responseHeaders = httpOutputMessage.getHeaders();

        long start = region.getPosition();
        long end = start + region.getCount() - 1;
        long resourceLength = region.getContentLength();
        end = Math.min(end, resourceLength - 1);
        long rangeLength = end - start + 1;
        responseHeaders.add("Content-Range", "bytes " + start + '-' + end + '/' + resourceLength);
        responseHeaders.setContentLength(rangeLength);

        InputStream in = region.getResource().getInputStream();
        try {
            StreamUtils.copyRange(in, httpOutputMessage.getBody(), start, end);
        } finally {
            try {
                in.close();
            } catch (IOException ex) {
                log.error("close input stream error!", ex);
            }
        }
    }

    private void writeResourceRegionCollection(Collection<MinioResourceRegion> resourceRegions,
                                               HttpOutputMessage outputMessage) throws IOException {

        Assert.notNull(resourceRegions, "Collection of ResourceRegion should not be null");
        HttpHeaders responseHeaders = outputMessage.getHeaders();

        MediaType contentType = responseHeaders.getContentType();
        String boundaryString = MimeTypeUtils.generateMultipartBoundaryString();
        responseHeaders.set(HttpHeaders.CONTENT_TYPE, "multipart/byteranges; boundary=" + boundaryString);
        OutputStream out = outputMessage.getBody();

        Resource resource = null;
        InputStream in = null;
        long inputStreamPosition = 0;

        try {
            for (MinioResourceRegion region : resourceRegions) {
                long start = region.getPosition() - inputStreamPosition;
                if (start < 0 || resource != region.getResource()) {
                    if (in != null) {
                        in.close();
                    }
                    resource = region.getResource();
                    in = resource.getInputStream();
                    inputStreamPosition = 0;
                    start = region.getPosition();
                }
                long end = start + region.getCount() - 1;
                // Writing MIME header.
                println(out);
                print(out, "--" + boundaryString);
                println(out);
                if (contentType != null) {
                    print(out, "Content-Type: " + contentType);
                    println(out);
                }
                long resourceLength = region.getContentLength();
                end = Math.min(end, resourceLength - inputStreamPosition - 1);
                print(out, "Content-Range: bytes " +
                        region.getPosition() + '-' + (region.getPosition() + region.getCount() - 1) +
                        '/' + resourceLength);
                println(out);
                println(out);
                // Printing content
                StreamUtils.copyRange(in, out, start, end);
                inputStreamPosition += (end + 1);
            }
        } finally {
            try {
                if (in != null) {
                    in.close();
                }
            } catch (IOException ex) {
                // ignore
            }
        }

        println(out);
        print(out, "--" + boundaryString + "--");
    }

    private static void println(OutputStream os) throws IOException {
        os.write('\r');
        os.write('\n');
    }

    private static void print(OutputStream os, String buf) throws IOException {
        os.write(buf.getBytes(StandardCharsets.US_ASCII));
    }


    @Override
    public boolean canRead(Class<?> clazz, @Nullable MediaType mediaType) {
        return false;
    }

    @Override
    public boolean canRead(Type type, @Nullable Class<?> contextClass, @Nullable MediaType mediaType) {
        return false;
    }

    @Override
    public MinioResourceRegion read(Type type, @Nullable Class<?> contextClass, HttpInputMessage inputMessage)
            throws IOException, HttpMessageNotReadableException {

        throw new UnsupportedOperationException();
    }

    @Override
    protected Object readInternal(Class<?> aClass, HttpInputMessage httpInputMessage) throws IOException, HttpMessageNotReadableException {

        throw new UnsupportedOperationException();
    }
}
