package win.hgfdodo.minio.config;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import win.hgfdodo.minio.service.ImageService;
import win.hgfdodo.minio.service.MinioTemplate;

/**
 * @author Guangfu He
 * @date 2020/10/27 9:19
 * @email hgfkeep@gmail.com
 */
@Configuration
public class ImageProcessAutoConfiguration {
    private final static Logger log = LoggerFactory.getLogger(ImageProcessAutoConfiguration.class);

    @Bean
    public ImageService imageService(MinioTemplate minioTemplate) {
        return new ImageService(minioTemplate);
    }

}
