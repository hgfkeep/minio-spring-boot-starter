package win.hgfdodo.minio.config;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import win.hgfdodo.minio.endpoint.converter.MinioResourceRegionHttpMessageConverter;
import win.hgfdodo.minio.service.MinioTemplate;

/**
 * @author Guangfu He
 * @date 2020/12/23 11:23
 * @email hgfkeep@gmail.com
 */
@Configuration
public class Config {
    private final static Logger log = LoggerFactory.getLogger(Config.class);

    @Bean
    public MinioResourceRegionHttpMessageConverter convertor(MinioTemplate minioTemplate) {
        return new MinioResourceRegionHttpMessageConverter(minioTemplate);
    }
}
