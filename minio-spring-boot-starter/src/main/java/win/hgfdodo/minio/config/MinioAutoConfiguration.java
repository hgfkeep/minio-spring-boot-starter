package win.hgfdodo.minio.config;

import io.minio.MinioClient;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.autoconfigure.condition.ConditionalOnBean;
import org.springframework.boot.autoconfigure.condition.ConditionalOnClass;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.util.StringUtils;
import win.hgfdodo.minio.connection.MinioConnectionFactory;
import win.hgfdodo.minio.service.MinioTemplate;

/**
 * minio connection auto configuration using MinioProperties
 *
 * @author @author Guangfu He
 */
@EnableConfigurationProperties(MinioProperties.class)
@Configuration
public class MinioAutoConfiguration {
    @Autowired
    MinioProperties minioProperties;

    /**
     * generate Minio Client
     *
     * @return Minio Client bean
     */
    @Bean
    @ConditionalOnClass(MinioClient.class)
    public MinioConnectionFactory minioConnectionFactory() {
        MinioClient.Builder builder = MinioClient.builder();
        if (!StringUtils.isEmpty(minioProperties.getRegion())) {
            builder.region(minioProperties.getRegion());
        }
        builder
                .endpoint(minioProperties.getUrl())
                .credentials(minioProperties.getAccessKey(), minioProperties.getSecretKey());
        return new MinioConnectionFactory(builder);
    }

    @Bean
    @ConditionalOnBean(MinioConnectionFactory.class)
    public MinioTemplate minioTemplate(MinioConnectionFactory minioConnectionFactory) {
        return new MinioTemplate(minioConnectionFactory);
    }
}
