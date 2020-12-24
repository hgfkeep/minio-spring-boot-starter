package win.hgfdodo.minio.config;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.context.annotation.Configuration;
import org.springframework.http.converter.HttpMessageConverter;
import org.springframework.web.servlet.config.annotation.WebMvcConfigurer;
import win.hgfdodo.minio.endpoint.converter.MinioResourceRegionHttpMessageConverter;

import java.util.List;

/**
 * @author Guangfu He
 * @date 2020/12/23 11:22
 * @email hgfkeep@gmail.com
 */
@Configuration
public class WebMvcConfig implements WebMvcConfigurer {
    private final static Logger log = LoggerFactory.getLogger(WebMvcConfig.class);
    private final MinioResourceRegionHttpMessageConverter convertor;

    public WebMvcConfig(MinioResourceRegionHttpMessageConverter convertor) {
        this.convertor = convertor;
    }

    @Override
    public void configureMessageConverters(List<HttpMessageConverter<?>> converters) {
        converters.add(convertor);
    }
}
