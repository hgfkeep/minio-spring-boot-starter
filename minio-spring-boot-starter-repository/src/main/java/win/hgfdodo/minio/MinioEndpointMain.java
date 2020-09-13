package win.hgfdodo.minio;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.autoconfigure.jdbc.DataSourceAutoConfiguration;

@SpringBootApplication(exclude = DataSourceAutoConfiguration.class)
public class MinioEndpointMain {
    public static void main(String[] args) {
        SpringApplication.run(MinioEndpointMain.class, args);
    }
}
