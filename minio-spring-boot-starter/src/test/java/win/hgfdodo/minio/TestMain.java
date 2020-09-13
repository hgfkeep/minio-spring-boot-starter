package win.hgfdodo.minio;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

@SpringBootApplication(scanBasePackages = "win.hgfdodo.minio")
public class TestMain {
    public static void main(String[] args) {
        SpringApplication.run(TestMain.class, args);
    }
}
