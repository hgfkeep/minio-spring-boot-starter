package win.hgfdodo.minio.config;

import org.springframework.boot.context.properties.ConfigurationProperties;

/**
 * Minio server connection settings
 *
 * @author Guangfu He
 */
@ConfigurationProperties("spring.minio")
public class MinioProperties {
    /**
     * minio URL, it should be a  URL, domain name, IPv4 address or IPv6 address
     */
    private String url;
    /**
     * uniquely identifies a minio account.
     */
    private String accessKey;
    /**
     * the password to a minio account.
     */
    private String secretKey;

    /**
     * Not Required, minio server region info， used with cloud minio
     */
    private String region;

    public String getUrl() {
        return url;
    }

    public void setUrl(String url) {
        this.url = url;
    }

    public String getAccessKey() {
        return accessKey;
    }

    public void setAccessKey(String accessKey) {
        this.accessKey = accessKey;
    }

    public String getSecretKey() {
        return secretKey;
    }

    public void setSecretKey(String secretKey) {
        this.secretKey = secretKey;
    }

    public String getRegion() {
        return region;
    }

    public void setRegion(String region) {
        this.region = region;
    }

    @Override
    public String toString() {
        return "MinioProperties{" +
                "url='" + url + '\'' +
                ", accessKey='" + accessKey + '\'' +
                ", secretKey='" + secretKey + '\'' +
                ", region='" + region + '\'' +
                '}';
    }
}
