package win.hgfdodo.minio.connection;

import io.minio.MinioClient;

/**
 * minio connection factory
 *
 * @author Guangfu He
 */
public class MinioConnectionFactory {
    private final MinioClient.Builder builder;

    public MinioConnectionFactory(MinioClient.Builder builder) {
        this.builder = builder;
    }

    public MinioClient getConnection() {
        return builder.build();
    }
}
