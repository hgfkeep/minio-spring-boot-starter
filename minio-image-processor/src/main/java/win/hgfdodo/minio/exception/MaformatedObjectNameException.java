package win.hgfdodo.minio.exception;

public class MaformatedObjectNameException extends RuntimeException {
    public MaformatedObjectNameException(String objectname) {
        super("mafotmated object name:" + objectname);
    }
}
