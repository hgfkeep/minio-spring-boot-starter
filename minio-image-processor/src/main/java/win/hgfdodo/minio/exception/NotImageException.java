package win.hgfdodo.minio.exception;

public class NotImageException extends RuntimeException {
    public NotImageException(String objectName) {
        super(objectName + " is not a picture");
    }
}
