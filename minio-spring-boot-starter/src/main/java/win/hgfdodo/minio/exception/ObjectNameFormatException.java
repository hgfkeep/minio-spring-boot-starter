package win.hgfdodo.minio.exception;

public class ObjectNameFormatException extends RuntimeException {
    public ObjectNameFormatException(String location) {
        super("object name format error:" + location);
    }
}
