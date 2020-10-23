package win.hgfdodo.minio.domain;

import win.hgfdodo.minio.exception.MaformatedObjectNameException;

import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Arrays;
import java.util.List;

public class FileNameAndType {

    public final static List<String> COMMON_IMAGE_SUFFIX = Arrays.asList("bmp", "dib", "gif", "jfif", "jpe", "jpeg", "jpg", "png", "tif", "tiff", "ico");

    public final static String PATH_DELEMITER = "/";
    public final static String FILE_TYPE_DOT = ".";

    private String originObjectName;
    private String filename;
    private String type;

    public FileNameAndType(String objectName) {
        Path path = Paths.get(objectName);
        this.originObjectName = objectName;
        String name = path.getFileName().toString();
        int dotIndex = name.lastIndexOf(FILE_TYPE_DOT);
        if (dotIndex > -1) {
            this.type = name.substring(dotIndex + 1, name.length());
            this.filename = name.substring(0, dotIndex);
        } else {
            throw new MaformatedObjectNameException(objectName);
        }
    }

    public String getFilename() {
        return filename;
    }

    public void setFilename(String filename) {
        this.filename = filename;
    }

    public String getType() {
        return type;
    }

    public void setType(String type) {
        this.type = type;
    }

    /**
     * check object is a picture
     *
     * @return
     */
    public boolean isPicture() {
        if (type != null && !type.equals("") && COMMON_IMAGE_SUFFIX.contains(type)) {
            return true;
        } else {
            return false;
        }
    }

    public String getScaledFileName() {
        return this.originObjectName.replace(this.filename + "." + this.type, this.filename + "-scaled." + this.type);
    }

    public String getImageContentType() {
        switch (type) {
            case "png":
                return "image/png";
            case "jpg":
                return "image/jpg";
            case "svg":
                return "text/xml";
            case "jpeg":
                return "image/jpeg";
            case "gif":
                return "image/gif";
            default:
                return "image/jpg";
        }
    }

    @Override
    public String toString() {
        return "FileNameAndSuffix{" +
                "filename='" + filename + '\'' +
                ", suffix='" + type + '\'' +
                '}';
    }
}
