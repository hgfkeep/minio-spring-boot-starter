package win.hgfdodo.minio.model;

import win.hgfdodo.minio.exception.ObjectNameFormatException;

public class Location {
    private final static String FILE_TYPE_DELIMETER = ".";
    private final static String PATH_DELIMETER = "/";

    private String bucketName;
    private String objectName;
    private String fileType;

    public Location(String bucketName, String objectName) {
        this.bucketName = bucketName;
        this.objectName = objectName;
        int index = objectName.lastIndexOf(FILE_TYPE_DELIMETER);
        if (index > -1) {
            this.fileType = objectName.substring(index + 1);
        }
    }

    /**
     * locationString
     *
     * @param locationString 可能类似 /bucket/a/b/path/x.jpg； 也可能是 bucket/a/b/path/x.jpg
     */
    public Location(String locationString) {
        int start = 0;
        if (locationString.startsWith(PATH_DELIMETER)) {
            start = 1;
        }
        int index = locationString.indexOf(PATH_DELIMETER, start);
        if (index >= start && index < locationString.length()) {
            this.bucketName = locationString.substring(start, index);
            this.objectName = locationString.substring(index + 1);
        }else{
            throw new ObjectNameFormatException(locationString);
        }
    }

    private String getBucketName() {
        return this.bucketName;
    }

    private String getFileType() {
        return this.getFileType();
    }

    public String getObjectName() {
        return objectName;
    }

}
