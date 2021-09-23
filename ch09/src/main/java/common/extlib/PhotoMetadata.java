package common.extlib;

import java.io.Serializable;
import java.nio.file.attribute.FileTime;
import java.sql.Timestamp;
import java.util.Date;

import common.utils.SparkColumn;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class PhotoMetadata implements Serializable {
    private static transient Logger log = LoggerFactory.getLogger(
            PhotoMetadata.class);
    private static final long serialVersionUID = -2589804417011601051L;

    private Timestamp dateTaken;
    private String directory;
    private String extension;
    private Timestamp fileCreationDate;
    private Timestamp fileLastAccessDate;
    private Timestamp fileLastModifiedDate;
    private String filename;
    private Float geoX;
    private Float geoY;
    private Float geoZ;
    private int height;
    private String mimeType;
    private String name;
    private long size;
    private int width;

    @SparkColumn(name = "Date")
    public Timestamp getDateTaken() {
        return dateTaken;
    }

    public String getDirectory() {
        return directory;
    }

    public String getExtension() {
        return extension;
    }

    public Timestamp getFileCreationDate() {
        return fileCreationDate;
    }

    public Timestamp getFileLastAccessDate() {
        return fileLastAccessDate;
    }

    public Timestamp getFileLastModifiedDate() {
        return fileLastModifiedDate;
    }

    @SparkColumn(nullable = false)
    public String getFilename() {
        return filename;
    }

    @SparkColumn(type = "float")
    public Float getGeoX() {
        return geoX;
    }

    public Float getGeoY() {
        return geoY;
    }

    public Float getGeoZ() {
        return geoZ;
    }

    public int getHeight() {
        return height;
    }

    public String getMimeType() {
        return mimeType;
    }

    public String getName() {
        return name;
    }

    public long getSize() {
        return size;
    }

    public int getWidth() {
        return width;
    }

    public void setDateTaken(Date date) {
        if (date == null) {
            log.warn("Attempt to set a null date.");
            return;
        }
        setDateTaken(new Timestamp(date.getTime()));
    }

    public void setDateTaken(Timestamp dateTaken) {
        this.dateTaken = dateTaken;
    }

    public void setDirectory(String directory) {
        this.directory = directory;
    }

    public void setExtension(String extension) {
        this.extension = extension;
    }

    public void setFileCreationDate(FileTime creationTime) {
        setFileCreationDate(new Timestamp(creationTime.toMillis()));
    }

    public void setFileCreationDate(Timestamp fileCreationDate) {
        this.fileCreationDate = fileCreationDate;
    }

    public void setFileLastAccessDate(FileTime lastAccessTime) {
        setFileLastAccessDate(new Timestamp(lastAccessTime.toMillis()));
    }

    public void setFileLastAccessDate(Timestamp fileLastAccessDate) {
        this.fileLastAccessDate = fileLastAccessDate;
    }

    public void setFileLastModifiedDate(FileTime lastModifiedTime) {
        setFileLastModifiedDate(new Timestamp(lastModifiedTime.toMillis()));
    }

    public void setFileLastModifiedDate(Timestamp fileLastModifiedDate) {
        this.fileLastModifiedDate = fileLastModifiedDate;
    }

    public void setFilename(String filename) {
        this.filename = filename;
    }

    public void setGeoX(Float geoX) {
        this.geoX = geoX;
    }

    public void setGeoY(Float geoY) {
        this.geoY = geoY;
    }

    public void setGeoZ(Float geoZ) {
        this.geoZ = geoZ;
    }

    public void setHeight(int height) {
        this.height = height;
    }

    public void setMimeType(String mimeType) {
        this.mimeType = mimeType;
    }

    public void setName(String name) {
        this.name = name;
    }

    public void setSize(long size) {
        this.size = size;
    }

    public void setWidth(int width) {
        this.width = width;
    }
}
