package io.github.matian2014.candys3;

import com.fasterxml.jackson.annotation.JsonAlias;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;

import java.util.Date;

@JsonIgnoreProperties(ignoreUnknown = true)
public class S3ObjectVersion {

    @JsonAlias("Key")
    private String key;

    @JsonAlias("VersionId")
    private String versionId;

    @JsonAlias("IsLatest")
    private Boolean isLatest;

    @JsonAlias("LastModified")
    private Date lastModified;

    @JsonAlias("ETag")
    private String eTag;

    @JsonAlias("Size")
    private long size;

    @JsonAlias("StorageClass")
    private String storageClass;

    @JsonAlias("Owner")
    private Owner owner;

    public String getKey() {
        return key;
    }

    public void setKey(String key) {
        this.key = key;
    }

    public Date getLastModified() {
        return lastModified;
    }

    public void setLastModified(Date lastModified) {
        this.lastModified = lastModified;
    }

    public Boolean getIsLatest() {
        return isLatest;
    }

    public void setIsLatest(Boolean latest) {
        isLatest = latest;
    }

    public String geteTag() {
        return eTag;
    }

    public void seteTag(String eTag) {
        this.eTag = eTag;
    }

    public long getSize() {
        return size;
    }

    public void setSize(long size) {
        this.size = size;
    }

    public String getStorageClass() {
        return storageClass;
    }

    public void setStorageClass(String storageClass) {
        this.storageClass = storageClass;
    }

    public Owner getOwner() {
        return owner;
    }

    public void setOwner(Owner owner) {
        this.owner = owner;
    }

    public String getVersionId() {
        return versionId;
    }

    public void setVersionId(String versionId) {
        this.versionId = versionId;
    }

    @Override
    public String toString() {
        return "S3ObjectVersion{" +
                "key='" + key + '\'' +
                ", versionId='" + versionId + '\'' +
                ", isLatest=" + isLatest +
                ", lastModified=" + lastModified +
                ", eTag='" + eTag + '\'' +
                ", size=" + size +
                ", storageClass=" + storageClass +
                ", owner=" + owner +
                '}';
    }
}
