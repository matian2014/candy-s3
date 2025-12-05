package io.github.matian2014.candys3;

import com.fasterxml.jackson.annotation.JsonAlias;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;

import java.util.Arrays;
import java.util.Date;

@JsonIgnoreProperties(ignoreUnknown = true)
public class S3Object {

    @JsonAlias("Key")
    private String key;

    @JsonAlias("Owner")
    private Owner owner;

    // These fields are returned in response body when listObjects.
    // And returned in response header when getObject or headObject.
    @JsonAlias("LastModified")
    private Date lastModified;
    @JsonAlias("ETag")
    private String eTag;
    @JsonAlias("Size")
    private long size;
    @JsonAlias("StorageClass")
    private String storageClass;

    private String versionId;

    private ServerSideEncryptionProperties serverSideEncryptionProperties;
    private ObjectLockProperties objectLockProperties;

    private Integer partsCount;
    private Integer tagCount;

    // When getObject or headObject, get metadata from response headers
    private S3ObjectMetadata objectMetadata;

    // when downloadObject, if data-output is not file or outputStream, object content will save to contentBytes.
    private byte[] contentBytes;

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

    public String getVersionId() {
        return versionId;
    }

    public void setVersionId(String versionId) {
        this.versionId = versionId;
    }

    public ServerSideEncryptionProperties getServerSideEncryptionConfiguration() {
        return serverSideEncryptionProperties;
    }

    public void setServerSideEncryptionConfiguration(ServerSideEncryptionProperties serverSideEncryptionProperties) {
        this.serverSideEncryptionProperties = serverSideEncryptionProperties;
    }

    public Integer getPartsCount() {
        return partsCount;
    }

    public void setPartsCount(Integer partsCount) {
        this.partsCount = partsCount;
    }

    public Integer getTagCount() {
        return tagCount;
    }

    public void setTagCount(Integer tagCount) {
        this.tagCount = tagCount;
    }

    public ObjectLockProperties getObjectLockConfiguration() {
        return objectLockProperties;
    }

    public void setObjectLockConfiguration(ObjectLockProperties objectLockProperties) {
        this.objectLockProperties = objectLockProperties;
    }

    public Owner getOwner() {
        return owner;
    }

    public void setOwner(Owner owner) {
        this.owner = owner;
    }

    public S3ObjectMetadata getObjectMetadata() {
        return objectMetadata;
    }

    public void setObjectMetadata(S3ObjectMetadata objectMetadata) {
        this.objectMetadata = objectMetadata;
    }

    public byte[] getContentBytes() {
        return contentBytes;
    }

    public void setContentBytes(byte[] contentBytes) {
        this.contentBytes = contentBytes;
    }

    @Override
    public String toString() {
        return "S3Object{" +
                "key='" + key + '\'' +
                ", owner=" + owner +
                ", lastModified=" + lastModified +
                ", eTag='" + eTag + '\'' +
                ", size=" + size +
                ", storageClass=" + storageClass +
                ", versionId='" + versionId + '\'' +
                ", serverSideEncryptionProperties=" + serverSideEncryptionProperties +
                ", objectLockProperties=" + objectLockProperties +
                ", partsCount=" + partsCount +
                ", tagCount=" + tagCount +
                ", objectMetadata=" + objectMetadata +
                ", contentBytes=" + Arrays.toString(contentBytes) +
                '}';
    }

    public final static class S3ObjectMetadata {
        private String cacheControl;
        private String contentDisposition;
        private String contentEncoding;
        private String contentLanguage;
        private String contentRange;
        private String contentType;
        private Date expires;

        public String getCacheControl() {
            return cacheControl;
        }

        public void setCacheControl(String cacheControl) {
            this.cacheControl = cacheControl;
        }

        public String getContentDisposition() {
            return contentDisposition;
        }

        public void setContentDisposition(String contentDisposition) {
            this.contentDisposition = contentDisposition;
        }

        public String getContentEncoding() {
            return contentEncoding;
        }

        public void setContentEncoding(String contentEncoding) {
            this.contentEncoding = contentEncoding;
        }

        public String getContentLanguage() {
            return contentLanguage;
        }

        public void setContentLanguage(String contentLanguage) {
            this.contentLanguage = contentLanguage;
        }

        public String getContentRange() {
            return contentRange;
        }

        public void setContentRange(String contentRange) {
            this.contentRange = contentRange;
        }

        public String getContentType() {
            return contentType;
        }

        public void setContentType(String contentType) {
            this.contentType = contentType;
        }

        public Date getExpires() {
            return expires;
        }

        public void setExpires(Date expires) {
            this.expires = expires;
        }

        @Override
        public String toString() {
            return "S3ObjectMetadata{" +
                    "cacheControl='" + cacheControl + '\'' +
                    ", contentDisposition='" + contentDisposition + '\'' +
                    ", contentEncoding='" + contentEncoding + '\'' +
                    ", contentLanguage='" + contentLanguage + '\'' +
                    ", contentRange='" + contentRange + '\'' +
                    ", contentType='" + contentType + '\'' +
                    ", expires=" + expires +
                    '}';
        }
    }
}
