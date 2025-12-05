package io.github.matian2014.candys3;

import com.fasterxml.jackson.annotation.JsonAlias;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;

import java.util.Date;

@JsonIgnoreProperties(ignoreUnknown = true)
public class S3MultipartUpload {

    @JsonAlias("Key")
    private String key;
    @JsonAlias("UploadId")
    private String uploadId;

    @JsonAlias("Initiated")
    private Date initiated;
    @JsonAlias("Initiator")
    private Owner initiator;

    @JsonAlias("Owner")
    private Owner owner;

    public String getKey() {
        return key;
    }

    public void setKey(String key) {
        this.key = key;
    }

    public String getUploadId() {
        return uploadId;
    }

    public void setUploadId(String uploadId) {
        this.uploadId = uploadId;
    }

    public Date getInitiated() {
        return initiated;
    }

    public void setInitiated(Date initiated) {
        this.initiated = initiated;
    }

    public Owner getInitiator() {
        return initiator;
    }

    public void setInitiator(Owner initiator) {
        this.initiator = initiator;
    }

    public Owner getOwner() {
        return owner;
    }

    public void setOwner(Owner owner) {
        this.owner = owner;
    }

    public static class S3MultipartUploadNextPaginationMarker {
        private String nextKeyMarker;
        private String nextUploadIdMarker;

        public S3MultipartUploadNextPaginationMarker() {
        }

        public String getNextKeyMarker() {
            return nextKeyMarker;
        }

        public void setNextKeyMarker(String nextKeyMarker) {
            this.nextKeyMarker = nextKeyMarker;
        }

        public String getNextUploadIdMarker() {
            return nextUploadIdMarker;
        }

        public void setNextUploadIdMarker(String nextUploadIdMarker) {
            this.nextUploadIdMarker = nextUploadIdMarker;
        }
    }
}
