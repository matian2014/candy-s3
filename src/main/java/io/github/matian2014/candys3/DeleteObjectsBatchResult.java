package io.github.matian2014.candys3;

import com.fasterxml.jackson.annotation.JsonAlias;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;

import java.util.List;

public class DeleteObjectsBatchResult {

    private boolean isSuccessful;
    private List<DeleteError> errors;
    private List<DeletedObject> deleted;

    public boolean isSuccessful() {
        return isSuccessful;
    }

    public void setSuccessful(boolean successful) {
        isSuccessful = successful;
    }

    public List<DeleteError> getErrors() {
        return errors;
    }

    public void setErrors(List<DeleteError> errors) {
        this.errors = errors;
    }

    public List<DeletedObject> getDeleted() {
        return deleted;
    }

    public void setDeleted(List<DeletedObject> deleted) {
        this.deleted = deleted;
    }

    @JsonIgnoreProperties(ignoreUnknown = true)
    public static class DeleteError {
        @JsonAlias("Key")
        private String key;
        @JsonAlias("Code")
        private String code;
        @JsonAlias("Message")
        private String message;
        @JsonAlias("VersionId")
        private String versionId;

        public String getKey() {
            return key;
        }

        public void setKey(String key) {
            this.key = key;
        }

        public String getCode() {
            return code;
        }

        public void setCode(String code) {
            this.code = code;
        }

        public String getMessage() {
            return message;
        }

        public void setMessage(String message) {
            this.message = message;
        }

        public String getVersionId() {
            return versionId;
        }

        public void setVersionId(String versionId) {
            this.versionId = versionId;
        }
    }

    @JsonIgnoreProperties(ignoreUnknown = true)
    public static class DeletedObject {
        @JsonAlias("Key")
        private String key;
        @JsonAlias("VersionId")
        private String versionId;
        @JsonAlias("DeleteMarker")
        private Boolean deleteMarker;
        @JsonAlias("DeleteMarkerVersionId")
        private String deleteMarkerVersionId;

        public String getKey() {
            return key;
        }

        public void setKey(String key) {
            this.key = key;
        }

        public String getVersionId() {
            return versionId;
        }

        public void setVersionId(String versionId) {
            this.versionId = versionId;
        }

        public Boolean getDeleteMarker() {
            return deleteMarker;
        }

        public void setDeleteMarker(Boolean deleteMarker) {
            this.deleteMarker = deleteMarker;
        }

        public String getDeleteMarkerVersionId() {
            return deleteMarkerVersionId;
        }

        public void setDeleteMarkerVersionId(String deleteMarkerVersionId) {
            this.deleteMarkerVersionId = deleteMarkerVersionId;
        }
    }

}
