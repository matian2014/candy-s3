package io.github.matian2014.candys3.options;

import org.apache.commons.lang3.StringUtils;

import java.util.Date;

public final class CopyPartOptions {

    private final String copySource;
    private final String copySourceVersionId;
    private final String copySourceRange; // You can copy a range only if the source object is greater than 5 MB.
    private final DownloadObjectOptions.DownloadObjectCondition copySourceCondition;

    private CopyPartOptions(String copySource, String copySourceVersionId, String copySourceRange, DownloadObjectOptions.DownloadObjectCondition copySourceCondition) {
        this.copySource = copySource;
        this.copySourceVersionId = copySourceVersionId;
        this.copySourceRange = copySourceRange;
        this.copySourceCondition = copySourceCondition;
    }

    public String getCopySource() {
        return copySource;
    }

    public String getCopySourceVersionId() {
        return copySourceVersionId;
    }

    public String getCopySourceRange() {
        return copySourceRange;
    }

    public DownloadObjectOptions.DownloadObjectCondition getCopySourceCondition() {
        return copySourceCondition;
    }

    public static class CopyPartOptionsBuilder {
        private String copySource;
        private String copySourceVersionId;
        private String copySourceRange;
        private DownloadObjectOptions.DownloadObjectCondition copySourceCondition;

        public CopyPartOptionsBuilder() {
        }

        public CopyPartOptionsBuilder copySource(String bucket, String objectKey) {
            this.copySource = "/" + bucket + "/" + objectKey;
            return this;
        }

        public CopyPartOptionsBuilder copySource(String bucket, String objectKey, String copySourceVersionId) {
            this.copySource = "/" + bucket + "/" + objectKey;
            this.copySourceVersionId = copySourceVersionId;
            return this;
        }

        public CopyPartOptionsBuilder copySourceRange(int startBytes, int endBytes) {
            if (startBytes < 0) {
                throw new IllegalArgumentException("Range start-bytes must be a positive integer or 0.");
            }
            if (endBytes <= startBytes) {
                throw new IllegalArgumentException("Range end-bytes must be a positive integer and greater to start-bytes.");
            }
            this.copySourceRange = startBytes + "-" + endBytes;
            return this;
        }

        public CopySourceConditionOptionsBuilder configureCopySourceCondition() {
            return new CopySourceConditionOptionsBuilder(this);
        }

        public CopyPartOptions build() {
            if (StringUtils.isEmpty(copySource)) {
                throw new IllegalArgumentException("copy source is required.");
            }
            return new CopyPartOptions(copySource, copySourceVersionId, copySourceRange, copySourceCondition);
        }

        public static final class CopySourceConditionOptionsBuilder {
            private final CopyPartOptionsBuilder parentBuilder;

            CopySourceConditionOptionsBuilder(CopyPartOptionsBuilder parentBuilder) {
                this.parentBuilder = parentBuilder;
                if (this.parentBuilder.copySourceCondition == null) {
                    this.parentBuilder.copySourceCondition = new DownloadObjectOptions.DownloadObjectCondition();
                }
            }

            public CopyPartOptionsBuilder endConfigureCondition() {
                return this.parentBuilder;
            }

            public CopySourceConditionOptionsBuilder ifMatch(String etag) {
                this.parentBuilder.copySourceCondition.ifMatch(etag);
                return this;
            }

            public CopySourceConditionOptionsBuilder ifNoneMatch(String etag) {
                this.parentBuilder.copySourceCondition.ifNoneMatch(etag);
                return this;
            }

            public CopySourceConditionOptionsBuilder ifModifiedSince(Date modifiedSince) {
                this.parentBuilder.copySourceCondition.ifModifiedSince(modifiedSince);
                return this;
            }

            public CopySourceConditionOptionsBuilder ifUnmodifiedSince(Date unmodifiedSince) {
                this.parentBuilder.copySourceCondition.ifUnmodifiedSince(unmodifiedSince);
                return this;
            }

        }

    }

}
