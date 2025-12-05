package io.github.matian2014.candys3.options;

import org.apache.commons.lang3.StringUtils;

public final class CreateBucketOptions {

    private final String bucket;
    private final String locationConstraint;

    /**
     * Enable object-lock when create bucket is not supported by these providers:
     * 1. Tencent cloud COS
     */
    private final boolean enableObjectLock;

    private CreateBucketOptions(String bucket, String locationConstraint, boolean enableObjectLock) {
        this.bucket = bucket;
        this.locationConstraint = locationConstraint;
        this.enableObjectLock = enableObjectLock;
    }

    public String getBucket() {
        return bucket;
    }

    public String getLocationConstraint() {
        return locationConstraint;
    }

    public boolean isEnableObjectLock() {
        return enableObjectLock;
    }

    public static final class CreateBucketOptionsBuilder {
        private final String bucket;
        private String locationConstraint;
        private boolean enableObjectLock = false;

        public CreateBucketOptionsBuilder(String bucket) {
            this.bucket = bucket;
        }

        public CreateBucketOptionsBuilder locationConstraint(String locationConstraint) {
            this.locationConstraint = locationConstraint;
            return this;
        }

        public CreateBucketOptionsBuilder enableObjectLock() {
            this.enableObjectLock = true;
            return this;
        }

        public CreateBucketOptions build() {
            if (StringUtils.isEmpty(bucket)) {
                throw new IllegalArgumentException("bucket is required.");
            }
            return new CreateBucketOptions(bucket, locationConstraint, enableObjectLock);
        }
    }

}
