package io.github.matian2014.candys3.options;

public final class UpdateBucketPublicAccessBlockOptions {

    private final boolean removeBlocks;

    private final boolean blockPublicAcls;
    private final boolean blockPublicPolicy;
    private final boolean ignorePublicAcls;
    private final boolean restrictPublicBuckets;

    private UpdateBucketPublicAccessBlockOptions(boolean removeBlocks,
                                                 boolean blockPublicAcls, boolean blockPublicPolicy,
                                                 boolean ignorePublicAcls, boolean restrictPublicBuckets) {
        this.removeBlocks = removeBlocks;
        this.blockPublicAcls = blockPublicAcls;
        this.blockPublicPolicy = blockPublicPolicy;
        this.ignorePublicAcls = ignorePublicAcls;
        this.restrictPublicBuckets = restrictPublicBuckets;
    }

    public boolean isRemoveBlocks() {
        return removeBlocks;
    }

    public boolean isBlockPublicAcls() {
        return blockPublicAcls;
    }

    public boolean isBlockPublicPolicy() {
        return blockPublicPolicy;
    }

    public boolean isIgnorePublicAcls() {
        return ignorePublicAcls;
    }

    public boolean isRestrictPublicBuckets() {
        return restrictPublicBuckets;
    }

    public static final class UpdateBucketPublicAccessBlockOptionsBuilder {
        private boolean removeBlocks = false;

        private boolean blockPublicAcls = true;
        private boolean blockPublicPolicy = true;
        private boolean ignorePublicAcls = true;
        private boolean restrictPublicBuckets = true;

        public UpdateBucketPublicAccessBlockOptionsBuilder() {
        }

        public UpdateBucketPublicAccessBlockOptions build() {
            return new UpdateBucketPublicAccessBlockOptions(this.removeBlocks,
                    this.blockPublicAcls, this.blockPublicPolicy,
                    this.ignorePublicAcls, this.restrictPublicBuckets);
        }

        public UpdateBucketPublicAccessBlockOptionsBuilder removeAccessBlocks() {
            this.removeBlocks = true;
            return this;
        }

        public UpdateBucketPublicAccessBlockOptionsBuilder blockPublicAcls(boolean block) {
            this.blockPublicAcls = block;
            return this;
        }

        public UpdateBucketPublicAccessBlockOptionsBuilder blockPublicPolicy(boolean block) {
            this.blockPublicPolicy = block;
            return this;
        }

        public UpdateBucketPublicAccessBlockOptionsBuilder ignorePublicAcls(boolean block) {
            this.ignorePublicAcls = block;
            return this;
        }

        public UpdateBucketPublicAccessBlockOptionsBuilder restrictPublicBuckets(boolean block) {
            this.restrictPublicBuckets = block;
            return this;
        }


    }


}
