package io.github.matian2014.candys3.options;

import org.apache.commons.lang3.StringUtils;

public final class DeleteObjectOptions {
    private final String key;
    private String versionId;
    private boolean bypassGovernanceRetention;

    private DeleteObjectConditionalOptions conditionalOptions;

    public DeleteObjectOptions(String key) {
        this.key = key;
    }

    public DeleteObjectOptions versionId(String versionId) {
        this.versionId = versionId;
        return this;
    }

    public DeleteObjectOptions bypassGovernanceRetention() {
        this.bypassGovernanceRetention = true;
        return this;
    }

    public String getKey() {
        return key;
    }

    public String getVersionId() {
        return versionId;
    }

    public boolean isBypassGovernanceRetention() {
        return bypassGovernanceRetention;
    }

    public DeleteObjectConditionalOptions configureConditionalOptions() {
        if (conditionalOptions == null) {
            conditionalOptions = new DeleteObjectConditionalOptions(this);
        }
        return conditionalOptions;
    }

    public DeleteObjectConditionalOptions getConditionalOptions() {
        return conditionalOptions;
    }

    public static final class DeleteObjectConditionalOptions {
        private final DeleteObjectOptions parent;
        private String ifMatch;

        public DeleteObjectConditionalOptions(DeleteObjectOptions parent) {
            this.parent = parent;
        }

        public DeleteObjectConditionalOptions ifMatch(String etag) {
            this.ifMatch = etag;
            return this;
        }

        public DeleteObjectConditionalOptions matchAny() {
            this.ifMatch = "*";
            return this;
        }

        public String getIfMatch() {
            return ifMatch;
        }

        public DeleteObjectOptions endConfigure() {
            if (StringUtils.isBlank(ifMatch)) {
                throw new IllegalArgumentException("ifMatch must be set");
            }
            parent.conditionalOptions = this;
            return parent;
        }
    }
}
