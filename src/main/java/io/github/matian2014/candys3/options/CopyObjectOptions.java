package io.github.matian2014.candys3.options;

import io.github.matian2014.candys3.ObjectRetentionMode;
import io.github.matian2014.candys3.ServerSideEncryptionAlgorithm;
import io.github.matian2014.candys3.StorageClass;
import org.apache.commons.lang3.StringUtils;

import java.util.Date;
import java.util.HashMap;
import java.util.Map;

public final class CopyObjectOptions {

    private final PutObjectHeaderOptions headerProperties;
    private final boolean replaceMetadataDirective;

    private final UpdateServerSideEncryptionOptions serverSideEncryptionOptions;
    private final ObjectLockOptions objectLockOptions;

    private final String copySource;
    private final String copySourceVersionId;
    private final DownloadObjectOptions.DownloadObjectCondition copySourceCondition;

    private final ObjectConditionalWriteOptions writeTargetCondition;

    private final String storageClass;

    private final Map<String, String> tagSet;
    private final boolean replaceTaggingDirective;

    /**
     * Not include header x-amz-tagging-directive if true when send request.
     * Some providers like Cloudflare R2 do not support this header.
     */
    private final boolean excludeTaggingDirective;

    private CopyObjectOptions(PutObjectHeaderOptions headerProperties,
                              boolean replaceMetadataDirective,
                              UpdateServerSideEncryptionOptions serverSideEncryptionOptions,
                              ObjectLockOptions objectLockOptions,
                              String copySource, String copySourceVersionId,
                              DownloadObjectOptions.DownloadObjectCondition copySourceCondition,
                              ObjectConditionalWriteOptions writeTargetCondition,
                              String storageClass,
                              Map<String, String> tagSet, boolean replaceTaggingDirective, boolean excludeTaggingDirective) {
        this.headerProperties = headerProperties;
        this.replaceMetadataDirective = replaceMetadataDirective;
        this.serverSideEncryptionOptions = serverSideEncryptionOptions;
        this.objectLockOptions = objectLockOptions;
        this.copySource = copySource;
        this.copySourceVersionId = copySourceVersionId;
        this.copySourceCondition = copySourceCondition;
        this.writeTargetCondition = writeTargetCondition;
        this.storageClass = storageClass;
        this.tagSet = tagSet;
        this.replaceTaggingDirective = replaceTaggingDirective;
        this.excludeTaggingDirective = excludeTaggingDirective;
    }

    public PutObjectHeaderOptions getHeaderProperties() {
        return headerProperties;
    }

    public boolean isReplaceMetadataDirective() {
        return replaceMetadataDirective;
    }

    public UpdateServerSideEncryptionOptions getServerSideEncryptionOptions() {
        return serverSideEncryptionOptions;
    }

    public ObjectLockOptions getObjectLockOptions() {
        return objectLockOptions;
    }

    public String getCopySource() {
        return copySource;
    }

    public String getCopySourceVersionId() {
        return copySourceVersionId;
    }

    public DownloadObjectOptions.DownloadObjectCondition getCopySourceCondition() {
        return copySourceCondition;
    }

    public ObjectConditionalWriteOptions getWriteTargetCondition() {
        return writeTargetCondition;
    }

    public String getStorageClass() {
        return storageClass;
    }

    public Map<String, String> getTagSet() {
        return tagSet;
    }

    public boolean isReplaceTaggingDirective() {
        return replaceTaggingDirective;
    }

    public boolean isExcludeTaggingDirective() {
        return excludeTaggingDirective;
    }

    public static class CopyObjectOptionsBuilder {
        private PutObjectHeaderOptions objectHeaderOptions;
        private boolean replaceMetadataDirective;

        private UpdateServerSideEncryptionOptions serverSideEncryptionOptions;
        private ObjectLockOptions objectLockOptions;

        private String copySource;
        private String copySourceVersionId;
        private DownloadObjectOptions.DownloadObjectCondition copySourceCondition;
        private ObjectConditionalWriteOptions writeTargetCondition;

        private String storageClass;

        private Map<String, String> tagSet;
        private boolean replaceTaggingDirective;

        private boolean excludeTaggingDirective;

        public CopyObjectOptionsBuilder() {
        }

        public CopyObjectHeaderOptionsBuilder configureCopyObjectHeaderOptions() {
            return new CopyObjectHeaderOptionsBuilder(this);
        }

        public CopyObjectServerSideEncryptionOptionsBuilder configureServerSideEncryptionOptions() {
            return new CopyObjectServerSideEncryptionOptionsBuilder(this);
        }

        public CopyObjectLockOptionsBuilder configureObjectLockOptions() {
            return new CopyObjectLockOptionsBuilder(this);
        }

        public CopySourceConditionOptionsBuilder configureCopySourceCondition() {
            return new CopySourceConditionOptionsBuilder(this);
        }

        public WriteTargetConditionOptionsBuilder configureTargetWriteCondition() {
            return new WriteTargetConditionOptionsBuilder(this);
        }

        public CopyObjectOptionsBuilder copySource(String sourceBucket, String sourceObjectKey) {
            this.copySource = "/" + sourceBucket + "/" + sourceObjectKey;
            return this;
        }

        public CopyObjectOptionsBuilder copySourceVersionId(String copySourceVersionId) {
            this.copySourceVersionId = copySourceVersionId;
            return this;
        }

        public CopyObjectOptionsBuilder storageClass(String storageClass) {
            this.storageClass = storageClass;
            return this;
        }

        public CopyObjectOptionsBuilder storageClass(StorageClass storageClass) {
            this.storageClass = storageClass.name();
            return this;
        }

        public CopyObjectOptionsBuilder replaceTaggingDirective() {
            this.replaceTaggingDirective = true;
            return this;
        }

        public CopyObjectOptionsBuilder excludeTaggingDirective() {
            this.excludeTaggingDirective = true;
            return this;
        }

        public CopyObjectOptionsBuilder addTags(Map<String, String> tags) {
            if (this.tagSet == null) {
                this.tagSet = new HashMap<>();
            }
            this.tagSet.putAll(tags);
            return this;
        }

        public CopyObjectOptionsBuilder addTag(String key, String val) {
            if (this.tagSet == null) {
                this.tagSet = new HashMap<>();
            }
            this.tagSet.put(key, val);
            return this;
        }

        public CopyObjectOptions build() {
            if (StringUtils.isEmpty(copySource)) {
                throw new IllegalArgumentException("copy source is required.");
            }
            return new CopyObjectOptions(objectHeaderOptions, replaceMetadataDirective, serverSideEncryptionOptions, objectLockOptions,
                    copySource, copySourceVersionId, copySourceCondition, writeTargetCondition, storageClass, tagSet, replaceTaggingDirective, excludeTaggingDirective);
        }

        public static final class CopyObjectHeaderOptionsBuilder {
            private final CopyObjectOptionsBuilder parentBuilder;

            CopyObjectHeaderOptionsBuilder(CopyObjectOptionsBuilder parentBuilder) {
                this.parentBuilder = parentBuilder;
                if (this.parentBuilder.objectHeaderOptions == null) {
                    this.parentBuilder.objectHeaderOptions = new PutObjectHeaderOptions();
                }
            }

            public CopyObjectOptionsBuilder endConfigureCopyObjectHeaderOptions() {
                return this.parentBuilder;
            }

            public CopyObjectHeaderOptionsBuilder replaceMetadataDirective() {
                this.parentBuilder.replaceMetadataDirective = true;
                return this;
            }

            public CopyObjectHeaderOptionsBuilder cacheControl(String cacheControl) {
                this.parentBuilder.objectHeaderOptions.cacheControl(cacheControl);
                return this;
            }

            public CopyObjectHeaderOptionsBuilder contentDisposition(String contentDisposition) {
                this.parentBuilder.objectHeaderOptions.contentDisposition(contentDisposition);
                return this;
            }

            public CopyObjectHeaderOptionsBuilder contentEncoding(String contentEncoding) {
                this.parentBuilder.objectHeaderOptions.contentEncoding(contentEncoding);
                return this;
            }


            public CopyObjectHeaderOptionsBuilder contentLanguage(String contentLanguage) {
                this.parentBuilder.objectHeaderOptions.contentLanguage(contentLanguage);
                return this;
            }


            public CopyObjectHeaderOptionsBuilder contentType(String contentType) {
                this.parentBuilder.objectHeaderOptions.contentType(contentType);
                return this;
            }


            public CopyObjectHeaderOptionsBuilder expires(Date expires) {
                this.parentBuilder.objectHeaderOptions.expires(expires);
                return this;
            }
        }

        public static final class CopyObjectServerSideEncryptionOptionsBuilder {
            private final CopyObjectOptionsBuilder parentBuilder;

            private final UpdateServerSideEncryptionOptions.UpdateServerSideEncryptionOptionsBuilder innerBuilder;

            CopyObjectServerSideEncryptionOptionsBuilder(CopyObjectOptionsBuilder parentBuilder) {
                this.parentBuilder = parentBuilder;
                this.innerBuilder = new UpdateServerSideEncryptionOptions.UpdateServerSideEncryptionOptionsBuilder();
            }

            public CopyObjectOptionsBuilder endConfigureServerSideEncryptionOptions() {
                this.parentBuilder.serverSideEncryptionOptions = this.innerBuilder.build();
                return this.parentBuilder;
            }

            public CopyObjectServerSideEncryptionOptionsBuilder sseAlgorithm(String algo) {
                this.innerBuilder.sseAlgorithm(algo);
                return this;
            }

            public CopyObjectServerSideEncryptionOptionsBuilder sseAlgorithm(ServerSideEncryptionAlgorithm algo) {
                this.innerBuilder.sseAlgorithm(algo.getAlgorithm());
                return this;
            }
        }

        public static final class CopyObjectLockOptionsBuilder {
            private final CopyObjectOptionsBuilder parentBuilder;

            CopyObjectLockOptionsBuilder(CopyObjectOptionsBuilder parentBuilder) {
                this.parentBuilder = parentBuilder;
                if (this.parentBuilder.objectLockOptions == null) {
                    this.parentBuilder.objectLockOptions = new ObjectLockOptions();
                }
            }

            public CopyObjectOptionsBuilder endConfigureObjectLockOptions() {
                return this.parentBuilder;
            }

            public CopyObjectLockOptionsBuilder lockMode(ObjectRetentionMode lockMode) {
                this.parentBuilder.objectLockOptions.lockMode(lockMode);
                return this;
            }

            public CopyObjectLockOptionsBuilder retainUntilDate(Date retainUntilDate) {
                this.parentBuilder.objectLockOptions.retainUntilDate(retainUntilDate);
                return this;
            }

            public CopyObjectLockOptionsBuilder legalHold(boolean legalHold) {
                this.parentBuilder.objectLockOptions.legalHold(legalHold);
                return this;
            }

        }

        public static final class CopySourceConditionOptionsBuilder {
            private final CopyObjectOptionsBuilder parentBuilder;

            CopySourceConditionOptionsBuilder(CopyObjectOptionsBuilder parentBuilder) {
                this.parentBuilder = parentBuilder;
                if (this.parentBuilder.copySourceCondition == null) {
                    this.parentBuilder.copySourceCondition = new DownloadObjectOptions.DownloadObjectCondition();
                }
            }

            public CopyObjectOptionsBuilder endConfigureCondition() {
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

        public static final class WriteTargetConditionOptionsBuilder {
            private final CopyObjectOptionsBuilder parentBuilder;

            WriteTargetConditionOptionsBuilder(CopyObjectOptionsBuilder parentBuilder) {
                this.parentBuilder = parentBuilder;
                if (this.parentBuilder.writeTargetCondition == null) {
                    this.parentBuilder.writeTargetCondition = new ObjectConditionalWriteOptions();
                }
            }

            public CopyObjectOptionsBuilder endConfigureCondition() {
                return this.parentBuilder;
            }

            public WriteTargetConditionOptionsBuilder ifMatch(String etag) {
                this.parentBuilder.writeTargetCondition.ifMatch(etag);
                return this;
            }
            public WriteTargetConditionOptionsBuilder ifNotExists() {
                this.parentBuilder.writeTargetCondition.ifNotExists();
                return this;
            }

        }

    }

}
