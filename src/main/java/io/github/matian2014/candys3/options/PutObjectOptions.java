package io.github.matian2014.candys3.options;

import io.github.matian2014.candys3.ObjectRetentionMode;
import io.github.matian2014.candys3.ServerSideEncryptionAlgorithm;
import io.github.matian2014.candys3.StorageClass;

import java.io.InputStream;
import java.util.Collections;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;

public final class PutObjectOptions {

    private final PutObjectHeaderOptions headerProperties;
    private final UpdateServerSideEncryptionOptions serverSideEncryptionOptions;
    private final ObjectLockOptions objectLockOptions;

    private final ObjectConditionalWriteOptions condition;
    private final ObjectDataContentOptions objectDataContentOptions;

    private final String storageClass;
    private final Map<String, String> tagSet;

    private PutObjectOptions(PutObjectHeaderOptions headerProperties,
                             UpdateServerSideEncryptionOptions serverSideEncryptionOptions, ObjectLockOptions objectLockOptions,
                             ObjectConditionalWriteOptions condition, ObjectDataContentOptions objectDataContentOptions,
                             String storageClass, Map<String, String> tagSet) {
        this.headerProperties = headerProperties;
        this.serverSideEncryptionOptions = serverSideEncryptionOptions;
        this.objectLockOptions = objectLockOptions;
        this.condition = condition;
        this.objectDataContentOptions = objectDataContentOptions;
        this.storageClass = storageClass;
        this.tagSet = tagSet;
    }

    public PutObjectHeaderOptions getHeaderProperties() {
        return headerProperties;
    }

    public String getStorageClass() {
        return storageClass;
    }

    public Map<String, String> getTagSet() {
        return tagSet == null ? null : Collections.unmodifiableMap(tagSet);
    }

    public UpdateServerSideEncryptionOptions getServerSideEncryptionOptions() {
        return serverSideEncryptionOptions;
    }

    public ObjectLockOptions getObjectLockOptions() {
        return objectLockOptions;
    }

    public ObjectDataContentOptions getObjectDataContentOptions() {
        return objectDataContentOptions;
    }

    public ObjectConditionalWriteOptions getCondition() {
        return condition;
    }

    public static class PutObjectOptionsBuilder {
        private String storageClass;
        private Map<String, String> tagSet;

        private PutObjectHeaderOptions objectHeaderOptions;
        private UpdateServerSideEncryptionOptions serverSideEncryptionOptions;
        private ObjectLockOptions objectLockOptions;

        private ObjectConditionalWriteOptions conditionalWriteOptions;
        private ObjectDataContentOptions dataOutput;

        public PutObjectOptionsBuilder() {
        }

        public PutObjectHeaderOptionsBuilder configurePutObjectHeaderOptions() {
            return new PutObjectHeaderOptionsBuilder(this);
        }

        public PutObjectServerSideEncryptionOptionsBuilder configureServerSideEncryptionOptions() {
            return new PutObjectServerSideEncryptionOptionsBuilder(this);
        }

        public PutObjectLockOptionsBuilder configureObjectLockOptions() {
            return new PutObjectLockOptionsBuilder(this);
        }

        public PutObjectConditionalWriteOptionsBuilder configureConditionalWrite() {
            return new PutObjectConditionalWriteOptionsBuilder(this);
        }

        public PutObjectDataContentBuilder configureUploadData() {
            return new PutObjectDataContentBuilder(this);
        }

        public PutObjectOptionsBuilder storageClass(String storageClass) {
            this.storageClass = storageClass;
            return this;
        }

        public PutObjectOptionsBuilder storageClass(StorageClass storageClass) {
            this.storageClass = storageClass.name();
            return this;
        }

        public PutObjectOptionsBuilder addTags(Map<String, String> tags) {
            if (this.tagSet == null) {
                this.tagSet = new HashMap<>();
            }
            this.tagSet.putAll(tags);
            return this;
        }

        public PutObjectOptionsBuilder addTag(String key, String val) {
            if (this.tagSet == null) {
                this.tagSet = new HashMap<>();
            }
            this.tagSet.put(key, val);
            return this;
        }

        public PutObjectOptions build() {
            return new PutObjectOptions(objectHeaderOptions, serverSideEncryptionOptions, objectLockOptions,
                    conditionalWriteOptions, dataOutput, storageClass, tagSet);
        }

        public static final class PutObjectHeaderOptionsBuilder {
            private final PutObjectOptionsBuilder parentBuilder;

            PutObjectHeaderOptionsBuilder(PutObjectOptionsBuilder parentBuilder) {
                this.parentBuilder = parentBuilder;
                if (this.parentBuilder.objectHeaderOptions == null) {
                    this.parentBuilder.objectHeaderOptions = new PutObjectHeaderOptions();
                }
            }

            public PutObjectOptionsBuilder endConfigurePutObjectHeaderOptions() {
                return this.parentBuilder;
            }

            public PutObjectHeaderOptionsBuilder cacheControl(String cacheControl) {
                this.parentBuilder.objectHeaderOptions.cacheControl(cacheControl);
                return this;
            }

            public PutObjectHeaderOptionsBuilder contentDisposition(String contentDisposition) {
                this.parentBuilder.objectHeaderOptions.contentDisposition(contentDisposition);
                return this;
            }

            public PutObjectHeaderOptionsBuilder contentEncoding(String contentEncoding) {
                this.parentBuilder.objectHeaderOptions.contentEncoding(contentEncoding);
                return this;
            }


            public PutObjectHeaderOptionsBuilder contentLanguage(String contentLanguage) {
                this.parentBuilder.objectHeaderOptions.contentLanguage(contentLanguage);
                return this;
            }


            public PutObjectHeaderOptionsBuilder contentType(String contentType) {
                this.parentBuilder.objectHeaderOptions.contentType(contentType);
                return this;
            }


            public PutObjectHeaderOptionsBuilder expires(Date expires) {
                this.parentBuilder.objectHeaderOptions.expires(expires);
                return this;
            }
        }

        public static final class PutObjectServerSideEncryptionOptionsBuilder {
            private final PutObjectOptionsBuilder parentBuilder;

            private final UpdateServerSideEncryptionOptions.UpdateServerSideEncryptionOptionsBuilder innerBuilder;

            PutObjectServerSideEncryptionOptionsBuilder(PutObjectOptionsBuilder parentBuilder) {
                this.parentBuilder = parentBuilder;
                this.innerBuilder = new UpdateServerSideEncryptionOptions.UpdateServerSideEncryptionOptionsBuilder();
            }

            public PutObjectOptionsBuilder endConfigureServerSideEncryptionOptions() {
                this.parentBuilder.serverSideEncryptionOptions = this.innerBuilder.build();
                return this.parentBuilder;
            }

            public PutObjectServerSideEncryptionOptionsBuilder sseAlgorithm(String algo) {
                this.innerBuilder.sseAlgorithm(algo);
                return this;
            }
            public PutObjectServerSideEncryptionOptionsBuilder sseAlgorithm(ServerSideEncryptionAlgorithm algo) {
                this.innerBuilder.sseAlgorithm(algo.getAlgorithm());
                return this;
            }
        }

        public static final class PutObjectLockOptionsBuilder {
            private final PutObjectOptionsBuilder parentBuilder;

            PutObjectLockOptionsBuilder(PutObjectOptionsBuilder parentBuilder) {
                this.parentBuilder = parentBuilder;
                if (this.parentBuilder.objectLockOptions == null) {
                    this.parentBuilder.objectLockOptions = new ObjectLockOptions();
                }
            }

            public PutObjectOptionsBuilder endConfigureObjectLockOptions() {
                return this.parentBuilder;
            }

            public PutObjectLockOptionsBuilder lockMode(ObjectRetentionMode lockMode) {
                this.parentBuilder.objectLockOptions.lockMode(lockMode);
                return this;
            }

            public PutObjectLockOptionsBuilder retainUntilDate(Date retainUntilDate) {
                this.parentBuilder.objectLockOptions.retainUntilDate(retainUntilDate);
                return this;
            }

            public PutObjectLockOptionsBuilder legalHold(boolean legalHold) {
                this.parentBuilder.objectLockOptions.legalHold(legalHold);
                return this;
            }

        }

        public static final class PutObjectDataContentBuilder {
            private final PutObjectOptionsBuilder parentBuilder;
            private final ObjectDataContentOptions.ObjectDataContentOptionsBuilder innerBuilder;

            PutObjectDataContentBuilder(PutObjectOptionsBuilder parentBuilder) {
                this.parentBuilder = parentBuilder;
                this.innerBuilder = new ObjectDataContentOptions.ObjectDataContentOptionsBuilder();
            }

            public PutObjectOptionsBuilder endConfigureDataContent() {
                this.parentBuilder.dataOutput = this.innerBuilder.build();
                return this.parentBuilder;
            }

            public PutObjectDataContentBuilder withData(byte[] bytes) {
                this.innerBuilder.withData(bytes);
                return this;
            }

            public PutObjectDataContentBuilder withData(InputStream in) {
                this.innerBuilder.withData(in);
                return this;
            }

            public PutObjectDataContentBuilder withData(String file) {
                this.innerBuilder.withData(file);
                return this;
            }
        }

        public static final class PutObjectConditionalWriteOptionsBuilder {
            private final PutObjectOptionsBuilder parentBuilder;

            PutObjectConditionalWriteOptionsBuilder(PutObjectOptionsBuilder parentBuilder) {
                this.parentBuilder = parentBuilder;
                if (this.parentBuilder.conditionalWriteOptions == null) {
                    this.parentBuilder.conditionalWriteOptions = new ObjectConditionalWriteOptions();
                }
            }

            public PutObjectOptionsBuilder endConfigureCondition() {
                return this.parentBuilder;
            }

            public PutObjectConditionalWriteOptionsBuilder ifMatch(String etag) {
                this.parentBuilder.conditionalWriteOptions.ifMatch(etag);
                return this;
            }

            public PutObjectConditionalWriteOptionsBuilder ifNotExists() {
                this.parentBuilder.conditionalWriteOptions.ifNotExists();
                return this;
            }

        }

    }

}
