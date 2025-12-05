package io.github.matian2014.candys3.options;

import io.github.matian2014.candys3.ObjectRetentionMode;
import io.github.matian2014.candys3.ServerSideEncryptionAlgorithm;
import io.github.matian2014.candys3.StorageClass;

import java.util.Collections;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;

public final class CreateMultipartUploadOptions {

    private final PutObjectHeaderOptions objectHeaderOptions;

    private final String storageClass;
    private final Map<String, String> tagSet;

    private final UpdateServerSideEncryptionOptions serverSideEncryptionOptions;
    private final ObjectLockOptions objectLockOptions;

    private CreateMultipartUploadOptions(PutObjectHeaderOptions objectHeaderOptions,
                                         UpdateServerSideEncryptionOptions serverSideEncryptionOptions,
                                         ObjectLockOptions objectLockOptions,
                                         String storageClass, Map<String, String> tagSet) {
        this.objectHeaderOptions = objectHeaderOptions;
        this.serverSideEncryptionOptions = serverSideEncryptionOptions;
        this.objectLockOptions = objectLockOptions;
        this.storageClass = storageClass;
        this.tagSet = tagSet;
    }

    public PutObjectHeaderOptions getObjectHeaderOptions() {
        return objectHeaderOptions;
    }

    public String getStorageClass() {
        return storageClass;
    }

    public Map<String, String> getTagSet() {
        return this.tagSet == null ? null : Collections.unmodifiableMap(this.tagSet);
    }

    public UpdateServerSideEncryptionOptions getServerSideEncryptionOptions() {
        return serverSideEncryptionOptions;
    }

    public ObjectLockOptions getObjectLockOptions() {
        return objectLockOptions;
    }

    public static final class CreateMultipartUploadOptionsBuilder {
        private String storageClass;
        private Map<String, String> tagSet;

        private PutObjectHeaderOptions objectHeaderOptions;
        private UpdateServerSideEncryptionOptions serverSideEncryptionOptions;
        private ObjectLockOptions objectLockOptions;

        public CreateMultipartUploadOptionsBuilder() {
        }

        public CreateMultipartUploadPutObjectHeaderOptionsBuilder configurePutObjectHeaderOptions() {
            return new CreateMultipartUploadPutObjectHeaderOptionsBuilder(this);
        }

        public CreateMultipartUploadServerSideEncryptionOptionsBuilder configureServerSideEncryptionOptions() {
            return new CreateMultipartUploadServerSideEncryptionOptionsBuilder(this);
        }

        public CreateMultipartUploadObjectLockOptionsBuilder configureObjectLockOptions() {
            return new CreateMultipartUploadObjectLockOptionsBuilder(this);
        }

        public CreateMultipartUploadOptionsBuilder storageClass(String storageClass) {
            this.storageClass = storageClass;
            return this;
        }

        public CreateMultipartUploadOptionsBuilder storageClass(StorageClass storageClass) {
            this.storageClass = storageClass.name();
            return this;
        }

        public CreateMultipartUploadOptionsBuilder addTags(Map<String, String> tags) {
            if (this.tagSet == null) {
                this.tagSet = new HashMap<>();
            }
            this.tagSet.putAll(tags);
            return this;
        }

        public CreateMultipartUploadOptionsBuilder addTag(String key, String val) {
            if (this.tagSet == null) {
                this.tagSet = new HashMap<>();
            }
            this.tagSet.put(key, val);
            return this;
        }

        public CreateMultipartUploadOptions build() {
            return new CreateMultipartUploadOptions(objectHeaderOptions, serverSideEncryptionOptions, objectLockOptions,
                    this.storageClass, this.tagSet);
        }

        public static final class CreateMultipartUploadPutObjectHeaderOptionsBuilder {
            private final CreateMultipartUploadOptionsBuilder parentBuilder;

            CreateMultipartUploadPutObjectHeaderOptionsBuilder(CreateMultipartUploadOptionsBuilder parentBuilder) {
                this.parentBuilder = parentBuilder;
                if (this.parentBuilder.objectHeaderOptions == null) {
                    this.parentBuilder.objectHeaderOptions = new PutObjectHeaderOptions();
                }
            }

            public CreateMultipartUploadOptionsBuilder endConfigureObjectHeaderOptions() {
                return this.parentBuilder;
            }

            public CreateMultipartUploadPutObjectHeaderOptionsBuilder cacheControl(String cacheControl) {
                this.parentBuilder.objectHeaderOptions.cacheControl(cacheControl);
                return this;
            }

            public CreateMultipartUploadPutObjectHeaderOptionsBuilder contentDisposition(String contentDisposition) {
                this.parentBuilder.objectHeaderOptions.contentDisposition(contentDisposition);
                return this;
            }

            public CreateMultipartUploadPutObjectHeaderOptionsBuilder contentEncoding(String contentEncoding) {
                this.parentBuilder.objectHeaderOptions.contentEncoding(contentEncoding);
                return this;
            }


            public CreateMultipartUploadPutObjectHeaderOptionsBuilder contentLanguage(String contentLanguage) {
                this.parentBuilder.objectHeaderOptions.contentLanguage(contentLanguage);
                return this;
            }


            public CreateMultipartUploadPutObjectHeaderOptionsBuilder contentType(String contentType) {
                this.parentBuilder.objectHeaderOptions.contentType(contentType);
                return this;
            }


            public CreateMultipartUploadPutObjectHeaderOptionsBuilder expires(Date expires) {
                this.parentBuilder.objectHeaderOptions.expires(expires);
                return this;
            }
        }

        public static final class CreateMultipartUploadServerSideEncryptionOptionsBuilder {
            private final CreateMultipartUploadOptionsBuilder parentBuilder;
            private final UpdateServerSideEncryptionOptions.UpdateServerSideEncryptionOptionsBuilder innerBuilder;

            CreateMultipartUploadServerSideEncryptionOptionsBuilder(CreateMultipartUploadOptionsBuilder parentBuilder) {
                this.parentBuilder = parentBuilder;
                this.innerBuilder = new UpdateServerSideEncryptionOptions.UpdateServerSideEncryptionOptionsBuilder();
            }

            public CreateMultipartUploadOptionsBuilder endConfigureServerSideEncryptionOptions() {
                this.parentBuilder.serverSideEncryptionOptions = this.innerBuilder.build();
                return this.parentBuilder;
            }

            public CreateMultipartUploadServerSideEncryptionOptionsBuilder sseAlgorithm(String algo) {
                this.innerBuilder.sseAlgorithm(algo);
                return this;
            }
            public CreateMultipartUploadServerSideEncryptionOptionsBuilder sseAlgorithm(ServerSideEncryptionAlgorithm algo) {
                this.innerBuilder.sseAlgorithm(algo.getAlgorithm());
                return this;
            }
        }

        public static final class CreateMultipartUploadObjectLockOptionsBuilder {
            private final CreateMultipartUploadOptionsBuilder parentBuilder;

            CreateMultipartUploadObjectLockOptionsBuilder(CreateMultipartUploadOptionsBuilder parentBuilder) {
                this.parentBuilder = parentBuilder;
                if (this.parentBuilder.objectLockOptions == null) {
                    this.parentBuilder.objectLockOptions = new ObjectLockOptions();
                }
            }

            public CreateMultipartUploadOptionsBuilder endConfigureObjectLockOptions() {
                return this.parentBuilder;
            }

            public CreateMultipartUploadObjectLockOptionsBuilder lockMode(ObjectRetentionMode lockMode) {
                this.parentBuilder.objectLockOptions.lockMode(lockMode);
                return this;
            }

            public CreateMultipartUploadObjectLockOptionsBuilder retainUntilDate(Date retainUntilDate) {
                this.parentBuilder.objectLockOptions.retainUntilDate(retainUntilDate);
                return this;
            }

            public CreateMultipartUploadObjectLockOptionsBuilder legalHold(boolean legalHold) {
                this.parentBuilder.objectLockOptions.legalHold(legalHold);
                return this;
            }

        }

    }

}
