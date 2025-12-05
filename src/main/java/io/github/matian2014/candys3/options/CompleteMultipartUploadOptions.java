package io.github.matian2014.candys3.options;

public final class CompleteMultipartUploadOptions {

    private ObjectConditionalWriteOptions condition;

    private CompleteMultipartUploadOptions(ObjectConditionalWriteOptions condition) {
        this.condition = condition;
    }

    public void setCondition(ObjectConditionalWriteOptions condition) {
        this.condition = condition;
    }

    public ObjectConditionalWriteOptions getCondition() {
        return condition;
    }

    public static class CompleteMultipartUploadOptionsBuilder {
        private ObjectConditionalWriteOptions conditionalWriteOptions;

        public CompleteMultipartUploadOptionsBuilder() {
        }

        public CompleteMultipartUploadConditionBuilder configureCondition() {
            this.conditionalWriteOptions = new ObjectConditionalWriteOptions();
            return new CompleteMultipartUploadConditionBuilder(this);
        }

        public CompleteMultipartUploadOptions build() {
            return new CompleteMultipartUploadOptions(conditionalWriteOptions);
        }

        public static final class CompleteMultipartUploadConditionBuilder {
            private final CompleteMultipartUploadOptionsBuilder parentBuilder;

            CompleteMultipartUploadConditionBuilder(CompleteMultipartUploadOptionsBuilder parentBuilder) {
                this.parentBuilder = parentBuilder;
                if (this.parentBuilder.conditionalWriteOptions == null) {
                    this.parentBuilder.conditionalWriteOptions = new ObjectConditionalWriteOptions();
                }
            }

            public CompleteMultipartUploadOptionsBuilder endConfigureCondition() {
                return this.parentBuilder;
            }

            public CompleteMultipartUploadConditionBuilder ifMatch(String etag) {
                this.parentBuilder.conditionalWriteOptions.ifMatch(etag);
                return this;
            }

            public CompleteMultipartUploadConditionBuilder ifNotExists() {
                this.parentBuilder.conditionalWriteOptions.ifNotExists();
                return this;
            }

        }

    }
}
