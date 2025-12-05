package io.github.matian2014.candys3.options;

import java.io.InputStream;

public final class UploadPartOptions {

    private final ObjectDataContentOptions dataContentOptions;

    private UploadPartOptions(ObjectDataContentOptions dataContentOptions) {
        this.dataContentOptions = dataContentOptions;
    }

    public byte[] getInputBytes() {
        return dataContentOptions.getInputBytes();
    }

    public InputStream getInputStream() {
        return dataContentOptions.getInputStream();
    }

    public String getInputFile() {
        return dataContentOptions.getInputFile();
    }

    public ObjectDataContentOptions getDataContentOptions() {
        return dataContentOptions;
    }

    public static class UploadPartOptionsBuilder {
        private ObjectDataContentOptions dataContentOptions;

        public UploadPartOptionsBuilder() {
        }

        public UploadPartDataContentOptionsBuilder configureUploadData() {
            return new UploadPartDataContentOptionsBuilder(this);
        }

        public UploadPartOptions build() {
            if (this.dataContentOptions == null) {
                throw new IllegalArgumentException("Input is required when upload part.");
            }
            return new UploadPartOptions(this.dataContentOptions);
        }


        public static final class UploadPartDataContentOptionsBuilder {
            private final UploadPartOptionsBuilder parentBuilder;
            private final ObjectDataContentOptions.ObjectDataContentOptionsBuilder innerBuilder;

            UploadPartDataContentOptionsBuilder(UploadPartOptionsBuilder parentBuilder) {
                this.parentBuilder = parentBuilder;
                this.innerBuilder = new ObjectDataContentOptions.ObjectDataContentOptionsBuilder();
            }

            public UploadPartOptionsBuilder endConfigureDataContent() {
                this.parentBuilder.dataContentOptions = this.innerBuilder.build();
                return this.parentBuilder;
            }

            public UploadPartDataContentOptionsBuilder withData(byte[] bytes) {
                this.innerBuilder.withData(bytes);
                return this;
            }

            public UploadPartDataContentOptionsBuilder withData(InputStream in) {
                this.innerBuilder.withData(in);
                return this;
            }

            public UploadPartDataContentOptionsBuilder withData(String file) {
                this.innerBuilder.withData(file);
                return this;
            }

        }

    }
}
