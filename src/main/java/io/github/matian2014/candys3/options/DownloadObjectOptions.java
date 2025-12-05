package io.github.matian2014.candys3.options;

import org.apache.commons.lang3.StringUtils;

import java.io.OutputStream;
import java.util.Date;

public final class DownloadObjectOptions {

    private final String range;
    private final String versionId;
    private final Integer partNumber;

    private final DownloadObjectResponseHeaderOptions responseHeaderOptions;
    private final DownloadObjectCondition condition;

    /**
     * configure the data output of the download object operation.
     *
     * @see DownloadObjectDataOutput
     * Ignore this field when use for getObjectMeta.
     */
    private final DownloadObjectDataOutput dataOutput;

    private DownloadObjectOptions(String range, String versionId, Integer partNumber,
                                  DownloadObjectResponseHeaderOptions responseHeaderOptions,
                                  DownloadObjectCondition condition,
                                  DownloadObjectDataOutput dataOutput) {
        this.range = range;
        this.versionId = versionId;
        this.partNumber = partNumber;
        this.responseHeaderOptions = responseHeaderOptions;
        this.condition = condition;
        this.dataOutput = dataOutput;
    }

    public String getVersionId() {
        return versionId;
    }

    public Integer getPartNumber() {
        return partNumber;
    }

    public String getRange() {
        return range;
    }

    public DownloadObjectCondition getCondition() {
        return condition;
    }

    public DownloadObjectDataOutput getDataOutput() {
        return dataOutput;
    }

    public DownloadObjectResponseHeaderOptions getResponseHeaderOptions() {
        return responseHeaderOptions;
    }

    public static final class DownloadObjectResponseHeaderOptions {
        private String responseCacheControl;
        private String responseContentDisposition;
        private String responseContentEncoding;
        private String responseContentLanguage;
        private String responseContentType;
        private Date responseExpires;

        DownloadObjectResponseHeaderOptions() {
        }

        public String getResponseCacheControl() {
            return responseCacheControl;
        }

        public String getResponseContentDisposition() {
            return responseContentDisposition;
        }

        public String getResponseContentEncoding() {
            return responseContentEncoding;
        }

        public String getResponseContentLanguage() {
            return responseContentLanguage;
        }

        public String getResponseContentType() {
            return responseContentType;
        }

        public Date getResponseExpires() {
            return responseExpires == null ? null : new Date(responseExpires.getTime());
        }

        public DownloadObjectResponseHeaderOptions responseCacheControl(String responseCacheControl) {
            this.responseCacheControl = responseCacheControl;
            return this;
        }

        public DownloadObjectResponseHeaderOptions responseContentDisposition(String responseContentDisposition) {
            this.responseContentDisposition = responseContentDisposition;
            return this;
        }

        public DownloadObjectResponseHeaderOptions responseContentEncoding(String responseContentEncoding) {
            this.responseContentEncoding = responseContentEncoding;
            return this;
        }

        public DownloadObjectResponseHeaderOptions responseContentLanguage(String responseContentLanguage) {
            this.responseContentLanguage = responseContentLanguage;
            return this;
        }

        public DownloadObjectResponseHeaderOptions responseContentType(String responseContentType) {
            this.responseContentType = responseContentType;
            return this;
        }

        public DownloadObjectResponseHeaderOptions responseExpires(Date responseContentType) {
            this.responseExpires = responseContentType;
            return this;
        }
    }

    public static final class DownloadObjectCondition {
        private String ifMatch; // Return the object only if its entity tag (ETag) is the same as the one specified in this header
        private String ifNoneMatch; // Return the object only if its entity tag (ETag) is different from the one specified in this header
        private Date ifModifiedSince;
        private Date ifUnmodifiedSince;

        DownloadObjectCondition() {
        }

        public DownloadObjectCondition ifMatch(String etag) {
            this.ifMatch = etag;
            return this;
        }

        public DownloadObjectCondition ifNoneMatch(String etag) {
            this.ifNoneMatch = etag;
            return this;
        }

        public DownloadObjectCondition ifModifiedSince(Date modifiedSince) {
            this.ifModifiedSince = modifiedSince;
            return this;
        }

        public DownloadObjectCondition ifUnmodifiedSince(Date unmodifiedSince) {
            this.ifUnmodifiedSince = unmodifiedSince;
            return this;
        }

        public String getIfMatch() {
            return ifMatch;
        }

        public String getIfNoneMatch() {
            return ifNoneMatch;
        }

        public Date getIfModifiedSince() {
            return ifModifiedSince == null ? null : new Date(ifModifiedSince.getTime());
        }

        public Date getIfUnmodifiedSince() {
            return ifUnmodifiedSince == null ? null : new Date(ifUnmodifiedSince.getTime());
        }
    }

    public static final class DownloadObjectDataOutput {
        /**
         * the path of the file to store the downloaded object.
         * outputFile is advanced to outputStream and outputBytes
         */
        private String outputFile;
        /**
         * if can overwrite file if already exits.
         */
        private boolean canOverwrite;
        /**
         * the output stream to read the downloaded object content.
         */
        private OutputStream outputStream;

        DownloadObjectDataOutput() {
        }

        public DownloadObjectDataOutput toFile(String file, boolean canOverwrite) {
            this.outputFile = file;
            this.canOverwrite = canOverwrite;

            this.outputStream = null;
            return this;
        }

        public DownloadObjectDataOutput toStream(OutputStream in) {
            this.outputStream = in;
            return this;
        }

        public DownloadObjectDataOutput toBytes() {
            return this;
        }

        public String getOutputFile() {
            return outputFile;
        }

        public boolean isCanOverwrite() {
            return canOverwrite;
        }

        public OutputStream getOutputStream() {
            return outputStream;
        }
    }

    public static class DownloadObjectOptionsBuilder {
        private String range;
        private String versionId;
        private Integer partNumber;

        private DownloadObjectResponseHeaderOptions responseHeaderOptions;
        private DownloadObjectCondition condition;
        private DownloadObjectDataOutput dataOutput;

        public DownloadObjectOptions build() {
            if (!StringUtils.isEmpty(range) && partNumber != null) {
                throw new IllegalArgumentException("Cannot specify both Range header and partNumber query parameter.");
            }
            return new DownloadObjectOptions(range, versionId, partNumber, responseHeaderOptions, condition, dataOutput);
        }

        public DownloadObjectOptionsBuilder range(int startBytes, int endBytes) {
            if (startBytes < 0) {
                throw new IllegalArgumentException("Range start-bytes must be a positive integer or 0.");
            }
            if (endBytes <= startBytes) {
                throw new IllegalArgumentException("Range end-bytes must be a positive integer and greater to start-bytes.");
            }

            this.range = startBytes + "-" + endBytes;
            return this;
        }

        public DownloadObjectOptionsBuilder range(int startBytes) {
            if (startBytes < 0) {
                throw new IllegalArgumentException("Range start-bytes must be a positive integer or 0.");
            }
            this.range = startBytes + "-";
            return this;
        }

        public DownloadObjectOptionsBuilder versionId(String versionId) {
            this.versionId = versionId;
            return this;
        }

        public DownloadObjectOptionsBuilder partNumber(Integer partNumber) {
            if (partNumber < 1) {
                throw new IllegalArgumentException("Part number must be an integer between 1 and 10000, inclusive.");
            }
            this.partNumber = partNumber;
            return this;
        }

        public DownloadObjectResponseHeaderOptionsBuilder configureResponseHeaderOptions() {
            return new DownloadObjectResponseHeaderOptionsBuilder(this);
        }

        public DownloadObjectConditionBuilder configureDownloadCondition() {
            return new DownloadObjectConditionBuilder(this);
        }

        public DownloadObjectDataOutputBuilder configureDataOutput() {
            return new DownloadObjectDataOutputBuilder(this);
        }

        public static final class DownloadObjectResponseHeaderOptionsBuilder {
            private final DownloadObjectOptionsBuilder parentBuilder;

            DownloadObjectResponseHeaderOptionsBuilder(DownloadObjectOptionsBuilder parentBuilder) {
                this.parentBuilder = parentBuilder;
                if (this.parentBuilder.responseHeaderOptions == null) {
                    this.parentBuilder.responseHeaderOptions = new DownloadObjectResponseHeaderOptions();
                }
            }

            public DownloadObjectOptionsBuilder endConfigureResponseHeader() {
                return this.parentBuilder;
            }

            public DownloadObjectResponseHeaderOptionsBuilder responseCacheControl(String responseCacheControl) {
                this.parentBuilder.responseHeaderOptions.responseCacheControl(responseCacheControl);
                return this;
            }

            public DownloadObjectResponseHeaderOptionsBuilder responseContentDisposition(String responseContentDisposition) {
                this.parentBuilder.responseHeaderOptions.responseContentDisposition(responseContentDisposition);
                return this;
            }

            public DownloadObjectResponseHeaderOptionsBuilder responseContentEncoding(String responseContentEncoding) {
                this.parentBuilder.responseHeaderOptions.responseContentEncoding(responseContentEncoding);
                return this;
            }

            public DownloadObjectResponseHeaderOptionsBuilder responseContentLanguage(String responseContentLanguage) {
                this.parentBuilder.responseHeaderOptions.responseContentLanguage(responseContentLanguage);
                return this;
            }

            public DownloadObjectResponseHeaderOptionsBuilder responseContentType(String responseContentType) {
                this.parentBuilder.responseHeaderOptions.responseContentType(responseContentType);
                return this;
            }

            public DownloadObjectResponseHeaderOptionsBuilder responseExpires(Date responseContentType) {
                this.parentBuilder.responseHeaderOptions.responseExpires(responseContentType);
                return this;
            }

        }

        public static final class DownloadObjectConditionBuilder {
            private final DownloadObjectOptionsBuilder parentBuilder;

            DownloadObjectConditionBuilder(DownloadObjectOptionsBuilder parentBuilder) {
                this.parentBuilder = parentBuilder;
                if (this.parentBuilder.condition == null) {
                    this.parentBuilder.condition = new DownloadObjectCondition();
                }
            }

            public DownloadObjectOptionsBuilder endConfigureCondition() {
                return this.parentBuilder;
            }

            public DownloadObjectConditionBuilder ifMatch(String etag) {
                this.parentBuilder.condition.ifMatch(etag);
                return this;
            }

            public DownloadObjectConditionBuilder ifNoneMatch(String etag) {
                this.parentBuilder.condition.ifNoneMatch(etag);
                return this;
            }

            public DownloadObjectConditionBuilder ifModifiedSince(Date modifiedSince) {
                this.parentBuilder.condition.ifModifiedSince(modifiedSince);
                return this;
            }

            public DownloadObjectConditionBuilder ifUnmodifiedSince(Date unmodifiedSince) {
                this.parentBuilder.condition.ifUnmodifiedSince(unmodifiedSince);
                return this;
            }

        }

        public static final class DownloadObjectDataOutputBuilder {
            private final DownloadObjectOptionsBuilder parentBuilder;

            DownloadObjectDataOutputBuilder(DownloadObjectOptionsBuilder parentBuilder) {
                this.parentBuilder = parentBuilder;
                if (this.parentBuilder.dataOutput == null) {
                    this.parentBuilder.dataOutput = new DownloadObjectDataOutput();
                }
            }

            public DownloadObjectOptionsBuilder endConfigureDataOutput() {
                return this.parentBuilder;
            }

            public DownloadObjectDataOutputBuilder toFile(String file, boolean canOverwrite) {
                this.parentBuilder.dataOutput.toFile(file, canOverwrite);
                return this;
            }

            public DownloadObjectDataOutputBuilder toStream(OutputStream in) {
                this.parentBuilder.dataOutput.toStream(in);
                return this;
            }

            public DownloadObjectDataOutputBuilder toBytes() {
                return this;
            }
        }
    }

}
