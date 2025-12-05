package io.github.matian2014.candys3;

public class HttpConstants {
    public static final String CONSTANT_AWS_SERVICENAME_S3 = "s3";

    public static final String HEADER_AUTHORIZATION = "Authorization";
    public static final String HEADER_CONTENT_MD5 = "Content-MD5";
    public static final String HEADER_AMZ_CONTENT_HASH = "x-amz-content-sha256";
    public static final String HEADER_AMZ_BUCKET_OBJECT_LOCK_ENABLED = "x-amz-bucket-object-lock-enabled";
    public static final String HEADER_AMZ_STORAGE_CLASS = "x-amz-storage-class";
    public static final String HEADER_AMZ_TAGGING = "x-amz-tagging";
    public static final String HEADER_AMZ_OBJECT_LOCK_MODE = "x-amz-object-lock-mode";
    public static final String HEADER_AMZ_OBJECT_LEGAL_HOLD = "x-amz-object-lock-legal-hold";
    public static final String HEADER_AMZ_OBJECT_LOCK_RETAIN_UNTIL_DATE = "x-amz-object-lock-retain-until-date";
    public static final String HEADER_AMZ_SSE_BUCKET_KEY_ENABLED = "x-amz-server-side-encryption-bucket-key-enabled";
    public static final String HEADER_AMZ_SSE_ALGORITHM = "x-amz-server-side-encryption";
    public static final String HEADER_AMZ_BYPASS_GOVERNANCE_RETENTION = "x-amz-bypass-governance-retention";

    public static final String HEADER_AMZ_COPY_SOURCE_IF_MATCH = "x-amz-copy-source-if-match";
    public static final String HEADER_AMZ_COPY_SOURCE_IF_MODIFIED_SINCE = "x-amz-copy-source-if-modified-since";
    public static final String HEADER_AMZ_COPY_SOURCE_IF_NONE_MATCH = "x-amz-copy-source-if-none-match";
    public static final String HEADER_AMZ_COPY_SOURCE_IF_UNMODIFIED_SINCE = "x-amz-copy-source-if-unmodified-since";

    public static final String HEADER_IF_MATCH = "If-Match";

    public static final String HEADER_AMZ_METADATA_DIRECTIVE = "x-amz-metadata-directive";
    public static final String HEADER_AMZ_TAGGING_DIRECTIVE = "x-amz-tagging-directive";

    public static final String HTTP_METHOD_GET = "GET";
    public static final String HTTP_METHOD_HEAD = "HEAD";
    public static final String HTTP_METHOD_PUT = "PUT";
    public static final String HTTP_METHOD_POST = "POST";
    public static final String HTTP_METHOD_DELETE = "DELETE";
}
