package io.github.matian2014.candys3.exceptions;

public enum CommonErrorCode {

    SERVER_ERROR(1001, "A server error occurred or can't process the response."),

    BUCKET_ALREADY_EXISTS(1010, "The requested bucket name is not available or the bucket you tried to create already exists"),
    NO_SUCH_BUCKET(1011, "The specified bucket does not exist."),
    BUCKET_OBJECT_LOCK_NOT_ENABLED(1012, "Object Lock configuration does not exist for this bucket"),
    BUCKET_NO_PUBLIC_ACCESS_BLOCK(1013, "The public access block configuration was not found"),
    BUCKET_NO_POLICY(1014, "The bucket policy does not exist"),
    BUCKET_NO_ASSOCIATED_TAG(1015, "There is no tag set associated with the bucket."),

    INPUT_FILE_NOT_EXISTS(1021, "Input file does not exist."),
    OUTPUT_FILE_ALREADY_EXISTS(1022, "Output file already exist."),
    OBJECT_PRECONDITION_FAILED(1023, "At least one of the pre-conditions you specified did not hold"),
    OBJECT_CONDITIONAL_REQUEST_CONFLICT(1024, "A conflicting operation occurs during the upload. You should fetch the object's ETag and retry the upload."),
    OBJECT_NOT_MODIFIED(1025, "Not Modified"),
    NO_SUCH_OBJECT(1026, "The specified key does not exist."),
    OBJECT_VERSION_IS_DELETE_MARKER(1027, "The version of the object is a delete marker."),
    NO_SUCH_OBJECT_LOCK_CONFIGURATION(1029, "The specified object does not have a ObjectLock configuration."),

    COPY_SOURCE_NOT_IN_ACTIVE_TIRE(1035, "The source object of the COPY action is not in the active tier and is only stored in S3 Glacier."),
    MULTIPART_UPLOAD_NOT_EXISTS(1040, "The specified multipart upload does not exist. The upload ID might be invalid, or the multipart upload might have been aborted or completed."),
    MULTIPART_UPLOAD_ENTITY_TOO_SMALL(1041, "Your proposed upload is smaller than the minimum allowed object size. Each part must be at least 5 MB in size, except the last part."),
    MULTIPART_UPLOAD_INVALID_PART(1042, "One or more of the specified parts could not be found. The part might not have been uploaded, or the specified ETag might not have matched the uploaded part's ETag."),
    MULTIPART_UPLOAD_INVALID_PART_ORDER(1043, "The list of parts was not in ascending order. The parts list must be specified in order by part number."),
    COPY_SOURCE_RANGE_INVALID(1050, "The specified copy source is not supported as a byte-range copy source."),
    ;

    private int code;
    private String msg;

    CommonErrorCode(int code, String msg) {
        this.code = code;
        this.msg = msg;
    }

    public int getCode() {
        return code;
    }

    public String getMsg() {
        return msg;
    }
}
