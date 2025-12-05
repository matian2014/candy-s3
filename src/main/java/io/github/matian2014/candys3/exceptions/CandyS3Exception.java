package io.github.matian2014.candys3.exceptions;

public class CandyS3Exception extends RuntimeException {

    private int code;
    private S3ServerError internalError;

    public CandyS3Exception(int code, String msg) {
        super(msg);
        this.code = code;
    }

    public CandyS3Exception(int code, S3ServerError serverError) {
        super(serverError == null ? "" : serverError.toString());
        this.code = code;
        this.internalError = serverError;
    }

    public int getCode() {
        return code;
    }

    public S3ServerError getParsedError() {
        return internalError;
    }

    @Override
    public String toString() {
        return "{" +
                "code=" + code +
                ", message=" + super.getMessage() +
                '}';
    }
}
