package io.github.matian2014.candys3.options;

public final class PresignUrlOptions {

    /**
     * The HTTP method to use for the presigned URL.
     * Usually GET for downloading, or PUT for uploading.
     */
    private final String method;
    private final long ttl;

    public PresignUrlOptions(String method, long ttl) {
        this.method = method;

        if (ttl <= 0) {
            throw new IllegalArgumentException("ttl must be greater than 0.");
        }
        if (ttl > 604800) {
            throw new IllegalArgumentException("ttl must be less than 604800 (seven days).");
        }
        this.ttl = ttl;
    }

    public String getMethod() {
        return method;
    }

    public long getTtl() {
        return ttl;
    }

}
