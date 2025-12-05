package io.github.matian2014.candys3;

public enum S3Provider {

    AWS(".amazonaws.com"),
    CLOUDFLARE_R2(".r2.cloudflarestorage.com"),
    ALIYUN_OSS(".aliyuncs.com"),
    TENCENTCLOUD_COS(".myqcloud.com"),
    CUSTOM(""),
    ;

    /**
     * The domain suffix for the S3 provider.
     * For CUSTOM, the domain suffix is empty and the user is responsible for providing the correct domain suffix.
     */
    private final String domain;

    S3Provider(String domain) {
        this.domain = domain;
    }

    public String getDomain() {
        return domain;
    }

}
