package io.github.matian2014.candys3;

import java.io.IOException;
import java.util.Map;

/**
 * Aliyun OSS provider test entry.
 *
 * Actual test cases live in {@link CandyS3Test} and are inherited.
 */
public class AliyunOssS3Test extends CandyS3Test {

    @Override
    protected S3Provider provider() {
        return S3Provider.ALIYUN_OSS;
    }

    @Override
    protected CandyS3 init(S3Provider provider) throws IOException {
        CandyS3 candyS3 = new CandyS3(provider);
        Map<String, String> properties = readIni("aliyun_oss.ini");
        candyS3.setAccessKey(properties.get("access-key"));
        candyS3.setSecretKey(properties.get("secret-key"));
        candyS3.setRegion(properties.get("default-region"));
        candyS3.setUseSSL(true);
        return candyS3;
    }

    @Override
    protected String bucketLocationOtherRegion() {
        return "cn-chengdu";
    }

    @Override
    protected String listBucketsFilterOtherRegion() {
        return "cn-chengdu";
    }
}

