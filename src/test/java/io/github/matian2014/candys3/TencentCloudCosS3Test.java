package io.github.matian2014.candys3;

import java.io.IOException;
import java.util.Map;

/**
 * Tencent Cloud COS provider test entry.
 *
 * Actual test cases live in {@link CandyS3Test} and are inherited.
 */
public class TencentCloudCosS3Test extends CandyS3Test {

    @Override
    protected S3Provider provider() {
        return S3Provider.TENCENTCLOUD_COS;
    }

    @Override
    protected CandyS3 init(S3Provider provider) throws IOException {
        CandyS3 candyS3 = new CandyS3(provider);
        Map<String, String> properties = readIni("tencentcloud_cos.ini");
        candyS3.setAccessKey(properties.get("access-key"));
        candyS3.setSecretKey(properties.get("secret-key"));
        candyS3.setRegion(properties.get("default-region"));
        super.TENCENTCLOUD_COS_APPID = properties.get("cos-app-id");
        candyS3.setUseSSL(true);
        return candyS3;
    }

    @Override
    protected String bucketLocationOtherRegion() {
        return "ap-chengdu";
    }

    @Override
    protected String listBucketsFilterOtherRegion() {
        return "ap-chengdu";
    }
}

