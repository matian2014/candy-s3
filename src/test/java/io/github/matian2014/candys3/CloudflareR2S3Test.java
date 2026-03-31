package io.github.matian2014.candys3;

import java.io.IOException;
import java.util.Map;

/**
 * Cloudflare R2 provider test entry.
 *
 * Actual test cases live in {@link CandyS3Test} and are inherited.
 */
public class CloudflareR2S3Test extends CandyS3Test {

    @Override
    protected S3Provider provider() {
        return S3Provider.CLOUDFLARE_R2;
    }

    @Override
    protected CandyS3 init(S3Provider provider) throws IOException {
        CandyS3 candyS3 = new CandyS3(provider);
        Map<String, String> properties = readIni("cloudflare_r2.ini");
        candyS3.setAccessKey(properties.get("access-key"));
        candyS3.setSecretKey(properties.get("secret-key"));
        candyS3.setRegion(properties.get("default-region"));
        candyS3.setCloudflareR2AccountId(properties.get("account-id"));
        candyS3.setUseSSL(true);
        return candyS3;
    }

    @Override
    protected String bucketLocationOtherRegion() {
        return "us-west-2";
    }

    @Override
    protected String listBucketsFilterOtherRegion() {
        return "us-west-2";
    }
}

