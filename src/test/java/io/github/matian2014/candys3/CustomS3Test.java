package io.github.matian2014.candys3;

import java.io.IOException;
import java.util.Map;

/**
 * Custom provider test entry.
 *
 * Actual test cases live in {@link CandyS3Test} and are inherited.
 */
public class CustomS3Test extends CandyS3Test {

    @Override
    protected S3Provider provider() {
        return S3Provider.CUSTOM;
    }

    @Override
    protected CandyS3 init(S3Provider provider) throws IOException {
        CandyS3 candyS3 = new CandyS3(provider);
        Map<String, String> properties = readIni("custom.ini");
        candyS3.setAccessKey(properties.get("access-key"));
        candyS3.setSecretKey(properties.get("secret-key"));
        candyS3.setRegion(properties.get("default-region"));
        candyS3.setUseSSL(Boolean.parseBoolean(properties.get("useSsl")));
        candyS3.setCustomProviderDomain(properties.get("custom-provider-domain"));
        candyS3.setCustomProviderUsePathStyle(Boolean.parseBoolean(properties.get("custom-provider-use-path-style")));
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

