package io.github.matian2014.candys3;

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
    protected String bucketLocationOtherRegion() {
        return "us-west-2";
    }

    @Override
    protected String listBucketsFilterOtherRegion() {
        return "us-west-2";
    }
}

