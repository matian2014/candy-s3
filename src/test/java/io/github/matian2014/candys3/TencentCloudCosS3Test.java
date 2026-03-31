package io.github.matian2014.candys3;

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
    protected String bucketLocationOtherRegion() {
        return "ap-chengdu";
    }

    @Override
    protected String listBucketsFilterOtherRegion() {
        return "ap-chengdu";
    }
}

