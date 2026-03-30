package io.github.matian2014.candys3;

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
    protected String bucketLocationOtherRegion() {
        return "cn-chengdu";
    }

    @Override
    protected String listBucketsFilterOtherRegion() {
        return "cn-chengdu";
    }
}

