package io.github.matian2014.candys3;

import org.junit.AfterClass;
import org.junit.Test;

import java.io.IOException;
import java.security.NoSuchAlgorithmException;

public class AliyunOSSTest {


    @AfterClass
    public static void aliyunOSSRemoveTestsBucket() throws IOException {
        new CandyS3Test().removeTestBuckets(S3Provider.ALIYUN_OSS);
        System.out.println("aliyunOSSRemoveTestsBucket done.");
    }

    @Test
    public void aliyunOSSCreateBucketTest() throws IOException {
        new CandyS3Test().createBucketTest(S3Provider.ALIYUN_OSS);
    }

    @Test
    public void aliyunOSSCreateBucketErrorTest() throws IOException {
        new CandyS3Test().createBucketErrorTest(S3Provider.ALIYUN_OSS);
    }

    @Test
    public void aliyunOSSBucketExistsTest() throws IOException {
        new CandyS3Test().bucketExistsTest(S3Provider.ALIYUN_OSS);
    }

    @Test
    public void aliyunOSSBucketVersioningTest() throws IOException {
        new CandyS3Test().bucketVersioningTest(S3Provider.ALIYUN_OSS);
    }

    @Test
    public void aliyunOSSBucketAccelerateTest() throws IOException {
        new CandyS3Test().bucketAccelerateTest(S3Provider.ALIYUN_OSS);
    }

    @Test
    public void aliyunOSSBucketLocationTest() throws IOException {
        new CandyS3Test().bucketLocationTest(S3Provider.ALIYUN_OSS, "cn-hangzhou", "cn-chengdu");
    }

    @Test
    public void aliyunOSSBucketObjectLockConfigurationTest() throws IOException, NoSuchAlgorithmException {
        new CandyS3Test().bucketObjectLockConfigurationTest(S3Provider.ALIYUN_OSS);
    }

    @Test
    public void aliyunOSSBucketPolicyTest() throws IOException, NoSuchAlgorithmException {
        new CandyS3Test().bucketPolicyTest(S3Provider.ALIYUN_OSS);
    }

    @Test
    public void aliyunOSSBucketBlockPublicAccessTest() throws IOException {
        new CandyS3Test().bucketBlockPublicAccessTest(S3Provider.ALIYUN_OSS);
    }

    @Test
    public void aliyunOSSBucketTagTest() throws IOException, NoSuchAlgorithmException {
        new CandyS3Test().bucketTagTest(S3Provider.ALIYUN_OSS);
    }

    @Test
    public void aliyunOSSBucketSSEConfigurationTest() throws IOException, NoSuchAlgorithmException {
        new CandyS3Test().bucketSSEConfigurationTest(S3Provider.ALIYUN_OSS);
    }

    @Test
    public void aliyunOSSListBucketsTest() throws IOException {
        new CandyS3Test().listBucketsTest(S3Provider.ALIYUN_OSS);
    }

    @Test
    public void aliyunOSSListBucketsFilterRegionTest() throws IOException {
        new CandyS3Test().listBucketsFilterRegionTest(S3Provider.ALIYUN_OSS, "cn-chengdu");
    }

    @Test
    public void aliyunOSSListBucketsPrefixTest() throws IOException {
        new CandyS3Test().listBucketsPrefixTest(S3Provider.ALIYUN_OSS);
    }

    @Test
    public void aliyunOSSListBucketsPaginationTest() throws IOException {
        new CandyS3Test().listBucketsPaginationTest(S3Provider.ALIYUN_OSS);
    }

    @Test
    public void aliyunOSSListObjectsPaginationTest() throws IOException, NoSuchAlgorithmException {
        new CandyS3Test().listObjectsPaginationTest(S3Provider.ALIYUN_OSS);
    }

    @Test
    public void aliyunOSSListObjectVersionsTest() throws IOException, NoSuchAlgorithmException {
        new CandyS3Test().listObjectVersionsTest(S3Provider.ALIYUN_OSS);
    }

    @Test
    public void aliyunOSSListObjectVersionsPaginationTest() throws IOException, NoSuchAlgorithmException, InterruptedException {
        new CandyS3Test().listObjectVersionsPaginationTest(S3Provider.ALIYUN_OSS);
    }

    @Test
    public void aliyunOSSListObjectsCommonPrefixTest() throws IOException, NoSuchAlgorithmException {
        new CandyS3Test().listObjectsCommonPrefixTest(S3Provider.ALIYUN_OSS);
    }

    @Test
    public void aliyunOSSListObjectVersionsWithoutVersioningTest() throws IOException, NoSuchAlgorithmException {
        new CandyS3Test().listObjectVersionsWithoutVersioningTest(S3Provider.ALIYUN_OSS);
    }

    @Test
    public void aliyunOSSListObjectVersionsCommonPrefixTest() throws IOException, NoSuchAlgorithmException {
        new CandyS3Test().listObjectVersionsCommonPrefixTest(S3Provider.ALIYUN_OSS);
    }

    @Test
    public void aliyunOSSGetVersioningObjectTest() throws IOException, NoSuchAlgorithmException {
        new CandyS3Test().getVersioningObjectTest(S3Provider.ALIYUN_OSS);
    }

    @Test
    public void aliyunOSSGetObjectVersionMetadataTest() throws IOException, NoSuchAlgorithmException {
        new CandyS3Test().getObjectVersionMetadataTest(S3Provider.ALIYUN_OSS);
    }

    @Test
    public void aliyunOSSDeleteVersioningObjectTest() throws IOException, NoSuchAlgorithmException {
        new CandyS3Test().deleteVersioningObjectTest(S3Provider.ALIYUN_OSS);
    }

    @Test
    public void aliyunOSSDeleteVersioningObjectsBatchTest() throws IOException, NoSuchAlgorithmException {
        new CandyS3Test().deleteVersioningObjectsBatchTest(S3Provider.ALIYUN_OSS);
    }

    @Test
    public void aliyunOSSDeleteObjectConditionalTest() throws IOException, NoSuchAlgorithmException {
        new CandyS3Test().deleteObjectConditionalTest(S3Provider.ALIYUN_OSS);
    }

    @Test
    public void aliyunOSSDeleteObjectsBatchConditionalTest() throws IOException, NoSuchAlgorithmException {
        new CandyS3Test().deleteObjectsBatchConditionalTest(S3Provider.ALIYUN_OSS);
    }

    @Test
    public void aliyunOSSPutDownloadEmptyObjectTest() throws IOException, NoSuchAlgorithmException {
        new CandyS3Test().putDownloadEmptyObjectTest(S3Provider.ALIYUN_OSS);
    }

    @Test
    public void aliyunOSSPutObjectWithNonEnglishKeyTest() throws IOException, NoSuchAlgorithmException {
        new CandyS3Test().putObjectWithNonEnglishKeyTest(S3Provider.ALIYUN_OSS);
    }

    @Test
    public void aliyunOSSPutObjectConditionalTest() throws IOException, NoSuchAlgorithmException {
        new CandyS3Test().putObjectConditionalTest(S3Provider.ALIYUN_OSS);
    }

    @Test
    public void aliyunOSSMultipartUploadConditionalTest() throws IOException, NoSuchAlgorithmException {
        new CandyS3Test().multipartUploadConditionalTest(S3Provider.ALIYUN_OSS);
    }

    @Test
    public void aliyunOSSPutObjectPropertiesTest() throws IOException, NoSuchAlgorithmException {
        new CandyS3Test().putObjectPropertiesTest(S3Provider.ALIYUN_OSS);
    }

    @Test
    public void aliyunOSSMultipartUploadPropertiesTest() throws IOException, NoSuchAlgorithmException {
        new CandyS3Test().multipartUploadPropertiesTest(S3Provider.ALIYUN_OSS);
    }

    @Test
    public void aliyunOSSPutDownloadSmallObjectTest() throws IOException, NoSuchAlgorithmException {
        new CandyS3Test().putDownloadSmallObjectTest(S3Provider.ALIYUN_OSS);
    }

    @Test
    public void aliyunOSSPutDownloadLargeObjectTest() throws IOException, NoSuchAlgorithmException {
        new CandyS3Test().putDownloadLargeObjectTest(S3Provider.ALIYUN_OSS);
    }

    @Test
    public void aliyunOSSMultipartUploadDownloadTest() throws IOException, NoSuchAlgorithmException {
        new CandyS3Test().multipartUploadDownloadTest(S3Provider.ALIYUN_OSS);
    }

    @Test
    public void aliyunOSSMultipartUploadToExistsObjectTest() throws IOException, NoSuchAlgorithmException {
        new CandyS3Test().multipartUploadToExistsObjectTest(S3Provider.ALIYUN_OSS);
    }

    @Test
    public void aliyunOSSAbortMultipartUploadToExistsObjectTest() throws IOException, NoSuchAlgorithmException {
        new CandyS3Test().abortMultipartUploadToExistsObjectTest(S3Provider.ALIYUN_OSS);
    }

    @Test
    public void aliyunOSSAbortMultipartUploadTest() throws IOException, NoSuchAlgorithmException {
        new CandyS3Test().abortMultipartUploadTest(S3Provider.ALIYUN_OSS);
    }

    @Test
    public void aliyunOSSListMultipartUploadsTest() throws IOException {
        new CandyS3Test().listMultipartUploadsTest(S3Provider.ALIYUN_OSS);
    }


    @Test
    public void aliyunOSSListMultipartUploadsPaginationTest() throws IOException, NoSuchAlgorithmException {
        new CandyS3Test().listMultipartUploadsPaginationTest(S3Provider.ALIYUN_OSS);
    }

    @Test
    public void aliyunOSSPutAndGetObjectSseTest() throws IOException, NoSuchAlgorithmException {
        new CandyS3Test().putAndGetObjectSseTest(S3Provider.ALIYUN_OSS);
    }


    @Test
    public void aliyunOSSCopyObjectAndDownloadTest() throws IOException, NoSuchAlgorithmException {
        new CandyS3Test().copyObjectAndDownloadTest(S3Provider.ALIYUN_OSS);
    }

    @Test
    public void aliyunOSSCopyObjectConditionalTest() throws IOException, NoSuchAlgorithmException {
        new CandyS3Test().copyObjectCopySourceConditionalTest(S3Provider.ALIYUN_OSS);
    }

    @Test
    public void aliyunOSSCopyObjectUnionConditionTestTest() throws IOException, NoSuchAlgorithmException {
        new CandyS3Test().copyObjectCopySourceUnionConditionTest(S3Provider.ALIYUN_OSS);
    }

    @Test
    public void aliyunOSSCopyObjectWriteTargetConditionalTest() throws IOException, NoSuchAlgorithmException {
        new CandyS3Test().copyObjectWriteTargetConditionalTest(S3Provider.ALIYUN_OSS);
    }

    @Test
    public void aliyunOSSCopyObjectVersionTest() throws IOException, NoSuchAlgorithmException {
        new CandyS3Test().copyObjectVersionTest(S3Provider.ALIYUN_OSS);
    }

    @Test
    public void aliyunOSSCopyObjectPropertiesTest() throws IOException, NoSuchAlgorithmException, InterruptedException {
        new CandyS3Test().copyObjectPropertiesTest(S3Provider.ALIYUN_OSS);
    }

    @Test
    public void aliyunOSSPutAndGetObjectLockPropertiesTest() throws IOException, NoSuchAlgorithmException, InterruptedException {
        new CandyS3Test().putAndGetObjectLockPropertiesTest(S3Provider.ALIYUN_OSS);
    }

    @Test
    public void aliyunOSSPutAndGetObjectStorageClassTest() throws IOException, NoSuchAlgorithmException {
        new CandyS3Test().putAndGetObjectStorageClassTest(S3Provider.ALIYUN_OSS);
    }

    @Test
    public void aliyunOSSPutAndGetObjectTagTest() throws IOException, NoSuchAlgorithmException {
        new CandyS3Test().putAndGetObjectTagTest(S3Provider.ALIYUN_OSS);
    }

    @Test
    public void aliyunOSSCopyPartTest() throws IOException, NoSuchAlgorithmException {
        new CandyS3Test().copyPartTest(S3Provider.ALIYUN_OSS);
    }

    @Test
    public void aliyunOSSCopyPartConditionalTest() throws IOException, NoSuchAlgorithmException {
        new CandyS3Test().copyPartConditionalTest(S3Provider.ALIYUN_OSS);
    }

    @Test
    public void aliyunCopyPartWithUnionConditionTest() throws IOException, NoSuchAlgorithmException {
        new CandyS3Test().copyPartWithUnionConditionTest(S3Provider.ALIYUN_OSS);
    }

    @Test
    public void aliyunOSSCopyPartVersionTest() throws IOException, NoSuchAlgorithmException {
        new CandyS3Test().copyPartVersionTest(S3Provider.ALIYUN_OSS);
    }


    @Test
    public void aliyunOSSListPartsTest() throws IOException, NoSuchAlgorithmException {
        new CandyS3Test().listPartsTest(S3Provider.ALIYUN_OSS);
    }

    @Test
    public void aliyunOSSListPartsPaginationTest() throws IOException, NoSuchAlgorithmException {
        new CandyS3Test().listPartsPaginationTest(S3Provider.ALIYUN_OSS);
    }

    @Test
    public void aliyunOSSDownloadObjectTest() throws IOException, NoSuchAlgorithmException {
        new CandyS3Test().downloadObjectTest(S3Provider.ALIYUN_OSS);
    }

    @Test
    public void aliyunOSSDownloadObjectRangeTest() throws IOException, NoSuchAlgorithmException {
        new CandyS3Test().downloadObjectRangeTest(S3Provider.ALIYUN_OSS);
    }

    @Test
    public void aliyunOSSDownloadObjectOverwriteTest() throws IOException, NoSuchAlgorithmException {
        new CandyS3Test().downloadObjectOverwriteTest(S3Provider.ALIYUN_OSS);
    }

    @Test
    public void aliyunOSSDownloadObjectConditionalTest() throws IOException, NoSuchAlgorithmException {
        new CandyS3Test().downloadObjectConditionalTest(S3Provider.ALIYUN_OSS);
    }

    @Test
    public void aliyunOSSDownloadObjectUnionConditionTest() throws IOException, NoSuchAlgorithmException {
        new CandyS3Test().downloadObjectUnionConditionTest(S3Provider.ALIYUN_OSS);
    }

    @Test
    public void aliyunOSSDownloadObjectPartTest() throws IOException, NoSuchAlgorithmException {
        new CandyS3Test().downloadObjectPartTest(S3Provider.ALIYUN_OSS);
    }

    @Test
    public void aliyunOSSDownloadObjectMetadataTest() throws IOException, NoSuchAlgorithmException, InterruptedException {
        new CandyS3Test().downloadObjectMetadataTest(S3Provider.ALIYUN_OSS);
    }

    @Test
    public void aliyunOSSDownloadObjectObjectLockPropertiesTest() throws IOException, NoSuchAlgorithmException, InterruptedException {
        new CandyS3Test().downloadObjectObjectLockPropertiesTest(S3Provider.ALIYUN_OSS);
    }

    @Test
    public void aliyunOSSDownloadObjectStorageClassTest() throws IOException, NoSuchAlgorithmException, InterruptedException {
        new CandyS3Test().downloadObjectStorageClassTest(S3Provider.ALIYUN_OSS);
    }

    @Test
    public void aliyunOSSDownloadObjectTagTest() throws IOException, NoSuchAlgorithmException, InterruptedException {
        new CandyS3Test().downloadObjectTagTest(S3Provider.ALIYUN_OSS);
    }

    @Test
    public void aliyunOSSGetPresignUrlTest() throws IOException, NoSuchAlgorithmException, InterruptedException {
        new CandyS3Test().getWithPresignUrlTest(S3Provider.ALIYUN_OSS);
    }

    @Test
    public void aliyunOSSPutWithPresignUrlTest() throws IOException, NoSuchAlgorithmException, InterruptedException {
        new CandyS3Test().putWithPresignUrlTest(S3Provider.ALIYUN_OSS);
    }

    @Test
    public void aliyunOSSObjectRetentionTest() throws IOException, NoSuchAlgorithmException {
        new CandyS3Test().objectRetentionTest(S3Provider.ALIYUN_OSS);
    }

    @Test
    public void aliyunOSSObjectVersionRetentionTest() throws IOException, NoSuchAlgorithmException {
        new CandyS3Test().objectVersionRetentionTest(S3Provider.ALIYUN_OSS);
    }

    @Test
    public void aliyunOSSUpdateObjectGovernancePeriodTest() throws IOException, NoSuchAlgorithmException {
        new CandyS3Test().updateObjectGovernancePeriodTest(S3Provider.ALIYUN_OSS);
    }

    @Test
    public void aliyunOSSUpdateObjectCompliancePeriodTest() throws IOException, NoSuchAlgorithmException, InterruptedException {
        new CandyS3Test().updateObjectCompliancePeriodTest(S3Provider.ALIYUN_OSS);
    }

    @Test
    public void aliyunOSSUpdateObjectRetentionModeTest() throws IOException, NoSuchAlgorithmException, InterruptedException {
        new CandyS3Test().updateObjectRetentionModeTest(S3Provider.ALIYUN_OSS);
    }

    @Test
    public void aliyunOSSMultipartUploadObjectRetentionTest() throws IOException, NoSuchAlgorithmException, InterruptedException {
        new CandyS3Test().multipartUploadObjectRetentionTest(S3Provider.ALIYUN_OSS);
    }

    @Test
    public void aliyunOSSCopyObjectRetentionTest() throws IOException, NoSuchAlgorithmException, InterruptedException {
        new CandyS3Test().copyObjectRetentionTest(S3Provider.ALIYUN_OSS);
    }

    @Test
    public void aliyunOSSObjectRetentionTimezoneTest() throws IOException, NoSuchAlgorithmException, InterruptedException {
        new CandyS3Test().objectRetentionTimezoneTest(S3Provider.ALIYUN_OSS);
    }

    @Test
    public void aliyunOSSDeleteRetainObjectTest() throws IOException, NoSuchAlgorithmException, InterruptedException {
        new CandyS3Test().deleteRetainObjectTest(S3Provider.ALIYUN_OSS);
    }

    @Test
    public void aliyunOSSDeleteRetainObjectsBatchTest() throws IOException, NoSuchAlgorithmException {
        new CandyS3Test().deleteRetainObjectsBatchTest(S3Provider.ALIYUN_OSS);
    }

    @Test
    public void aliyunOSSDeleteLegalHoldObjectsBatchTest() throws IOException, NoSuchAlgorithmException {
        new CandyS3Test().deleteLegalHoldObjectsBatchTest(S3Provider.ALIYUN_OSS);
    }

    @Test
    public void aliyunOSSDeleteLegalHoldObjectTest() throws IOException, NoSuchAlgorithmException, InterruptedException {
        new CandyS3Test().deleteLegalHoldObjectTest(S3Provider.ALIYUN_OSS);
    }

    @Test
    public void aliyunOSSUpdateObjectLegalHoldTest() throws IOException, NoSuchAlgorithmException, InterruptedException {
        new CandyS3Test().updateObjectLegalHoldTest(S3Provider.ALIYUN_OSS);
    }

    @Test
    public void aliyunOSSDeleteObjectsBatchTest() throws IOException, NoSuchAlgorithmException {
        new CandyS3Test().deleteObjectsBatchTest(S3Provider.ALIYUN_OSS);
    }

}
