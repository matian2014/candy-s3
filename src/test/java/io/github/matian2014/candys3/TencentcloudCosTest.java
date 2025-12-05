package io.github.matian2014.candys3;

import org.junit.AfterClass;
import org.junit.Ignore;
import org.junit.Test;

import java.io.IOException;
import java.security.NoSuchAlgorithmException;

public class TencentcloudCosTest {


    @AfterClass
    public static void tencentcloudCosRemoveTestsBucket() throws IOException {
        new CandyS3Test().removeTestBuckets(S3Provider.TENCENTCLOUD_COS);
        System.out.println("tencentcloudCosRemoveTestsBucket done.");
    }

    @Test
    public void tencentcloudCosCreateBucketTest() throws IOException {
        new CandyS3Test().createBucketTest(S3Provider.TENCENTCLOUD_COS);
    }

    @Test
    public void tencentcloudCosCreateBucketErrorTest() throws IOException {
        new CandyS3Test().createBucketErrorTest(S3Provider.TENCENTCLOUD_COS);
    }

    @Test
    public void tencentcloudCosBucketExistsTest() throws IOException {
        new CandyS3Test().bucketExistsTest(S3Provider.TENCENTCLOUD_COS);
    }

    @Test
    public void tencentcloudCosBucketVersioningTest() throws IOException {
        new CandyS3Test().bucketVersioningTest(S3Provider.TENCENTCLOUD_COS);
    }

    @Test
    public void tencentcloudCosBucketAccelerateTest() throws IOException {
        new CandyS3Test().bucketAccelerateTest(S3Provider.TENCENTCLOUD_COS);
    }

    @Test
    public void tencentcloudCosBucketLocationTest() throws IOException {
        new CandyS3Test().bucketLocationTest(S3Provider.TENCENTCLOUD_COS, "ap-guangzhou", "ap-chengdu");
    }

    @Test
    public void tencentcloudCosBucketObjectLockConfigurationTest() throws IOException, NoSuchAlgorithmException {
        new CandyS3Test().bucketObjectLockConfigurationTest(S3Provider.TENCENTCLOUD_COS);
    }

    @Test
    public void tencentcloudCosBucketPolicyTest() throws IOException, NoSuchAlgorithmException {
        new CandyS3Test().bucketPolicyTest(S3Provider.TENCENTCLOUD_COS);
    }

    @Test
    public void tencentcloudCosBucketBlockPublicAccessTest() throws IOException {
        new CandyS3Test().bucketBlockPublicAccessTest(S3Provider.TENCENTCLOUD_COS);
    }

    @Test
    public void tencentcloudCosBucketTagTest() throws IOException, NoSuchAlgorithmException {
        new CandyS3Test().bucketTagTest(S3Provider.TENCENTCLOUD_COS);
    }

    @Test
    public void tencentcloudCosBucketSSEConfigurationTest() throws IOException, NoSuchAlgorithmException {
        new CandyS3Test().bucketSSEConfigurationTest(S3Provider.TENCENTCLOUD_COS);
    }

    @Test
    public void tencentcloudCosListBucketsTest() throws IOException {
        new CandyS3Test().listBucketsTest(S3Provider.TENCENTCLOUD_COS);
    }

    @Test
    public void tencentcloudCosListBucketsFilterRegionTest() throws IOException {
        new CandyS3Test().listBucketsFilterRegionTest(S3Provider.TENCENTCLOUD_COS, "ap-chengdu");
    }


    @Test
    public void tencentcloudCosListBucketsPrefixTest() throws IOException {
        new CandyS3Test().listBucketsPrefixTest(S3Provider.TENCENTCLOUD_COS);
    }

    @Test
    public void tencentcloudCosListBucketsPaginationTest() throws IOException {
        new CandyS3Test().listBucketsPaginationTest(S3Provider.TENCENTCLOUD_COS);
    }

    @Test
    public void tencentcloudCosListObjectsPaginationTest() throws IOException, NoSuchAlgorithmException {
        new CandyS3Test().listObjectsPaginationTest(S3Provider.TENCENTCLOUD_COS);
    }

    @Test
    public void tencentcloudCosListObjectVersionsTest() throws IOException, NoSuchAlgorithmException {
        new CandyS3Test().listObjectVersionsTest(S3Provider.TENCENTCLOUD_COS);
    }

    @Test
    public void tencentcloudCosListObjectVersionsPaginationTest() throws IOException, NoSuchAlgorithmException, InterruptedException {
        new CandyS3Test().listObjectVersionsPaginationTest(S3Provider.TENCENTCLOUD_COS);
    }

    @Test
    public void tencentcloudCosListObjectsCommonPrefixTest() throws IOException, NoSuchAlgorithmException {
        new CandyS3Test().listObjectsCommonPrefixTest(S3Provider.TENCENTCLOUD_COS);
    }

    @Test
    public void tencentcloudCosListObjectVersionsWithoutVersioningTest() throws IOException, NoSuchAlgorithmException {
        new CandyS3Test().listObjectVersionsWithoutVersioningTest(S3Provider.TENCENTCLOUD_COS);
    }

    @Test
    public void tencentcloudCosListObjectVersionsCommonPrefixTest() throws IOException, NoSuchAlgorithmException {
        new CandyS3Test().listObjectVersionsCommonPrefixTest(S3Provider.TENCENTCLOUD_COS);
    }

    @Test
    public void tencentcloudCosGetVersioningObjectTest() throws IOException, NoSuchAlgorithmException {
        new CandyS3Test().getVersioningObjectTest(S3Provider.TENCENTCLOUD_COS);
    }

    @Test
    public void tencentcloudCosGetObjectVersionMetadataTest() throws IOException, NoSuchAlgorithmException {
        new CandyS3Test().getObjectVersionMetadataTest(S3Provider.TENCENTCLOUD_COS);
    }

    @Test
    public void tencentcloudCosDeleteVersioningObjectTest() throws IOException, NoSuchAlgorithmException {
        new CandyS3Test().deleteVersioningObjectTest(S3Provider.TENCENTCLOUD_COS);
    }

    @Test
    public void tencentcloudCosDeleteVersioningObjectsBatchTest() throws IOException, NoSuchAlgorithmException {
        new CandyS3Test().deleteVersioningObjectsBatchTest(S3Provider.TENCENTCLOUD_COS);
    }

    @Test
    public void tencentcloudCosDeleteObjectConditionalTest() throws IOException, NoSuchAlgorithmException {
        new CandyS3Test().deleteObjectConditionalTest(S3Provider.TENCENTCLOUD_COS);
    }

    @Test
    public void tencentcloudCosDeleteObjectsBatchConditionalTest() throws IOException, NoSuchAlgorithmException {
        new CandyS3Test().deleteObjectsBatchConditionalTest(S3Provider.TENCENTCLOUD_COS);
    }

    @Test
    public void tencentcloudCosPutDownloadEmptyObjectTest() throws IOException, NoSuchAlgorithmException {
        new CandyS3Test().putDownloadEmptyObjectTest(S3Provider.TENCENTCLOUD_COS);
    }

    @Test
    public void tencentcloudCosPutObjectWithNonEnglishKeyTest() throws IOException, NoSuchAlgorithmException {
        new CandyS3Test().putObjectWithNonEnglishKeyTest(S3Provider.TENCENTCLOUD_COS);
    }

    @Test
    public void tencentcloudCosPutObjectConditionalTest() throws IOException, NoSuchAlgorithmException {
        new CandyS3Test().putObjectConditionalTest(S3Provider.TENCENTCLOUD_COS);
    }

    @Test
    public void tencentcloudCosMultipartUploadConditionalTest() throws IOException, NoSuchAlgorithmException {
        new CandyS3Test().multipartUploadConditionalTest(S3Provider.TENCENTCLOUD_COS);
    }

    @Test
    public void tencentcloudCosPutObjectPropertiesTest() throws IOException, NoSuchAlgorithmException {
        new CandyS3Test().putObjectPropertiesTest(S3Provider.TENCENTCLOUD_COS);
    }

    @Test
    public void tencentcloudCosMultipartUploadPropertiesTest() throws IOException, NoSuchAlgorithmException {
        new CandyS3Test().multipartUploadPropertiesTest(S3Provider.TENCENTCLOUD_COS);
    }

    @Test
    public void tencentcloudCosPutDownloadSmallObjectTest() throws IOException, NoSuchAlgorithmException {
        new CandyS3Test().putDownloadSmallObjectTest(S3Provider.TENCENTCLOUD_COS);
    }

    @Test
    public void tencentcloudCosPutDownloadLargeObjectTest() throws IOException, NoSuchAlgorithmException {
        new CandyS3Test().putDownloadLargeObjectTest(S3Provider.TENCENTCLOUD_COS);
    }

    @Test
    public void tencentcloudCosMultipartUploadDownloadTest() throws IOException, NoSuchAlgorithmException {
        new CandyS3Test().multipartUploadDownloadTest(S3Provider.TENCENTCLOUD_COS);
    }

    @Test
    public void tencentcloudCosMultipartUploadToExistsObjectTest() throws IOException, NoSuchAlgorithmException {
        new CandyS3Test().multipartUploadToExistsObjectTest(S3Provider.TENCENTCLOUD_COS);
    }

    @Test
    public void tencentcloudCosAbortMultipartUploadToExistsObjectTest() throws IOException, NoSuchAlgorithmException {
        new CandyS3Test().abortMultipartUploadToExistsObjectTest(S3Provider.TENCENTCLOUD_COS);
    }

    @Test
    public void tencentcloudCosAbortMultipartUploadTest() throws IOException, NoSuchAlgorithmException {
        new CandyS3Test().abortMultipartUploadTest(S3Provider.TENCENTCLOUD_COS);
    }

    @Test
    public void tencentcloudCosListMultipartUploadsTest() throws IOException {
        new CandyS3Test().listMultipartUploadsTest(S3Provider.TENCENTCLOUD_COS);
    }


    @Test
    public void tencentcloudCosListMultipartUploadsPaginationTest() throws IOException, NoSuchAlgorithmException {
        new CandyS3Test().listMultipartUploadsPaginationTest(S3Provider.TENCENTCLOUD_COS);
    }

    @Test
    public void tencentcloudCosPutAndGetObjectSseTest() throws IOException, NoSuchAlgorithmException {
        new CandyS3Test().putAndGetObjectSseTest(S3Provider.TENCENTCLOUD_COS);
    }


    @Test
    public void tencentcloudCosCopyObjectAndDownloadTest() throws IOException, NoSuchAlgorithmException {
        new CandyS3Test().copyObjectAndDownloadTest(S3Provider.TENCENTCLOUD_COS);
    }

    @Test
    public void tencentcloudCosCopyObjectConditionalTest() throws IOException, NoSuchAlgorithmException {
        new CandyS3Test().copyObjectCopySourceConditionalTest(S3Provider.TENCENTCLOUD_COS);
    }

    @Test
    public void tencentcloudCosCopyObjectUnionConditionTestTest() throws IOException, NoSuchAlgorithmException {
        new CandyS3Test().copyObjectCopySourceUnionConditionTest(S3Provider.TENCENTCLOUD_COS);
    }

    @Test
    public void tencentcloudCosCopyObjectWriteTargetConditionalTest() throws IOException, NoSuchAlgorithmException {
        new CandyS3Test().copyObjectWriteTargetConditionalTest(S3Provider.TENCENTCLOUD_COS);
    }

    @Test
    public void tencentcloudCosCopyObjectVersionTest() throws IOException, NoSuchAlgorithmException {
        new CandyS3Test().copyObjectVersionTest(S3Provider.TENCENTCLOUD_COS);
    }

    @Test
    public void tencentcloudCosCopyObjectPropertiesTest() throws IOException, NoSuchAlgorithmException, InterruptedException {
        new CandyS3Test().copyObjectPropertiesTest(S3Provider.TENCENTCLOUD_COS);
    }

    @Test
    public void tencentcloudCosPutAndGetObjectLockPropertiesTest() throws IOException, NoSuchAlgorithmException, InterruptedException {
        new CandyS3Test().putAndGetObjectLockPropertiesTest(S3Provider.TENCENTCLOUD_COS);
    }

    @Test
    public void tencentcloudCosPutAndGetObjectStorageClassTest() throws IOException, NoSuchAlgorithmException {
        new CandyS3Test().putAndGetObjectStorageClassTest(S3Provider.TENCENTCLOUD_COS);
    }

    @Test
    public void tencentcloudCosPutAndGetObjectTagTest() throws IOException, NoSuchAlgorithmException {
        new CandyS3Test().putAndGetObjectTagTest(S3Provider.TENCENTCLOUD_COS);
    }

    @Test
    public void tencentcloudCosCopyPartTest() throws IOException, NoSuchAlgorithmException {
        new CandyS3Test().copyPartTest(S3Provider.TENCENTCLOUD_COS);
    }

    @Test
    public void tencentcloudCosCopyPartConditionalTest() throws IOException, NoSuchAlgorithmException {
        new CandyS3Test().copyPartConditionalTest(S3Provider.TENCENTCLOUD_COS);
    }

    @Test
    public void tencentcloudCosCopyPartWithUnionConditionTest() throws IOException, NoSuchAlgorithmException {
        new CandyS3Test().copyPartWithUnionConditionTest(S3Provider.TENCENTCLOUD_COS);
    }

    @Test
    public void tencentcloudCosCopyPartVersionTest() throws IOException, NoSuchAlgorithmException {
        new CandyS3Test().copyPartVersionTest(S3Provider.TENCENTCLOUD_COS);
    }


    @Test
    public void tencentcloudCosListPartsTest() throws IOException, NoSuchAlgorithmException {
        new CandyS3Test().listPartsTest(S3Provider.TENCENTCLOUD_COS);
    }

    @Test
    public void tencentcloudCosListPartsPaginationTest() throws IOException, NoSuchAlgorithmException {
        new CandyS3Test().listPartsPaginationTest(S3Provider.TENCENTCLOUD_COS);
    }

    @Test
    public void tencentcloudCosDownloadObjectTest() throws IOException, NoSuchAlgorithmException {
        new CandyS3Test().downloadObjectTest(S3Provider.TENCENTCLOUD_COS);
    }

    @Test
    public void tencentcloudCosDownloadObjectRangeTest() throws IOException, NoSuchAlgorithmException {
        new CandyS3Test().downloadObjectRangeTest(S3Provider.TENCENTCLOUD_COS);
    }

    @Test
    public void tencentcloudCosDownloadObjectOverwriteTest() throws IOException, NoSuchAlgorithmException {
        new CandyS3Test().downloadObjectOverwriteTest(S3Provider.TENCENTCLOUD_COS);
    }

    @Test
    public void tencentcloudCosDownloadObjectConditionalTest() throws IOException, NoSuchAlgorithmException {
        new CandyS3Test().downloadObjectConditionalTest(S3Provider.TENCENTCLOUD_COS);
    }

    @Test
    public void tencentcloudCosDownloadObjectUnionConditionTest() throws IOException, NoSuchAlgorithmException {
        new CandyS3Test().downloadObjectUnionConditionTest(S3Provider.TENCENTCLOUD_COS);
    }

    @Test
    public void tencentcloudCosDownloadObjectPartTest() throws IOException, NoSuchAlgorithmException {
        new CandyS3Test().downloadObjectPartTest(S3Provider.TENCENTCLOUD_COS);
    }

    @Test
    public void tencentcloudCosDownloadObjectMetadataTest() throws IOException, NoSuchAlgorithmException, InterruptedException {
        new CandyS3Test().downloadObjectMetadataTest(S3Provider.TENCENTCLOUD_COS);
    }

    @Test
    public void tencentcloudCosDownloadObjectObjectLockPropertiesTest() throws IOException, NoSuchAlgorithmException, InterruptedException {
        new CandyS3Test().downloadObjectObjectLockPropertiesTest(S3Provider.TENCENTCLOUD_COS);
    }

    @Test
    public void tencentcloudCosDownloadObjectStorageClassTest() throws IOException, NoSuchAlgorithmException, InterruptedException {
        new CandyS3Test().downloadObjectStorageClassTest(S3Provider.TENCENTCLOUD_COS);
    }

    @Test
    public void tencentcloudCosDownloadObjectTagTest() throws IOException, NoSuchAlgorithmException, InterruptedException {
        new CandyS3Test().downloadObjectTagTest(S3Provider.TENCENTCLOUD_COS);
    }

    @Test
    public void tencentcloudCosGetPresignUrlTestTest() throws IOException, NoSuchAlgorithmException, InterruptedException {
        new CandyS3Test().getWithPresignUrlTest(S3Provider.TENCENTCLOUD_COS);
    }

    @Test
    public void tencentcloudCosPutWithPresignUrlTest() throws IOException, NoSuchAlgorithmException, InterruptedException {
        new CandyS3Test().putWithPresignUrlTest(S3Provider.TENCENTCLOUD_COS);
    }

    @Ignore("invalid object lock header, x-cos-object-lock mode must be COMPLIANCE")
    @Test
    public void tencentcloudCosObjectRetentionTest() throws IOException, NoSuchAlgorithmException {
        new CandyS3Test().objectRetentionTest(S3Provider.TENCENTCLOUD_COS);
    }

    @Ignore("Bucket versioning is not allowed when bucket object-lock configuration is enabled. Enable bucket lock configuration is not allowed when the bucket versioning is enabled or suspended.")
    @Test
    public void tencentcloudCosObjectVersionRetentionTest() throws IOException, NoSuchAlgorithmException {
        new CandyS3Test().objectVersionRetentionTest(S3Provider.TENCENTCLOUD_COS);
    }

    @Ignore("invalid object lock header, x-cos-object-lock mode must be COMPLIANCE")
    @Test
    public void tencentcloudCosUpdateObjectGovernancePeriodTest() throws IOException, NoSuchAlgorithmException {
        new CandyS3Test().updateObjectGovernancePeriodTest(S3Provider.TENCENTCLOUD_COS);
    }

    @Test
    public void tencentcloudCosUpdateObjectCompliancePeriodTest() throws IOException, NoSuchAlgorithmException, InterruptedException {
        new CandyS3Test().updateObjectCompliancePeriodTest(S3Provider.TENCENTCLOUD_COS);
    }

    @Ignore("invalid object lock header, x-cos-object-lock mode must be COMPLIANCE")
    @Test
    public void tencentcloudCosUpdateObjectRetentionModeTest() throws IOException, NoSuchAlgorithmException, InterruptedException {
        new CandyS3Test().updateObjectRetentionModeTest(S3Provider.TENCENTCLOUD_COS);
    }

    @Test
    public void tencentcloudCosMultipartUploadObjectRetentionTest() throws IOException, NoSuchAlgorithmException, InterruptedException {
        new CandyS3Test().multipartUploadObjectRetentionTest(S3Provider.TENCENTCLOUD_COS);
    }

    @Test
    public void tencentcloudCosCopyObjectRetentionTest() throws IOException, NoSuchAlgorithmException, InterruptedException {
        new CandyS3Test().copyObjectRetentionTest(S3Provider.TENCENTCLOUD_COS);
    }

    @Test
    public void tencentcloudCosObjectRetentionTimezoneTest() throws IOException, NoSuchAlgorithmException, InterruptedException {
        new CandyS3Test().objectRetentionTimezoneTest(S3Provider.TENCENTCLOUD_COS);
    }

    @Ignore("invalid object lock header, x-cos-object-lock mode must be COMPLIANCE")
    @Test
    public void tencentcloudCosDeleteRetainObjectTest() throws IOException, NoSuchAlgorithmException, InterruptedException {
        new CandyS3Test().deleteRetainObjectTest(S3Provider.TENCENTCLOUD_COS);
    }

    @Ignore("invalid object lock header, x-cos-object-lock mode must be COMPLIANCE")
    @Test
    public void tencentcloudCosDeleteRetainObjectsBatchTest() throws IOException, NoSuchAlgorithmException {
        new CandyS3Test().deleteRetainObjectsBatchTest(S3Provider.TENCENTCLOUD_COS);
    }

    @Test
    public void tencentcloudCosDeleteLegalHoldObjectsBatchTest() throws IOException, NoSuchAlgorithmException {
        new CandyS3Test().deleteLegalHoldObjectsBatchTest(S3Provider.TENCENTCLOUD_COS);
    }

    @Ignore("Delete object without versionId fail when the object is in legal hold: The object is locked, you are not allowd to put/delete object or modify the metadata via copy object.")
    @Test
    public void tencentcloudCosDeleteLegalHoldObjectTest() throws IOException, NoSuchAlgorithmException, InterruptedException {
        new CandyS3Test().deleteLegalHoldObjectTest(S3Provider.TENCENTCLOUD_COS);
    }

    @Test
    public void tencentcloudCosUpdateObjectLegalHoldTest() throws IOException, NoSuchAlgorithmException, InterruptedException {
        new CandyS3Test().updateObjectLegalHoldTest(S3Provider.TENCENTCLOUD_COS);
    }

    @Test
    public void tencentcloudCosDeleteObjectsBatchTest() throws IOException, NoSuchAlgorithmException {
        new CandyS3Test().deleteObjectsBatchTest(S3Provider.TENCENTCLOUD_COS);
    }

}
