package io.github.matian2014.candys3;

import org.junit.AfterClass;
import org.junit.Test;

import java.io.IOException;
import java.security.NoSuchAlgorithmException;

public class AwsS3Test {

    @AfterClass
    public static void awsRemoveTestsBucket() throws IOException {
        new CandyS3Test().removeTestBuckets(S3Provider.AWS);
        System.out.println("awsRemoveTestsBucket done.");
    }

    @Test
    public void awsCreateBucketTest() throws IOException {
        new CandyS3Test().createBucketTest(S3Provider.AWS);
    }

    @Test
    public void awsCreateBucketErrorTest() throws IOException {
        new CandyS3Test().createBucketErrorTest(S3Provider.AWS);
    }

    @Test
    public void awsBucketExistsTest() throws IOException {
        new CandyS3Test().bucketExistsTest(S3Provider.AWS);
    }

    @Test
    public void awsBucketVersioningTest() throws IOException {
        new CandyS3Test().bucketVersioningTest(S3Provider.AWS);
    }

    @Test
    public void awsBucketAccelerateTest() throws IOException {
        new CandyS3Test().bucketAccelerateTest(S3Provider.AWS);
    }

    @Test
    public void awsBucketLocationTest() throws IOException {
        new CandyS3Test().bucketLocationTest(S3Provider.AWS, "us-east-1", "us-west-2");
    }

    @Test
    public void awsBucketObjectLockConfigurationTest() throws IOException, NoSuchAlgorithmException {
        new CandyS3Test().bucketObjectLockConfigurationTest(S3Provider.AWS);
    }

    @Test
    public void awsBucketPolicyTest() throws IOException, NoSuchAlgorithmException {
        new CandyS3Test().bucketPolicyTest(S3Provider.AWS);
    }

    @Test
    public void awsBucketBlockPublicAccessTest() throws IOException {
        new CandyS3Test().bucketBlockPublicAccessTest(S3Provider.AWS);
    }

    @Test
    public void awsBucketTagTest() throws IOException, NoSuchAlgorithmException {
        new CandyS3Test().bucketTagTest(S3Provider.AWS);
    }

    @Test
    public void awsBucketSSEConfigurationTest() throws IOException, NoSuchAlgorithmException {
        new CandyS3Test().bucketSSEConfigurationTest(S3Provider.AWS);
    }

    @Test
    public void awsListBucketsTest() throws IOException {
        new CandyS3Test().listBucketsTest(S3Provider.AWS);
    }

    @Test
    public void awsListBucketsFilterRegionTest() throws IOException {
        new CandyS3Test().listBucketsFilterRegionTest(S3Provider.AWS, "us-west-2");
    }

    @Test
    public void awsListBucketsPrefixTest() throws IOException {
        new CandyS3Test().listBucketsPrefixTest(S3Provider.AWS);
    }

    @Test
    public void awsListBucketsPaginationTest() throws IOException {
        new CandyS3Test().listBucketsPaginationTest(S3Provider.AWS);
    }

    @Test
    public void awsListObjectsPaginationTest() throws IOException, NoSuchAlgorithmException {
        new CandyS3Test().listObjectsPaginationTest(S3Provider.AWS);
    }

    @Test
    public void awsListObjectVersionsTest() throws IOException, NoSuchAlgorithmException {
        new CandyS3Test().listObjectVersionsTest(S3Provider.AWS);
    }

    @Test
    public void awsListObjectVersionsPaginationTest() throws IOException, NoSuchAlgorithmException, InterruptedException {
        new CandyS3Test().listObjectVersionsPaginationTest(S3Provider.AWS);
    }

    @Test
    public void awsListObjectsCommonPrefixTest() throws IOException, NoSuchAlgorithmException {
        new CandyS3Test().listObjectsCommonPrefixTest(S3Provider.AWS);
    }

    @Test
    public void awsListObjectVersionsWithoutVersioningTest() throws IOException, NoSuchAlgorithmException {
        new CandyS3Test().listObjectVersionsWithoutVersioningTest(S3Provider.AWS);
    }

    @Test
    public void awsListObjectVersionsCommonPrefixTest() throws IOException, NoSuchAlgorithmException {
        new CandyS3Test().listObjectVersionsCommonPrefixTest(S3Provider.AWS);
    }

    @Test
    public void awsGetVersioningObjectTest() throws IOException, NoSuchAlgorithmException {
        new CandyS3Test().getVersioningObjectTest(S3Provider.AWS);
    }

    @Test
    public void awsGetObjectVersionMetadataTest() throws IOException, NoSuchAlgorithmException {
        new CandyS3Test().getObjectVersionMetadataTest(S3Provider.AWS);
    }

    @Test
    public void awsDeleteVersioningObjectTest() throws IOException, NoSuchAlgorithmException {
        new CandyS3Test().deleteVersioningObjectTest(S3Provider.AWS);
    }

    @Test
    public void awsDeleteVersioningObjectsBatchTest() throws IOException, NoSuchAlgorithmException {
        new CandyS3Test().deleteVersioningObjectsBatchTest(S3Provider.AWS);
    }

    @Test
    public void awsDeleteObjectConditionalTest() throws IOException, NoSuchAlgorithmException {
        new CandyS3Test().deleteObjectConditionalTest(S3Provider.AWS);
    }

    @Test
    public void awsDeleteObjectsBatchConditionalTest() throws IOException, NoSuchAlgorithmException {
        new CandyS3Test().deleteObjectsBatchConditionalTest(S3Provider.AWS);
    }

    @Test
    public void awsPutDownloadEmptyObjectTest() throws IOException, NoSuchAlgorithmException {
        new CandyS3Test().putDownloadEmptyObjectTest(S3Provider.AWS);
    }

    @Test
    public void awsPutObjectWithNonEnglishKeyTest() throws IOException, NoSuchAlgorithmException {
        new CandyS3Test().putObjectWithNonEnglishKeyTest(S3Provider.AWS);
    }

    @Test
    public void awsPutObjectConditionalTest() throws IOException, NoSuchAlgorithmException {
        new CandyS3Test().putObjectConditionalTest(S3Provider.AWS);
    }

    @Test
    public void awsMultipartUploadConditionalTest() throws IOException, NoSuchAlgorithmException {
        new CandyS3Test().multipartUploadConditionalTest(S3Provider.AWS);
    }

    @Test
    public void awsPutObjectPropertiesTest() throws IOException, NoSuchAlgorithmException {
        new CandyS3Test().putObjectPropertiesTest(S3Provider.AWS);
    }

    @Test
    public void awsMultipartUploadPropertiesTest() throws IOException, NoSuchAlgorithmException {
        new CandyS3Test().multipartUploadPropertiesTest(S3Provider.AWS);
    }

    @Test
    public void awsPutDownloadSmallObjectTest() throws IOException, NoSuchAlgorithmException {
        new CandyS3Test().putDownloadSmallObjectTest(S3Provider.AWS);
    }

    @Test
    public void awsPutDownloadLargeObjectTest() throws IOException, NoSuchAlgorithmException {
        new CandyS3Test().putDownloadLargeObjectTest(S3Provider.AWS);
    }

    @Test
    public void awsMultipartUploadDownloadTest() throws IOException, NoSuchAlgorithmException {
        new CandyS3Test().multipartUploadDownloadTest(S3Provider.AWS);
    }

    @Test
    public void awsMultipartUploadToExistsObjectTest() throws IOException, NoSuchAlgorithmException {
        new CandyS3Test().multipartUploadToExistsObjectTest(S3Provider.AWS);
    }

    @Test
    public void awsAbortMultipartUploadToExistsObjectTest() throws IOException, NoSuchAlgorithmException {
        new CandyS3Test().abortMultipartUploadToExistsObjectTest(S3Provider.AWS);
    }

    @Test
    public void awsAbortMultipartUploadTest() throws IOException, NoSuchAlgorithmException {
        new CandyS3Test().abortMultipartUploadTest(S3Provider.AWS);
    }

    @Test
    public void awsListMultipartUploadsTest() throws IOException {
        new CandyS3Test().listMultipartUploadsTest(S3Provider.AWS);
    }


    @Test
    public void awsListMultipartUploadsPaginationTest() throws IOException, NoSuchAlgorithmException {
        new CandyS3Test().listMultipartUploadsPaginationTest(S3Provider.AWS);
    }

    @Test
    public void awsPutAndGetObjectSseTest() throws IOException, NoSuchAlgorithmException {
        new CandyS3Test().putAndGetObjectSseTest(S3Provider.AWS);
    }


    @Test
    public void awsCopyObjectAndDownloadTest() throws IOException, NoSuchAlgorithmException {
        new CandyS3Test().copyObjectAndDownloadTest(S3Provider.AWS);
    }

    @Test
    public void awsCopyObjectConditionalTest() throws IOException, NoSuchAlgorithmException {
        new CandyS3Test().copyObjectCopySourceConditionalTest(S3Provider.AWS);
    }

    @Test
    public void awsCopyObjectUnionConditionTestTest() throws IOException, NoSuchAlgorithmException {
        new CandyS3Test().copyObjectCopySourceUnionConditionTest(S3Provider.AWS);
    }

    @Test
    public void awsCopyObjectWriteTargetConditionalTest() throws IOException, NoSuchAlgorithmException {
        new CandyS3Test().copyObjectWriteTargetConditionalTest(S3Provider.AWS);
    }

    @Test
    public void awsCopyObjectVersionTest() throws IOException, NoSuchAlgorithmException {
        new CandyS3Test().copyObjectVersionTest(S3Provider.AWS);
    }

    @Test
    public void awsCopyObjectPropertiesTest() throws IOException, NoSuchAlgorithmException, InterruptedException {
        new CandyS3Test().copyObjectPropertiesTest(S3Provider.AWS);
    }

    @Test
    public void awsPutAndGetObjectLockPropertiesTest() throws IOException, NoSuchAlgorithmException, InterruptedException {
        new CandyS3Test().putAndGetObjectLockPropertiesTest(S3Provider.AWS);
    }


    @Test
    public void awsPutAndGetObjectStorageClassTest() throws IOException, NoSuchAlgorithmException {
        new CandyS3Test().putAndGetObjectStorageClassTest(S3Provider.AWS);
    }

    @Test
    public void awsPutAndGetObjectTagTest() throws IOException, NoSuchAlgorithmException {
        new CandyS3Test().putAndGetObjectTagTest(S3Provider.AWS);
    }

    @Test
    public void awsCopyPartTest() throws IOException, NoSuchAlgorithmException {
        new CandyS3Test().copyPartTest(S3Provider.AWS);
    }

    @Test
    public void awsCopyPartConditionalTest() throws IOException, NoSuchAlgorithmException {
        new CandyS3Test().copyPartConditionalTest(S3Provider.AWS);
    }

    @Test
    public void awsCopyPartWithUnionConditionTest() throws IOException, NoSuchAlgorithmException {
        new CandyS3Test().copyPartWithUnionConditionTest(S3Provider.AWS);
    }

    @Test
    public void awsCopyPartVersionTest() throws IOException, NoSuchAlgorithmException {
        new CandyS3Test().copyPartVersionTest(S3Provider.AWS);
    }


    @Test
    public void awsListPartsTest() throws IOException, NoSuchAlgorithmException {
        new CandyS3Test().listPartsTest(S3Provider.AWS);
    }

    @Test
    public void awsListPartsPaginationTest() throws IOException, NoSuchAlgorithmException {
        new CandyS3Test().listPartsPaginationTest(S3Provider.AWS);
    }

    @Test
    public void awsDownloadObjectTest() throws IOException, NoSuchAlgorithmException {
        new CandyS3Test().downloadObjectTest(S3Provider.AWS);
    }

    @Test
    public void awsDownloadObjectRangeTest() throws IOException, NoSuchAlgorithmException {
        new CandyS3Test().downloadObjectRangeTest(S3Provider.AWS);
    }

    @Test
    public void awsDownloadObjectOverwriteTest() throws IOException, NoSuchAlgorithmException {
        new CandyS3Test().downloadObjectOverwriteTest(S3Provider.AWS);
    }

    @Test
    public void awsDownloadObjectConditionalTest() throws IOException, NoSuchAlgorithmException {
        new CandyS3Test().downloadObjectConditionalTest(S3Provider.AWS);
    }

    @Test
    public void awsDownloadObjectUnionConditionTest() throws IOException, NoSuchAlgorithmException {
        new CandyS3Test().downloadObjectUnionConditionTest(S3Provider.AWS);
    }

    @Test
    public void awsDownloadObjectPartTest() throws IOException, NoSuchAlgorithmException {
        new CandyS3Test().downloadObjectPartTest(S3Provider.AWS);
    }

    @Test
    public void awsDownloadObjectMetadataTest() throws IOException, NoSuchAlgorithmException, InterruptedException {
        new CandyS3Test().downloadObjectMetadataTest(S3Provider.AWS);
    }

    @Test
    public void awsDownloadObjectObjectLockPropertiesTest() throws IOException, NoSuchAlgorithmException, InterruptedException {
        new CandyS3Test().downloadObjectObjectLockPropertiesTest(S3Provider.AWS);
    }

    @Test
    public void awsDownloadObjectStorageClassTest() throws IOException, NoSuchAlgorithmException, InterruptedException {
        new CandyS3Test().downloadObjectStorageClassTest(S3Provider.AWS);
    }

    @Test
    public void awsDownloadObjectTagTest() throws IOException, NoSuchAlgorithmException, InterruptedException {
        new CandyS3Test().downloadObjectTagTest(S3Provider.AWS);
    }

    @Test
    public void awsGetPresignUrlTest() throws IOException, NoSuchAlgorithmException, InterruptedException {
        new CandyS3Test().getWithPresignUrlTest(S3Provider.AWS);
    }

    @Test
    public void awsPutWithPresignUrlTest() throws IOException, NoSuchAlgorithmException, InterruptedException {
        new CandyS3Test().putWithPresignUrlTest(S3Provider.AWS);
    }

    @Test
    public void awsObjectRetentionTest() throws IOException, NoSuchAlgorithmException {
        new CandyS3Test().objectRetentionTest(S3Provider.AWS);
    }

    @Test
    public void awsObjectVersionRetentionTest() throws IOException, NoSuchAlgorithmException {
        new CandyS3Test().objectVersionRetentionTest(S3Provider.AWS);
    }

    @Test
    public void awsUpdateObjectGovernancePeriodTest() throws IOException, NoSuchAlgorithmException {
        new CandyS3Test().updateObjectGovernancePeriodTest(S3Provider.AWS);
    }

    @Test
    public void awsUpdateObjectCompliancePeriodTest() throws IOException, NoSuchAlgorithmException, InterruptedException {
        new CandyS3Test().updateObjectCompliancePeriodTest(S3Provider.AWS);
    }

    @Test
    public void awsUpdateObjectRetentionModeTest() throws IOException, NoSuchAlgorithmException, InterruptedException {
        new CandyS3Test().updateObjectRetentionModeTest(S3Provider.AWS);
    }

    @Test
    public void awsMultipartUploadObjectRetentionTest() throws IOException, NoSuchAlgorithmException, InterruptedException {
        new CandyS3Test().multipartUploadObjectRetentionTest(S3Provider.AWS);
    }

    @Test
    public void awsCopyObjectRetentionTest() throws IOException, NoSuchAlgorithmException, InterruptedException {
        new CandyS3Test().copyObjectRetentionTest(S3Provider.AWS);
    }

    @Test
    public void awsObjectRetentionTimezoneTest() throws IOException, NoSuchAlgorithmException, InterruptedException {
        new CandyS3Test().objectRetentionTimezoneTest(S3Provider.AWS);
    }

    @Test
    public void awsDeleteRetainObjectTest() throws IOException, NoSuchAlgorithmException, InterruptedException {
        new CandyS3Test().deleteRetainObjectTest(S3Provider.AWS);
    }

    @Test
    public void awsDeleteRetainObjectsBatchTest() throws IOException, NoSuchAlgorithmException {
        new CandyS3Test().deleteRetainObjectsBatchTest(S3Provider.AWS);
    }

    @Test
    public void awsDeleteLegalHoldObjectsBatchTest() throws IOException, NoSuchAlgorithmException {
        new CandyS3Test().deleteLegalHoldObjectsBatchTest(S3Provider.AWS);
    }

    @Test
    public void awsDeleteLegalHoldObjectTest() throws IOException, NoSuchAlgorithmException, InterruptedException {
        new CandyS3Test().deleteLegalHoldObjectTest(S3Provider.AWS);
    }

    @Test
    public void awsUpdateObjectLegalHoldTest() throws IOException, NoSuchAlgorithmException, InterruptedException {
        new CandyS3Test().updateObjectLegalHoldTest(S3Provider.AWS);
    }

    @Test
    public void awsDeleteObjectsBatchTest() throws IOException, NoSuchAlgorithmException {
        new CandyS3Test().deleteObjectsBatchTest(S3Provider.AWS);
    }

}
