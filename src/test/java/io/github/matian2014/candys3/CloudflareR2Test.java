package io.github.matian2014.candys3;

import org.junit.AfterClass;
import org.junit.Ignore;
import org.junit.Test;

import java.io.IOException;
import java.security.NoSuchAlgorithmException;

public class CloudflareR2Test {

    @AfterClass
    public static void cloudflareR2RemoveTestsBucket() throws IOException {
        new CandyS3Test().removeTestBuckets(S3Provider.CLOUDFLARE_R2);
        System.out.println("cloudflareR2RemoveTestsBucket done.");
    }

    @Test
    public void cloudflareR2CreateBucketTest() throws IOException {
        new CandyS3Test().createBucketTest(S3Provider.CLOUDFLARE_R2);
    }

    @Test
    public void cloudflareR2CreateBucketErrorTest() throws IOException {
        new CandyS3Test().createBucketErrorTest(S3Provider.CLOUDFLARE_R2);
    }

    @Test
    public void cloudflareR2BucketExistsTest() throws IOException {
        new CandyS3Test().bucketExistsTest(S3Provider.CLOUDFLARE_R2);
    }

    @Ignore("GetBucketVersioning and PutBucketVersioning not implemented")
    @Test
    public void cloudflareR2BucketVersioningTest() throws IOException {
        new CandyS3Test().bucketVersioningTest(S3Provider.CLOUDFLARE_R2);
    }

    @Ignore("GetBucketAccelerateConfiguration and PutBucketAccelerateConfiguration not implemented")
    @Test
    public void cloudflareR2BucketAccelerateTest() throws IOException {
        new CandyS3Test().bucketAccelerateTest(S3Provider.CLOUDFLARE_R2);
    }

    @Ignore("When using the S3 API, the region for an R2 bucket is auto. For compatibility with tools that do not allow you to specify a region, an empty value and us-east-1 will alias to the auto region.")
    @Test
    public void cloudflareR2BucketLocationTest() throws IOException {
        new CandyS3Test().bucketLocationTest(S3Provider.CLOUDFLARE_R2, "us-east-1", "us-west-2");
    }

    @Ignore("Object lock not implemented")
    @Test
    public void cloudflareR2BucketObjectLockConfigurationTest() throws IOException, NoSuchAlgorithmException {
        new CandyS3Test().bucketObjectLockConfigurationTest(S3Provider.CLOUDFLARE_R2);
    }

    @Test
    public void cloudflareR2BucketPolicyTest() throws IOException, NoSuchAlgorithmException {
        new CandyS3Test().bucketPolicyTest(S3Provider.CLOUDFLARE_R2);
    }

    @Ignore("GetBucketBlockPublicAccess not implemented")
    @Test
    public void cloudflareR2BucketBlockPublicAccessTest() throws IOException {
        new CandyS3Test().bucketBlockPublicAccessTest(S3Provider.CLOUDFLARE_R2);
    }

    @Ignore("GetObjectTagging, PutBucketTagging and DeleteObjectTagging not implemented")
    @Test
    public void cloudflareR2BucketTagTest() throws IOException, NoSuchAlgorithmException {
        new CandyS3Test().bucketTagTest(S3Provider.CLOUDFLARE_R2);
    }

    @Test
    public void cloudflareR2BucketSSEConfigurationTest() throws IOException, NoSuchAlgorithmException {
        new CandyS3Test().bucketSSEConfigurationTest(S3Provider.CLOUDFLARE_R2);
    }

    @Test
    public void cloudflareR2ListBucketsTest() throws IOException {
        new CandyS3Test().listBucketsTest(S3Provider.CLOUDFLARE_R2);
    }

    @Test
    @Ignore("When using the S3 API, the region for an R2 bucket is auto. For compatibility with tools that do not allow you to specify a region, an empty value and us-east-1 will alias to the auto region.")
    public void cloudflareR2ListBucketsFilterRegionTest() throws IOException {
        new CandyS3Test().listBucketsFilterRegionTest(S3Provider.CLOUDFLARE_R2, "us-west-2");
    }

    @Test
    public void cloudflareR2ListBucketsPrefixTest() throws IOException {
        new CandyS3Test().listBucketsPrefixTest(S3Provider.CLOUDFLARE_R2);
    }

    @Ignore("ListBuckets search parameter max-buckets not implemented")
    @Test
    public void cloudflareR2ListBucketsPaginationTest() throws IOException {
        new CandyS3Test().listBucketsPaginationTest(S3Provider.CLOUDFLARE_R2);
    }

    @Test
    public void cloudflareR2ListObjectsPaginationTest() throws IOException, NoSuchAlgorithmException {
        new CandyS3Test().listObjectsPaginationTest(S3Provider.CLOUDFLARE_R2);
    }

    @Ignore("GetBucketVersioning and PutBucketVersioning not implemented")
    @Test
    public void cloudflareR2ListObjectVersionsTest() throws IOException, NoSuchAlgorithmException {
        new CandyS3Test().listObjectVersionsTest(S3Provider.CLOUDFLARE_R2);
    }

    @Ignore("GetBucketVersioning and PutBucketVersioning not implemented")
    @Test
    public void cloudflareR2ListObjectVersionsPaginationTest() throws IOException, NoSuchAlgorithmException, InterruptedException {
        new CandyS3Test().listObjectVersionsPaginationTest(S3Provider.CLOUDFLARE_R2);
    }

    @Test
    public void cloudflareR2ListObjectsCommonPrefixTest() throws IOException, NoSuchAlgorithmException {
        new CandyS3Test().listObjectsCommonPrefixTest(S3Provider.CLOUDFLARE_R2);
    }

    @Ignore("ListObjectVersions not implemented")
    @Test
    public void cloudflareR2ListObjectVersionsWithoutVersioningTest() throws IOException, NoSuchAlgorithmException {
        new CandyS3Test().listObjectVersionsWithoutVersioningTest(S3Provider.CLOUDFLARE_R2);
    }

    @Ignore("ListObjectVersions not implemented")
    @Test
    public void cloudflareR2ListObjectVersionsCommonPrefixTest() throws IOException, NoSuchAlgorithmException {
        new CandyS3Test().listObjectVersionsCommonPrefixTest(S3Provider.CLOUDFLARE_R2);
    }

    @Ignore("GetBucketVersioning and PutBucketVersioning not implemented")
    @Test
    public void cloudflareR2GetVersioningObjectTest() throws IOException, NoSuchAlgorithmException {
        new CandyS3Test().getVersioningObjectTest(S3Provider.CLOUDFLARE_R2);
    }

    @Ignore("GetBucketVersioning and PutBucketVersioning not implemented")
    @Test
    public void cloudflareR2GetObjectVersionMetadataTest() throws IOException, NoSuchAlgorithmException {
        new CandyS3Test().getObjectVersionMetadataTest(S3Provider.CLOUDFLARE_R2);
    }

    @Ignore("GetBucketVersioning and PutBucketVersioning not implemented")
    @Test
    public void cloudflareR2DeleteVersioningObjectTest() throws IOException, NoSuchAlgorithmException {
        new CandyS3Test().deleteVersioningObjectTest(S3Provider.CLOUDFLARE_R2);
    }

    @Ignore("GetBucketVersioning and PutBucketVersioning not implemented")
    @Test
    public void cloudflareR2DeleteVersioningObjectsBatchTest() throws IOException, NoSuchAlgorithmException {
        new CandyS3Test().deleteVersioningObjectsBatchTest(S3Provider.CLOUDFLARE_R2);
    }

    @Test
    public void cloudflareR2DeleteObjectConditionalTest() throws IOException, NoSuchAlgorithmException {
        new CandyS3Test().deleteObjectConditionalTest(S3Provider.CLOUDFLARE_R2);
    }

    @Test
    public void cloudflareR2DeleteObjectsBatchConditionalTest() throws IOException, NoSuchAlgorithmException {
        new CandyS3Test().deleteObjectsBatchConditionalTest(S3Provider.CLOUDFLARE_R2);
    }

    @Test
    public void cloudflareR2PutDownloadEmptyObjectTest() throws IOException, NoSuchAlgorithmException {
        new CandyS3Test().putDownloadEmptyObjectTest(S3Provider.CLOUDFLARE_R2);
    }

    @Test
    public void cloudflareR2PutObjectWithNonEnglishKeyTest() throws IOException, NoSuchAlgorithmException {
        new CandyS3Test().putObjectWithNonEnglishKeyTest(S3Provider.CLOUDFLARE_R2);
    }

    @Test
    public void cloudflareR2PutObjectConditionalTest() throws IOException, NoSuchAlgorithmException {
        new CandyS3Test().putObjectConditionalTest(S3Provider.CLOUDFLARE_R2);
    }

    @Test
    public void cloudflareR2MultipartUploadConditionalTest() throws IOException, NoSuchAlgorithmException {
        new CandyS3Test().multipartUploadConditionalTest(S3Provider.CLOUDFLARE_R2);
    }

    @Test
    public void cloudflareR2PutObjectPropertiesTest() throws IOException, NoSuchAlgorithmException {
        new CandyS3Test().putObjectPropertiesTest(S3Provider.CLOUDFLARE_R2);
    }

    @Test
    public void cloudflareR2MultipartUploadPropertiesTest() throws IOException, NoSuchAlgorithmException {
        new CandyS3Test().multipartUploadPropertiesTest(S3Provider.CLOUDFLARE_R2);
    }

    @Test
    public void cloudflareR2PutDownloadSmallObjectTest() throws IOException, NoSuchAlgorithmException {
        new CandyS3Test().putDownloadSmallObjectTest(S3Provider.CLOUDFLARE_R2);
    }

    @Test
    public void cloudflareR2PutDownloadLargeObjectTest() throws IOException, NoSuchAlgorithmException {
        new CandyS3Test().putDownloadLargeObjectTest(S3Provider.CLOUDFLARE_R2);
    }

    @Test
    public void cloudflareR2MultipartUploadDownloadTest() throws IOException, NoSuchAlgorithmException {
        new CandyS3Test().multipartUploadDownloadTest(S3Provider.CLOUDFLARE_R2);
    }

    @Test
    public void cloudflareR2MultipartUploadToExistsObjectTest() throws IOException, NoSuchAlgorithmException {
        new CandyS3Test().multipartUploadToExistsObjectTest(S3Provider.CLOUDFLARE_R2);
    }

    @Test
    public void cloudflareR2AbortMultipartUploadToExistsObjectTest() throws IOException, NoSuchAlgorithmException {
        new CandyS3Test().abortMultipartUploadToExistsObjectTest(S3Provider.CLOUDFLARE_R2);
    }

    @Test
    public void cloudflareR2AbortMultipartUploadTest() throws IOException, NoSuchAlgorithmException {
        // TODO listMultipartUploads for the only object and multipartupload, but get different uploadId everytime?
        new CandyS3Test().abortMultipartUploadTest(S3Provider.CLOUDFLARE_R2);
    }

    @Test
    public void cloudflareR2ListMultipartUploadsTest() throws IOException {
        new CandyS3Test().listMultipartUploadsTest(S3Provider.CLOUDFLARE_R2);
    }


    @Test
    public void cloudflareR2ListMultipartUploadsPaginationTest() throws IOException, NoSuchAlgorithmException {
        new CandyS3Test().listMultipartUploadsPaginationTest(S3Provider.CLOUDFLARE_R2);
    }

    @Ignore("SSE not implemented for putObject/CreateMultipartUpload/CopyObject")
    @Test
    public void cloudflareR2PutAndGetObjectSseTest() throws IOException, NoSuchAlgorithmException {
        new CandyS3Test().putAndGetObjectSseTest(S3Provider.CLOUDFLARE_R2);
    }


    @Test
    public void cloudflareR2CopyObjectAndDownloadTest() throws IOException, NoSuchAlgorithmException {
        new CandyS3Test().copyObjectAndDownloadTest(S3Provider.CLOUDFLARE_R2);
    }

    @Test
    public void cloudflareR2CopyObjectConditionalTest() throws IOException, NoSuchAlgorithmException {
        new CandyS3Test().copyObjectCopySourceConditionalTest(S3Provider.CLOUDFLARE_R2);
    }

    @Test
    public void cloudflareR2CopyObjectUnionConditionTestTest() throws IOException, NoSuchAlgorithmException {
        new CandyS3Test().copyObjectCopySourceUnionConditionTest(S3Provider.CLOUDFLARE_R2);
    }

    @Test
    public void cloudflareR2CopyObjectWriteTargetConditionalTest() throws IOException, NoSuchAlgorithmException {
        new CandyS3Test().copyObjectWriteTargetConditionalTest(S3Provider.CLOUDFLARE_R2);
    }

    @Ignore("GetBucketVersioning and PutBucketVersioning not implemented")
    @Test
    public void cloudflareR2CopyObjectVersionTest() throws IOException, NoSuchAlgorithmException {
        new CandyS3Test().copyObjectVersionTest(S3Provider.CLOUDFLARE_R2);
    }

    @Test
    public void cloudflareR2CopyObjectPropertiesTest() throws IOException, NoSuchAlgorithmException, InterruptedException {
        new CandyS3Test().copyObjectPropertiesTest(S3Provider.CLOUDFLARE_R2);
    }

    @Ignore("Object lock not implemented")
    @Test
    public void cloudflareR2PutAndGetObjectLockPropertiesTest() throws IOException, NoSuchAlgorithmException, InterruptedException {
        new CandyS3Test().putAndGetObjectLockPropertiesTest(S3Provider.CLOUDFLARE_R2);
    }


    @Test
    // TODO: Cloudflare R2: The free tier only applies to Standard storage, and does not apply to Infrequent Access storage.
    @Ignore("Note: The free tier only applies to Standard storage, and does not apply to Infrequent Access storage.")
    public void cloudflareR2PutAndGetObjectStorageClassTest() throws IOException, NoSuchAlgorithmException {
        new CandyS3Test().putAndGetObjectStorageClassTest(S3Provider.CLOUDFLARE_R2);
    }

    @Ignore("Header x-amz-tagging and x-amz-tagging-directive not implemented")
    @Test
    public void cloudflareR2PutAndGetObjectTagTest() throws IOException, NoSuchAlgorithmException {
        new CandyS3Test().putAndGetObjectTagTest(S3Provider.CLOUDFLARE_R2);
    }

    // TODO: Cloudflare R2: All non-trailing parts must have the same length.
    @Test
    public void cloudflareR2CopyPartTest() throws IOException, NoSuchAlgorithmException {
        new CandyS3Test().copyPartTest(S3Provider.CLOUDFLARE_R2);
    }

    @Ignore("CopyPart conditional operations not implemented")
    @Test
    public void cloudflareR2CopyPartConditionalTest() throws IOException, NoSuchAlgorithmException {
        new CandyS3Test().copyPartConditionalTest(S3Provider.CLOUDFLARE_R2);
    }

    @Test
    public void cloudflareR2CopyPartWithUnionConditionTest() throws IOException, NoSuchAlgorithmException {
        new CandyS3Test().copyPartWithUnionConditionTest(S3Provider.CLOUDFLARE_R2);
    }

    @Ignore("GetBucketVersioning and PutBucketVersioning not implemented")
    @Test
    public void cloudflareR2CopyPartVersionTest() throws IOException, NoSuchAlgorithmException {
        new CandyS3Test().copyPartVersionTest(S3Provider.CLOUDFLARE_R2);
    }


    @Test
    public void cloudflareR2ListPartsTest() throws IOException, NoSuchAlgorithmException {
        new CandyS3Test().listPartsTest(S3Provider.CLOUDFLARE_R2);
    }

    @Test
    public void cloudflareR2ListPartsPaginationTest() throws IOException, NoSuchAlgorithmException {
        new CandyS3Test().listPartsPaginationTest(S3Provider.CLOUDFLARE_R2);
    }

    @Test
    public void cloudflareR2DownloadObjectTest() throws IOException, NoSuchAlgorithmException {
        new CandyS3Test().downloadObjectTest(S3Provider.CLOUDFLARE_R2);
    }

    @Test
    public void cloudflareR2DownloadObjectRangeTest() throws IOException, NoSuchAlgorithmException {
        new CandyS3Test().downloadObjectRangeTest(S3Provider.CLOUDFLARE_R2);
    }

    @Test
    public void cloudflareR2DownloadObjectOverwriteTest() throws IOException, NoSuchAlgorithmException {
        new CandyS3Test().downloadObjectOverwriteTest(S3Provider.CLOUDFLARE_R2);
    }

    @Test
    public void cloudflareR2DownloadObjectConditionalTest() throws IOException, NoSuchAlgorithmException {
        new CandyS3Test().downloadObjectConditionalTest(S3Provider.CLOUDFLARE_R2);
    }

    @Test
    public void cloudflareR2DownloadObjectUnionConditionTest() throws IOException, NoSuchAlgorithmException {
        new CandyS3Test().downloadObjectUnionConditionTest(S3Provider.CLOUDFLARE_R2);
    }

    @Test
    public void cloudflareR2DownloadObjectPartTest() throws IOException, NoSuchAlgorithmException {
        new CandyS3Test().downloadObjectPartTest(S3Provider.CLOUDFLARE_R2);
    }

    @Test
    public void cloudflareR2DownloadObjectMetadataTest() throws IOException, NoSuchAlgorithmException, InterruptedException {
        new CandyS3Test().downloadObjectMetadataTest(S3Provider.CLOUDFLARE_R2);
    }

    @Ignore("Object lock not implemented")
    @Test
    public void cloudflareR2DownloadObjectObjectLockPropertiesTest() throws IOException, NoSuchAlgorithmException, InterruptedException {
        new CandyS3Test().downloadObjectObjectLockPropertiesTest(S3Provider.CLOUDFLARE_R2);
    }

    @Test
    // TODO: Cloudflare R2: The free tier only applies to Standard storage, and does not apply to Infrequent Access storage.
    @Ignore("Note: The free tier only applies to Standard storage, and does not apply to Infrequent Access storage.")
    public void cloudflareR2DownloadObjectStorageClassTest() throws IOException, NoSuchAlgorithmException, InterruptedException {
        new CandyS3Test().downloadObjectStorageClassTest(S3Provider.CLOUDFLARE_R2);
    }

    @Ignore("Header x-amz-tagging and x-amz-tagging-directive not implemented")
    @Test
    public void cloudflareR2DownloadObjectTagTest() throws IOException, NoSuchAlgorithmException, InterruptedException {
        new CandyS3Test().downloadObjectTagTest(S3Provider.CLOUDFLARE_R2);
    }

    @Test
    public void cloudflareR2GetPresignUrlTest() throws IOException, NoSuchAlgorithmException, InterruptedException {
        new CandyS3Test().getWithPresignUrlTest(S3Provider.CLOUDFLARE_R2);
    }

    @Test
    public void cloudflareR2PutWithPresignUrlTest() throws IOException, NoSuchAlgorithmException, InterruptedException {
        new CandyS3Test().putWithPresignUrlTest(S3Provider.CLOUDFLARE_R2);
    }

    @Ignore("Object lock not implemented")
    @Test
    public void cloudflareR2ObjectRetentionTest() throws IOException, NoSuchAlgorithmException {
        new CandyS3Test().objectRetentionTest(S3Provider.CLOUDFLARE_R2);
    }

    @Ignore("GetBucketVersioning and PutBucketVersioning not implemented")
    @Test
    public void cloudflareR2ObjectVersionRetentionTest() throws IOException, NoSuchAlgorithmException {
        new CandyS3Test().objectVersionRetentionTest(S3Provider.CLOUDFLARE_R2);
    }

    @Ignore("Object lock not implemented")
    @Test
    public void cloudflareR2UpdateObjectGovernancePeriodTest() throws IOException, NoSuchAlgorithmException {
        new CandyS3Test().updateObjectGovernancePeriodTest(S3Provider.CLOUDFLARE_R2);
    }

    @Ignore("Object lock not implemented")
    @Test
    public void cloudflareR2UpdateObjectCompliancePeriodTest() throws IOException, NoSuchAlgorithmException, InterruptedException {
        new CandyS3Test().updateObjectCompliancePeriodTest(S3Provider.CLOUDFLARE_R2);
    }

    @Ignore("Object lock not implemented")
    @Test
    public void cloudflareR2UpdateObjectRetentionModeTest() throws IOException, NoSuchAlgorithmException, InterruptedException {
        new CandyS3Test().updateObjectRetentionModeTest(S3Provider.CLOUDFLARE_R2);
    }

    @Ignore("Object lock not implemented")
    @Test
    public void cloudflareR2MultipartUploadObjectRetentionTest() throws IOException, NoSuchAlgorithmException, InterruptedException {
        new CandyS3Test().multipartUploadObjectRetentionTest(S3Provider.CLOUDFLARE_R2);
    }

    @Ignore("Object lock not implemented")
    @Test
    public void cloudflareR2CopyObjectRetentionTest() throws IOException, NoSuchAlgorithmException, InterruptedException {
        new CandyS3Test().copyObjectRetentionTest(S3Provider.CLOUDFLARE_R2);
    }

    @Ignore("Object lock not implemented")
    @Test
    public void cloudflareR2ObjectRetentionTimezoneTest() throws IOException, NoSuchAlgorithmException, InterruptedException {
        new CandyS3Test().objectRetentionTimezoneTest(S3Provider.CLOUDFLARE_R2);
    }

    @Ignore("Object lock not implemented")
    @Test
    public void cloudflareR2DeleteRetainObjectTest() throws IOException, NoSuchAlgorithmException, InterruptedException {
        new CandyS3Test().deleteRetainObjectTest(S3Provider.CLOUDFLARE_R2);
    }

    @Ignore("Object lock not implemented")
    @Test
    public void cloudflareR2DeleteRetainObjectsBatchTest() throws IOException, NoSuchAlgorithmException {
        new CandyS3Test().deleteRetainObjectsBatchTest(S3Provider.CLOUDFLARE_R2);
    }

    @Ignore("Object lock not implemented")
    @Test
    public void cloudflareR2DeleteLegalHoldObjectsBatchTest() throws IOException, NoSuchAlgorithmException {
        new CandyS3Test().deleteLegalHoldObjectsBatchTest(S3Provider.CLOUDFLARE_R2);
    }

    @Ignore("Object lock not implemented")
    @Test
    public void cloudflareR2DeleteLegalHoldObjectTest() throws IOException, NoSuchAlgorithmException, InterruptedException {
        new CandyS3Test().deleteLegalHoldObjectTest(S3Provider.CLOUDFLARE_R2);
    }

    @Ignore("Object lock not implemented")
    @Test
    public void cloudflareR2UpdateObjectLegalHoldTest() throws IOException, NoSuchAlgorithmException, InterruptedException {
        new CandyS3Test().updateObjectLegalHoldTest(S3Provider.CLOUDFLARE_R2);
    }

    @Test
    public void cloudflareR2DeleteObjectsBatchTest() throws IOException, NoSuchAlgorithmException {
        new CandyS3Test().deleteObjectsBatchTest(S3Provider.CLOUDFLARE_R2);
    }

}
