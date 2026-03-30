package io.github.matian2014.candys3;

import io.github.matian2014.candys3.exceptions.CandyS3Exception;
import io.github.matian2014.candys3.exceptions.CommonErrorCode;
import io.github.matian2014.candys3.options.*;
import okhttp3.*;
import org.apache.commons.lang3.StringUtils;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;
import org.junit.jupiter.api.condition.DisabledIf;

import java.io.*;
import java.net.URL;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.security.NoSuchAlgorithmException;
import java.text.SimpleDateFormat;
import java.util.*;
import java.util.concurrent.TimeUnit;
import java.util.logging.Level;
import java.util.logging.Logger;
import java.util.stream.Collectors;

@TestInstance(TestInstance.Lifecycle.PER_CLASS)
abstract class CandyS3Test {

    protected abstract S3Provider provider();

    // Used by bucketLocationTest/listBucketsFilterRegionTest to pick a non-default region.
    protected abstract String bucketLocationOtherRegion();

    protected abstract String listBucketsFilterOtherRegion();

    @BeforeAll
    public void setUpTempFiles() {
        CandyS3Test.mkTempFiles();
    }

    @AfterAll
    public void removeTestsBucketAndCleanUp() throws IOException {
        removeTestBuckets(provider());
        CandyS3Test.cleanUpTempFiles();
        System.out.println("removeTestsBucketAndCleanUp done.");
    }

    private static String S3_ACCESSKEY;
    private static String S3_SECRETKEY;
    private static String S3_DEFAULT_REGION;
    private static boolean S3_USE_SSL = true;

    private static String TENCENTCLOUD_COS_APPID;

    private CandyS3 init(S3Provider provider) throws IOException {
        CandyS3 candyS3 = new CandyS3(provider);

        if (S3Provider.AWS.equals(provider)) {
            Map<String, String> properties = readIni("aws.ini");
            S3_ACCESSKEY = properties.get("access-key");
            S3_SECRETKEY = properties.get("secret-key");
            S3_DEFAULT_REGION = properties.get("default-region");
        } else if (S3Provider.CLOUDFLARE_R2.equals(provider)) {
            Map<String, String> properties = readIni("cloudflare_r2.ini");
            S3_ACCESSKEY = properties.get("access-key");
            S3_SECRETKEY = properties.get("secret-key");
            S3_DEFAULT_REGION = properties.get("default-region");
            candyS3.setCloudflareR2AccountId(properties.get("account-id"));
        } else if (S3Provider.ALIYUN_OSS.equals(provider)) {
            Map<String, String> properties = readIni("aliyun_oss.ini");
            S3_DEFAULT_REGION = properties.get("default-region");
            S3_ACCESSKEY = properties.get("access-key");
            S3_SECRETKEY = properties.get("secret-key");
        } else if (S3Provider.TENCENTCLOUD_COS.equals(provider)) {
            Map<String, String> properties = readIni("tencentcloud_cos.ini");
            S3_DEFAULT_REGION = properties.get("default-region");
            S3_ACCESSKEY = properties.get("access-key");
            S3_SECRETKEY = properties.get("secret-key");
            TENCENTCLOUD_COS_APPID = properties.get("cos-app-id");
        } else if (S3Provider.CUSTOM.equals(provider)) {
            Map<String, String> properties = readIni("custom.ini");
            S3_ACCESSKEY = properties.get("access-key");
            S3_SECRETKEY = properties.get("secret-key");
            S3_DEFAULT_REGION = properties.get("default-region");
            S3_USE_SSL = Boolean.parseBoolean(properties.get("useSsl"));
            candyS3.setCustomProviderDomain(properties.get("custom-provider-domain"));
            candyS3.setCustomProviderUsePathStyle(Boolean.parseBoolean(properties.get("custom-provider-use-path-style")));
        }

        candyS3.setAccessKey(S3_ACCESSKEY);
        candyS3.setSecretKey(S3_SECRETKEY);
        candyS3.setRegion(S3_DEFAULT_REGION);
        candyS3.setUseSSL(S3_USE_SSL);
        return candyS3;
    }

    private Map<String, String> readIni(String resourceFile) throws IOException {
        Map<String, String> properties = new HashMap<>();

        InputStream in = this.getClass().getClassLoader().getResourceAsStream(resourceFile);
        BufferedReader reader = new BufferedReader(new InputStreamReader(in, StandardCharsets.UTF_8));
        String line = null;
        while ((line = reader.readLine()) != null) {
            if (line.contains("=")) {
                properties.put(line.substring(0, line.indexOf("=")), line.substring(line.indexOf("=") + 1));
            } else {
                properties.put(line, "");
            }
        }

        reader.close();
        return properties;
    }

    String genTestBucketName(String base) {
        if (StringUtils.isBlank(TENCENTCLOUD_COS_APPID)) {
            return ("test-" + base + "-" + System.currentTimeMillis()).toLowerCase();
        } else {
            return ("test-" + base + "-" + System.currentTimeMillis()).toLowerCase() + "-" + TENCENTCLOUD_COS_APPID;
        }
    }

    @Test
    @Tag(TestTag.basic)
    void createBucketTest() throws IOException {
        CandyS3 candyS3 = init(provider());
        String createdBucket = genTestBucketName("createBucketTest");
        candyS3.createBucket(new CreateBucketOptions.CreateBucketOptionsBuilder(createdBucket).build());
        List<Bucket> buckets = candyS3.listBucket(new ListBucketOptions()).getResults();
        Assertions.assertTrue(buckets.stream()
                .anyMatch(bucket -> bucket.getName().equals(createdBucket)));

        candyS3.deleteBucket(createdBucket);
        buckets = candyS3.listBucket(new ListBucketOptions()).getResults();
        Assertions.assertFalse(buckets.stream()
                .anyMatch(bucket -> bucket.getName().equals(createdBucket)));
    }

    @Test
    @Tag(TestTag.basic)
    void createBucketErrorTest() throws IOException {
        CandyS3 candyS3 = init(provider());
        String existsBucket = "aaa"; // a bucket name has existed
        String createdBucket = genTestBucketName("createBucketErrorTest");
        try {
            // Tencent cloud COS: Bucket format should be <bucketname>-<appid>
            if (!S3Provider.TENCENTCLOUD_COS.equals(provider())) {
                try {
                    candyS3.createBucket(new CreateBucketOptions.CreateBucketOptionsBuilder(existsBucket).build());
                } catch (Exception ex) {
                    Assertions.assertTrue(ex instanceof CandyS3Exception);
                    Assertions.assertEquals(((CandyS3Exception) ex).getCode(), CommonErrorCode.BUCKET_ALREADY_EXISTS.getCode());
                    Assertions.assertEquals(((CandyS3Exception) ex).getParsedError().getCode(), "BucketAlreadyExists");
                }
            }

            candyS3.createBucket(new CreateBucketOptions.CreateBucketOptionsBuilder(createdBucket).build());

            try {
                candyS3.createBucket(new CreateBucketOptions.CreateBucketOptionsBuilder(createdBucket).build());
            } catch (Exception ex) {
                Assertions.assertTrue(ex instanceof CandyS3Exception);
                Assertions.assertEquals(((CandyS3Exception) ex).getCode(), CommonErrorCode.BUCKET_ALREADY_EXISTS.getCode());
                Assertions.assertEquals(((CandyS3Exception) ex).getParsedError().getCode(), "BucketAlreadyOwnedByYou");
            }
        } finally {
        }
    }

    @Test
    @Tag(TestTag.basic)
    void listBucketsTest() throws IOException {
        CandyS3 candyS3 = init(provider());
        String createdBucket = genTestBucketName("listBucketsTest");
        try {
            List<Bucket> buckets = candyS3.listBucket(new ListBucketOptions()).getResults();
            int initialCnt = buckets.size();

            candyS3.createBucket(new CreateBucketOptions.CreateBucketOptionsBuilder(createdBucket).build());
            buckets = candyS3.listBucket(new ListBucketOptions()).getResults();
            Assertions.assertEquals(buckets.size(), initialCnt + 1);
        } finally {
            candyS3.deleteBucket(createdBucket);
        }
    }

    @Test
    @Tag(TestTag.bucketRegion)
    void listBucketsFilterRegionTest() throws IOException {
        CandyS3 candyS3 = init(provider());
        String bucket = genTestBucketName("listBucketsFilterRegionTest");
        String createRegion = listBucketsFilterOtherRegion();
        try {
            String listRegion = candyS3.region; // default to 'us-east-1'
            // Ensure we use different region here.
            Assertions.assertNotEquals(createRegion, listRegion);

            candyS3.createBucket(new CreateBucketOptions.CreateBucketOptionsBuilder(bucket)
                    .locationConstraint(createRegion).build());

            candyS3.setRegion(listRegion);
            List<Bucket> allRegionBuckets = candyS3.listBucket(new ListBucketOptions()).getResults();
            Assertions.assertTrue(allRegionBuckets.stream()
                    .anyMatch(b -> b.getName().equals(bucket)));
            List<Bucket> filterRegionNoneMatchBuckets = candyS3.listBucket(new ListBucketOptions().filterBucketRegion(true)).getResults();
            Assertions.assertTrue(filterRegionNoneMatchBuckets.stream()
                    .noneMatch(b -> b.getName().equals(bucket)));

            candyS3.setRegion(createRegion);
            List<Bucket> filterRegionMatchBuckets = candyS3.listBucket(new ListBucketOptions().filterBucketRegion(true)).getResults();
            Assertions.assertTrue(filterRegionMatchBuckets.stream()
                    .anyMatch(b -> b.getName().equals(bucket)));
        } finally {
            candyS3.setRegion(createRegion);
            candyS3.deleteBucket(bucket);
        }
    }

    @Test
    @Tag(TestTag.basic)
    void listBucketsPrefixTest() throws IOException {
        CandyS3 candyS3 = init(provider());

        // Tencent cloud COS: Bucket format should be <bucketname>-<appid>
        String bucket1 = genTestBucketName("lsBPrefix--1");
        String bucket2 = genTestBucketName("lsBPrefix--2");

        // bucket name should use lowercase letters
        String prefix = bucket1.substring(0, bucket1.indexOf("--1".toLowerCase()));
        Assertions.assertTrue(prefix.endsWith("lsBPrefix".toLowerCase()));
        Assertions.assertTrue(bucket1.startsWith(prefix));
        Assertions.assertTrue(bucket2.startsWith(prefix));

        try {
            candyS3.createBucket(new CreateBucketOptions.CreateBucketOptionsBuilder(bucket1).build());
            candyS3.createBucket(new CreateBucketOptions.CreateBucketOptionsBuilder(bucket2).build());

            List<Bucket> bucketsList1 = candyS3.listBucket(new ListBucketOptions().prefix(prefix)).getResults();
            Assertions.assertEquals(2, bucketsList1.size());
            Assertions.assertTrue(bucketsList1.stream()
                    .anyMatch(b -> b.getName().equals(bucket1)));
            Assertions.assertTrue(bucketsList1.stream()
                    .anyMatch(b -> b.getName().equals(bucket2)));

            List<Bucket> bucketsList2 = candyS3.listBucket(new ListBucketOptions().prefix(prefix + "--1")).getResults();
            Assertions.assertEquals(1, bucketsList2.size());
            Assertions.assertTrue(bucketsList2.stream()
                    .anyMatch(b -> b.getName().equals(bucket1)));
            Assertions.assertTrue(bucketsList2.stream()
                    .noneMatch(b -> b.getName().equals(bucket2)));

        } finally {
            candyS3.deleteBucket(bucket1);
            candyS3.deleteBucket(bucket2);
        }
    }

    @Test
    @Tag(TestTag.basic)
    void listBucketsPaginationTest() throws IOException {
        CandyS3 candyS3 = init(provider());
        List<String> createdBuckets = new ArrayList<>();
        try {
            int oldCount = candyS3.listBucket(new ListBucketOptions()).getResults().size();

            for (int i = 0; i < 10; i++) {
                String bucket = genTestBucketName("listBucketsPaginationTest" + i);
                createdBuckets.add(bucket);
                candyS3.createBucket(new CreateBucketOptions.CreateBucketOptionsBuilder(bucket).build());
                try {
                    Thread.sleep(200);
                } catch (InterruptedException e) {
                    throw new RuntimeException(e);
                }
            }

            int newCount = candyS3.listBucket(new ListBucketOptions()).getResults().size();
            Assertions.assertEquals(newCount, oldCount + 10);

            List<Bucket> listBuckets = new ArrayList<>();
            ListPaginationResult<Bucket> result;
            ListBucketOptions listBucketOptions = new ListBucketOptions().maxBuckets(3);

            int loopTime = 0;
            int lastCount = 0;
            do {
                result = candyS3.listBucket(listBucketOptions);
                listBuckets.addAll(result.getResults());
                listBucketOptions.continuationToken(result.getNextPaginationMarker());

                loopTime++;
                if (loopTime * 3 < newCount) {
                    Assertions.assertTrue(listBuckets.size() > lastCount);
                    Assertions.assertTrue(StringUtils.isNotEmpty(result.getNextPaginationMarker()));
                }
                lastCount = listBuckets.size();
            } while (StringUtils.isNotEmpty(result.getNextPaginationMarker()));

            int exceptedLoopTime = newCount % 3 == 0 ? newCount / 3 : newCount / 3 + 1;
            Assertions.assertEquals(newCount, listBuckets.size());
            Assertions.assertEquals(exceptedLoopTime, loopTime);
            Assertions.assertTrue(StringUtils.isEmpty(listBucketOptions.getContinuationToken()));
        } finally {
            for (int i = 0, len = createdBuckets.size(); i < len; i++) {
                try {
                    candyS3.deleteBucket(createdBuckets.get(i));
                    Thread.sleep(100);
                } catch (Exception ex) {
                }
            }
        }
    }

    @Test
    @Tag(TestTag.basic)
    void bucketExistsTest() throws IOException {
        CandyS3 candyS3 = init(provider());
        String bucket = genTestBucketName("bucketExistsTest");
        try {
            Assertions.assertFalse(candyS3.bucketExists(bucket));
            candyS3.createBucket(new CreateBucketOptions.CreateBucketOptionsBuilder(bucket).build());
            Assertions.assertTrue(candyS3.bucketExists(bucket));
            candyS3.deleteBucket(bucket);
            Assertions.assertFalse(candyS3.bucketExists(bucket));
        } finally {
        }
    }

    @Test
    @Tag(TestTag.versioning)
    void bucketVersioningTest() throws IOException {
        CandyS3 candyS3 = init(provider());
        String bucket = genTestBucketName("bucketVersioningTest");
        try {
            candyS3.createBucket(new CreateBucketOptions.CreateBucketOptionsBuilder(bucket).build());
            Assertions.assertFalse(candyS3.isBucketVersioning(bucket));
            candyS3.setBucketVersioning(bucket, true);
            Assertions.assertTrue(candyS3.isBucketVersioning(bucket));
        } finally {
            candyS3.deleteBucket(bucket);
        }
    }

    @Test
    @Tag(TestTag.bucketRegion)
    void bucketAccelerateTest() throws IOException {
        CandyS3 candyS3 = init(provider());
        String bucket = genTestBucketName("bucketAccelerateTest");
        try {
            candyS3.createBucket(new CreateBucketOptions.CreateBucketOptionsBuilder(bucket).build());
            Assertions.assertFalse(candyS3.isBucketAccelerated(bucket));
            candyS3.setBucketAccelerate(bucket, true);
            Assertions.assertTrue(candyS3.isBucketAccelerated(bucket));
        } finally {
            candyS3.deleteBucket(bucket);
        }
    }

    @Test
    @Tag(TestTag.bucketRegion)
    void bucketLocationTest() throws IOException {
        CandyS3 candyS3 = init(provider());
        String bucket1 = genTestBucketName("bucketLocationTest1");
        String bucket2 = genTestBucketName("bucketLocationTest2");
        String region = bucketLocationOtherRegion();
        String s3DefaultRegion = candyS3.region;
        try {
            candyS3.region = region;
            candyS3.createBucket(new CreateBucketOptions.CreateBucketOptionsBuilder(bucket1).locationConstraint(region).build());
            Assertions.assertEquals(region, candyS3.getBucketLocation(bucket1));

            // Buckets in Default Region have a LocationConstraint of null.
            candyS3.region = s3DefaultRegion;
            candyS3.createBucket(new CreateBucketOptions.CreateBucketOptionsBuilder(bucket2).build());
            if (S3Provider.AWS.equals(provider())) {
                Assertions.assertTrue(StringUtils.isEmpty(candyS3.getBucketLocation(bucket2)));
            } else {
                Assertions.assertEquals(s3DefaultRegion, candyS3.getBucketLocation(bucket2));
            }
        } finally {
            candyS3.region = region;
            candyS3.deleteBucket(bucket1);
            candyS3.region = s3DefaultRegion;
            candyS3.deleteBucket(bucket2);
        }
    }

    @Test
    @Tag(TestTag.objectLock)
    @Tag(TestTag.objectRetention)
    void bucketObjectLockConfigurationTest() throws IOException, NoSuchAlgorithmException {
        CandyS3 candyS3 = init(provider());
        String bucket1 = genTestBucketName("bOLTest1");
        String bucket2 = genTestBucketName("bOLTest2");
        try {
            // Update object lock configuration when create bucket with object-lock enabled.
            candyS3.createBucket(new CreateBucketOptions.CreateBucketOptionsBuilder(bucket1).enableObjectLock().build());
            BucketObjectLockConfiguration lockConf = candyS3.getBucketObjectLockConfiguration(bucket1);
            Assertions.assertTrue(lockConf.isObjectLockEnabled());

            // Enable object retention
            candyS3.enableBucketObjectLock(bucket1, new UpdateBucketObjectLockOptions.UpdateBucketObjectLockOptionsBuilder()
                    .retentionDays(ObjectRetentionMode.GOVERNANCE, 1)
                    .build());
            lockConf = candyS3.getBucketObjectLockConfiguration(bucket1);
            Assertions.assertTrue(lockConf.isObjectLockEnabled());
            Assertions.assertEquals(ObjectRetentionMode.GOVERNANCE, lockConf.getMode());
            Assertions.assertEquals(1, (int) lockConf.getDays());
            Assertions.assertNull(lockConf.getYears());

            // Disable object retention
            candyS3.enableBucketObjectLock(bucket1, new UpdateBucketObjectLockOptions.UpdateBucketObjectLockOptionsBuilder()
                    .buildWithoutRetention());
            lockConf = candyS3.getBucketObjectLockConfiguration(bucket1);
            Assertions.assertTrue(lockConf.isObjectLockEnabled());
            Assertions.assertNull(lockConf.getMode());
            Assertions.assertNull(lockConf.getDays());
            Assertions.assertNull(lockConf.getYears());

            // Enable and update object lock configuration when create bucket with object-lock disabled.
            candyS3.createBucket(new CreateBucketOptions.CreateBucketOptionsBuilder(bucket2).build());
            try {
                candyS3.getBucketObjectLockConfiguration(bucket2);
                Assertions.fail("Should not be here. Exception should be thrown when get configuration if object-lock is disabled");
            } catch (Exception ex) {
                Assertions.assertTrue(ex instanceof CandyS3Exception);
                Assertions.assertEquals(((CandyS3Exception) ex).getCode(), CommonErrorCode.BUCKET_OBJECT_LOCK_NOT_ENABLED.getCode());
                Assertions.assertEquals(((CandyS3Exception) ex).getParsedError().getCode(), "ObjectLockConfigurationNotFoundError");
            }

            // Must enable versioning when use object-lock
            candyS3.setBucketVersioning(bucket2, true);
            candyS3.enableBucketObjectLock(bucket2, new UpdateBucketObjectLockOptions.UpdateBucketObjectLockOptionsBuilder()
                    .retentionYears(ObjectRetentionMode.COMPLIANCE, 2)
                    .build());
            BucketObjectLockConfiguration lockConf2 = candyS3.getBucketObjectLockConfiguration(bucket2);
            Assertions.assertTrue(lockConf2.isObjectLockEnabled());
            Assertions.assertEquals(ObjectRetentionMode.COMPLIANCE, lockConf2.getMode());
            Assertions.assertEquals(2, (int) lockConf2.getYears());
            Assertions.assertNull(lockConf2.getDays());
        } finally {
            candyS3.deleteBucket(bucket1);
            candyS3.deleteBucket(bucket2);
        }
    }

    @Test
    @Tag(TestTag.basic)
    void bucketPolicyTest() throws IOException, NoSuchAlgorithmException {
        CandyS3 candyS3 = init(provider());
        String bucket = genTestBucketName("bucketPolicyTest");
        try {
            candyS3.createBucket(new CreateBucketOptions.CreateBucketOptionsBuilder(bucket).build());

            try {
                candyS3.getBucketPolicy(bucket);
                Assertions.fail("Should not be here. Exception should be thrown when get policy if no policy exists");
            } catch (Exception ex) {
                Assertions.assertTrue(ex instanceof CandyS3Exception);
                Assertions.assertEquals(CommonErrorCode.BUCKET_NO_POLICY.getCode(), ((CandyS3Exception) ex).getCode());
            }

            candyS3.updateBucketPolicy(bucket, new UpdateBucketPolicyOptions().updatePolicy("{\n" +
                    "\"Version\": \"2012-10-17\",\n" +
                    "\"Id\": \"PutObjPolicy\",\n" +
                    "\"Statement\": [{\n" +
                    "  \"Sid\": \"DenyObjectsThatAreNotSSEKMS\",\n" +
                    "  \"Principal\": \"*\",\n" +
                    "  \"Effect\": \"Deny\",\n" +
                    "  \"Action\": \"s3:PutObject\",\n" +
                    "  \"Resource\": \"arn:aws:s3:::" + bucket + "/*\",\n" +
                    "  \"Condition\": {\n" +
                    "    \"Null\": {\n" +
                    "      \"s3:x-amz-server-side-encryption-aws-kms-key-id\": \"true\"\n" +
                    "    }\n" +
                    "  }\n" +
                    "}]\n" +
                    "}"));
            Assertions.assertNotNull(candyS3.getBucketPolicy(bucket));
            Assertions.assertFalse(candyS3.getBucketPolicyStatus(bucket).isPublic());

            // Must disable blockPublicPolicy in public access block configuration before put a public policy
            candyS3.updateBucketPublicAccessBlock(bucket,
                    new UpdateBucketPublicAccessBlockOptions.UpdateBucketPublicAccessBlockOptionsBuilder()
                            .blockPublicPolicy(false).build());
            candyS3.updateBucketPolicy(bucket, new UpdateBucketPolicyOptions().updatePolicy("{\n" +
                    "\"Version\": \"2012-10-17\",\n" +
                    "\"Id\": \"GetObjectPolicy\",\n" +
                    "\"Statement\": [{\n" +
                    "  \"Principal\": \"*\", \n" +
                    "  \"Resource\": \"arn:aws:s3:::" + bucket + "/*\", \n" +
                    "  \"Action\": \"s3:GetObject\", \n" +
                    "  \"Effect\": \"Allow\" \n" +
                    "}]\n" +
                    "}"));
            Assertions.assertNotNull(candyS3.getBucketPolicy(bucket));
            Assertions.assertTrue(candyS3.getBucketPolicyStatus(bucket).isPublic());

            candyS3.updateBucketPolicy(bucket, new UpdateBucketPolicyOptions().removePolicy());
            try {
                candyS3.getBucketPolicy(bucket);
                Assertions.fail("Should not be here. Exception should be thrown when get policy if no policy exists");
            } catch (Exception ex) {
                Assertions.assertTrue(ex instanceof CandyS3Exception);
                Assertions.assertEquals(CommonErrorCode.BUCKET_NO_POLICY.getCode(), ((CandyS3Exception) ex).getCode());
            }
        } finally {
            candyS3.deleteBucket(bucket);
        }
    }

    @Test
    @Tag(TestTag.accessBlock)
    void bucketBlockPublicAccessTest() throws IOException {
        CandyS3 candyS3 = init(provider());
        String bucket = genTestBucketName("bucketBlockPublicAccessTest");
        try {
            candyS3.createBucket(new CreateBucketOptions.CreateBucketOptionsBuilder(bucket).build());

            BucketPublicAccessBlock block = candyS3.getBucketPublicAccessBlock(bucket);
            Assertions.assertTrue(block.isBlockPublicAcls());
            Assertions.assertTrue(block.isBlockPublicPolicy());
            Assertions.assertTrue(block.isIgnorePublicAcls());
            Assertions.assertTrue(block.isRestrictPublicBuckets());

            candyS3.updateBucketPublicAccessBlock(bucket,
                    new UpdateBucketPublicAccessBlockOptions.UpdateBucketPublicAccessBlockOptionsBuilder()
                            .removeAccessBlocks()
                            .build());
            try {
                candyS3.getBucketPublicAccessBlock(bucket);
                Assertions.fail("Should not be here. Exception should be thrown when get public access block if no public access block configuration exists");
            } catch (Exception ex) {
                Assertions.assertTrue(ex instanceof CandyS3Exception);
                Assertions.assertEquals(CommonErrorCode.BUCKET_NO_PUBLIC_ACCESS_BLOCK.getCode(), ((CandyS3Exception) ex).getCode());
                Assertions.assertEquals("NoSuchPublicAccessBlockConfiguration", ((CandyS3Exception) ex).getParsedError().getCode());
            }

            candyS3.updateBucketPublicAccessBlock(bucket, new UpdateBucketPublicAccessBlockOptions.UpdateBucketPublicAccessBlockOptionsBuilder()
                    .blockPublicAcls(true)
                    .blockPublicPolicy(false)
                    .ignorePublicAcls(true)
                    .restrictPublicBuckets(false)
                    .build());
            BucketPublicAccessBlock block2 = candyS3.getBucketPublicAccessBlock(bucket);
            Assertions.assertTrue(block2.isBlockPublicAcls());
            Assertions.assertFalse(block2.isBlockPublicPolicy());
            Assertions.assertTrue(block2.isIgnorePublicAcls());
            Assertions.assertFalse(block2.isRestrictPublicBuckets());
        } finally {
            candyS3.deleteBucket(bucket);
        }
    }

    @Test
    @Tag(TestTag.tagging)
    void bucketTagTest() throws IOException, NoSuchAlgorithmException {
        CandyS3 candyS3 = init(provider());
        String bucket = genTestBucketName("bucketTagTest");
        try {
            candyS3.createBucket(new CreateBucketOptions.CreateBucketOptionsBuilder(bucket).build());

            try {
                candyS3.getBucketTag(bucket);
            } catch (Exception ex) {
                Assertions.assertTrue(ex instanceof CandyS3Exception);
                Assertions.assertEquals(CommonErrorCode.BUCKET_NO_ASSOCIATED_TAG.getCode(), ((CandyS3Exception) ex).getCode());
                Assertions.assertEquals("NoSuchTagSet", ((CandyS3Exception) ex).getParsedError().getCode());
            }

            candyS3.updateBucketTag(bucket, new UpdateBucketTagOptions().addTag("a", "b").addTag("b", null));
            Map<String, String> tags = candyS3.getBucketTag(bucket);
            Assertions.assertEquals(tags.size(), 2);
            Assertions.assertEquals(tags.get("a"), "b");
            Assertions.assertTrue(StringUtils.isEmpty(tags.get("b")));

            candyS3.updateBucketTag(bucket, new UpdateBucketTagOptions().removeTags());
            try {
                candyS3.getBucketTag(bucket);
            } catch (Exception ex) {
                Assertions.assertTrue(ex instanceof CandyS3Exception);
                Assertions.assertEquals(CommonErrorCode.BUCKET_NO_ASSOCIATED_TAG.getCode(), ((CandyS3Exception) ex).getCode());
                Assertions.assertEquals("NoSuchTagSet", ((CandyS3Exception) ex).getParsedError().getCode());
            }
        } finally {
            candyS3.deleteBucket(bucket);
        }
    }

    @Test
    @Tag(TestTag.sse)
    void bucketSSEConfigurationTest() throws IOException, NoSuchAlgorithmException {
        CandyS3 candyS3 = init(provider());
        String bucket = genTestBucketName("bucketSSEConfigurationTest");
        try {
            candyS3.createBucket(new CreateBucketOptions.CreateBucketOptionsBuilder(bucket).build());

            // Tencent cloud COS does not enable SSE when create bucket by default
            if (!S3Provider.TENCENTCLOUD_COS.equals(provider())) {
                List<ServerSideEncryptionProperties> configurations = candyS3.getBucketServerSideEncryption(bucket);
                Assertions.assertEquals(configurations.get(0).getSseAlgorithm(), ServerSideEncryptionAlgorithm.AES256.getAlgorithm());
            }

            // Tencent cloud COS: BucketKeyEnabled is not applicable if the sse algorithm is not KMS or SM4
            if (S3Provider.TENCENTCLOUD_COS.equals(provider())) {
                candyS3.updateBucketServerSideEncryption(bucket, new UpdateServerSideEncryptionOptions.UpdateServerSideEncryptionOptionsBuilder()
                        .sseAlgorithm("SM4")
                        .build());
                List<ServerSideEncryptionProperties> configurations2 = candyS3.getBucketServerSideEncryption(bucket);
                Assertions.assertEquals(configurations2.get(0).getSseAlgorithm(), "SM4");
            } else {
                candyS3.updateBucketServerSideEncryption(bucket, new UpdateServerSideEncryptionOptions.UpdateServerSideEncryptionOptionsBuilder()
                        .sseAlgorithm(ServerSideEncryptionAlgorithm.AES256)
                        .build());
                List<ServerSideEncryptionProperties> configurations2 = candyS3.getBucketServerSideEncryption(bucket);
                Assertions.assertEquals(configurations2.get(0).getSseAlgorithm(), ServerSideEncryptionAlgorithm.AES256.getAlgorithm());
            }

        } finally {
            candyS3.deleteBucket(bucket);
        }
    }

    @Test
    @Tag(TestTag.basic)
    void listObjectsPaginationTest() throws IOException, NoSuchAlgorithmException {
        CandyS3 candyS3 = init(provider());
        String bucket = genTestBucketName("listObjectsPaginationTest");
        List<String> createdObjectKeys = new ArrayList<>();
        try {
            candyS3.createBucket(new CreateBucketOptions.CreateBucketOptionsBuilder(bucket).build());
            for (int i = 0; i < 10; i++) {
                String objectKey = "objectKey" + i;
                createdObjectKeys.add(objectKey);
                candyS3.putObject(bucket, objectKey, new PutObjectOptions.PutObjectOptionsBuilder().build());
            }

            int newCount = candyS3.listObjects(bucket, new ListObjectOptions()).getResults().size();
            Assertions.assertEquals(newCount, 10);

            List<S3Object> listObjects = new ArrayList<>();
            ListPaginationResult<S3Object> result;
            ListObjectOptions listObjectOptions = new ListObjectOptions().maxKeys(3);

            int loopTime = 0;
            int lastCount = 0;
            do {
                result = candyS3.listObjects(bucket, listObjectOptions);
                listObjects.addAll(result.getResults());
                listObjectOptions.continuationToken(result.getNextPaginationMarker());

                loopTime++;
                if (loopTime * 3 < newCount) {
                    Assertions.assertTrue(listObjects.size() > lastCount);
                    Assertions.assertTrue(StringUtils.isNotEmpty(result.getNextPaginationMarker()));
                }
                lastCount = listObjects.size();
            } while (StringUtils.isNotEmpty(result.getNextPaginationMarker()));

            int exceptedLoopTime = newCount % 3 == 0 ? newCount / 3 : newCount / 3 + 1;
            Assertions.assertEquals(exceptedLoopTime, loopTime);
            Assertions.assertEquals(newCount, listObjects.size());
            Assertions.assertTrue(StringUtils.isEmpty(listObjectOptions.getContinuationToken()));
        } finally {
            candyS3.deleteObjectsBatch(bucket, new DeleteObjectsBatchOptions.DeleteObjectsBatchOptionsBuilder()
                    .addDeleteObjects(createdObjectKeys)
                    .build());
            candyS3.deleteBucket(bucket);
        }
    }

    @Test
    @Tag(TestTag.basic)
    void listObjectsCommonPrefixTest() throws IOException, NoSuchAlgorithmException {
        CandyS3 candyS3 = init(provider());
        String bucket = genTestBucketName("listObjectsCommonPrefixTest");
        try {
            candyS3.createBucket(new CreateBucketOptions.CreateBucketOptionsBuilder(bucket).build());

            candyS3.putObject(bucket, "sample.jpg",
                    new PutObjectOptions.PutObjectOptionsBuilder().build());
            candyS3.putObject(bucket, "photos/2006/January/sample.jpg",
                    new PutObjectOptions.PutObjectOptionsBuilder().build());
            candyS3.putObject(bucket, "photos/2006/February/sample2.jpg",
                    new PutObjectOptions.PutObjectOptionsBuilder().build());
            candyS3.putObject(bucket, "photos/2006/February/sample3.jpg",
                    new PutObjectOptions.PutObjectOptionsBuilder().build());
            candyS3.putObject(bucket, "photos/2006/February/sample4.jpg",
                    new PutObjectOptions.PutObjectOptionsBuilder().build());

            ListPaginationResult<S3Object> objects = candyS3.listObjects(bucket, new ListObjectOptions().delimiter('/'));
            Assertions.assertEquals(objects.getResults().size(), 1);
            Assertions.assertEquals(objects.getResults().get(0).getKey(), "sample.jpg");
            Assertions.assertEquals(objects.getCommonPrefixes().size(), 1);
            Assertions.assertEquals(objects.getCommonPrefixes().get(0).getPrefix(), "photos/");

            objects = candyS3.listObjects(bucket, new ListObjectOptions().prefix("photos/2006/").delimiter('/'));
            Assertions.assertEquals(objects.getResults().size(), 0);
            Assertions.assertEquals(objects.getCommonPrefixes().size(), 2);
            Assertions.assertEquals(objects.getCommonPrefixes().get(0).getPrefix(), "photos/2006/February/");
            Assertions.assertEquals(objects.getCommonPrefixes().get(1).getPrefix(), "photos/2006/January/");

            objects = candyS3.listObjects(bucket, new ListObjectOptions().prefix("photos/2006/February/").delimiter('/'));
            Assertions.assertEquals(objects.getResults().size(), 3);
            Assertions.assertEquals(objects.getCommonPrefixes().size(), 0);

        } finally {
            candyS3.deleteObjectsBatch(bucket, new DeleteObjectsBatchOptions.DeleteObjectsBatchOptionsBuilder()
                    .addDeleteObject("sample.jpg")
                    .addDeleteObject("photos/2006/January/sample.jpg")
                    .addDeleteObject("photos/2006/February/sample2.jpg")
                    .addDeleteObject("photos/2006/February/sample3.jpg")
                    .addDeleteObject("photos/2006/February/sample4.jpg")
                    .build());
            candyS3.deleteBucket(bucket);
        }
    }

    @Test
    @Tag(TestTag.versioning)
    void listObjectVersionsWithoutVersioningTest() throws IOException, NoSuchAlgorithmException {
        CandyS3 candyS3 = init(provider());
        String bucketWithoutVersioning = genTestBucketName("lOVWithoutV");
        String objectKey = "listObjectVersionsWithoutVersioningTest.data";

        try {
            // put object in bucket without versioning, object versionId will be null
            candyS3.createBucket(new CreateBucketOptions.CreateBucketOptionsBuilder(bucketWithoutVersioning).build());
            Assertions.assertFalse(candyS3.isBucketVersioning(bucketWithoutVersioning));

            candyS3.putObject(bucketWithoutVersioning, objectKey, new PutObjectOptions.PutObjectOptionsBuilder().build());
            candyS3.putObject(bucketWithoutVersioning, objectKey, new PutObjectOptions.PutObjectOptionsBuilder()
                    .configureUploadData().withData(new byte[]{1}).endConfigureDataContent()
                    .build());
            ListPaginationResult<S3ObjectVersion> result =
                    candyS3.listObjectVersions(bucketWithoutVersioning, new ListObjectVersionsOptions());
            Assertions.assertEquals(result.getResults().size(), 1);
            if (S3Provider.TENCENTCLOUD_COS.equals(provider())) {
                Assertions.assertEquals(result.getResults().get(0).getVersionId(), "");
            } else {
                Assertions.assertEquals(result.getResults().get(0).getVersionId(), "null");
            }
        } finally {
            candyS3.deleteObject(bucketWithoutVersioning, new DeleteObjectOptions(objectKey));
            candyS3.deleteBucket(bucketWithoutVersioning);
        }
    }

    @Test
    @Tag(TestTag.versioning)
    void listObjectVersionsTest() throws IOException, NoSuchAlgorithmException {
        CandyS3 candyS3 = init(provider());
        String bucket = genTestBucketName("listObjectVersionsTest2");
        String objectKey1 = "listObjectVersionsTest1.data";
        String objectKey2 = "listObjectVersionsTest2.data";

        try {
            // Create bucket and enable versioning
            candyS3.createBucket(new CreateBucketOptions.CreateBucketOptionsBuilder(bucket).build());
            candyS3.setBucketVersioning(bucket, true);
            Assertions.assertTrue(candyS3.isBucketVersioning(bucket));

            candyS3.putObject(bucket, objectKey1, new PutObjectOptions.PutObjectOptionsBuilder().build());
            candyS3.putObject(bucket, objectKey1, new PutObjectOptions.PutObjectOptionsBuilder()
                    .configureUploadData().withData(new byte[]{1}).endConfigureDataContent()
                    .build());
            candyS3.putObject(bucket, objectKey1, new PutObjectOptions.PutObjectOptionsBuilder()
                    .configureUploadData().withData(new byte[]{11, 12}).endConfigureDataContent()
                    .build());

            candyS3.putObject(bucket, objectKey2, new PutObjectOptions.PutObjectOptionsBuilder().build());
            candyS3.putObject(bucket, objectKey2, new PutObjectOptions.PutObjectOptionsBuilder()
                    .configureUploadData().withData(new byte[]{21}).endConfigureDataContent()
                    .build());

            ListPaginationResult<S3ObjectVersion> result =
                    candyS3.listObjectVersions(bucket, new ListObjectVersionsOptions());
            Assertions.assertEquals(result.getResults().size(), 5);

            Map<String, List<S3ObjectVersion>> s3ObjectVersionsByKey = result.getResults()
                    .stream()
                    .collect(Collectors.groupingBy(S3ObjectVersion::getKey));
            Assertions.assertTrue(s3ObjectVersionsByKey.containsKey(objectKey1));
            Assertions.assertTrue(s3ObjectVersionsByKey.containsKey(objectKey2));
            Assertions.assertEquals(s3ObjectVersionsByKey.size(), 2);

            {
                Assertions.assertEquals(s3ObjectVersionsByKey.get(objectKey1).size(), 3);
                // every object has only one version is latest
                int objectIsLatestVersionCnt = 0;
                int objectNotLatestVersionCnt = 0;
                for (S3ObjectVersion object1Version : s3ObjectVersionsByKey.get(objectKey1)) {
                    if (object1Version.getIsLatest() != null) {
                        if (object1Version.getIsLatest()) {
                            objectIsLatestVersionCnt++;
                            Assertions.assertEquals(object1Version.getSize(), 2);
                        } else {
                            objectNotLatestVersionCnt++;
                        }
                    }
                }
                Assertions.assertEquals(objectIsLatestVersionCnt, 1);
                Assertions.assertEquals(objectNotLatestVersionCnt, 2);
            }

            {
                Assertions.assertEquals(s3ObjectVersionsByKey.get(objectKey2).size(), 2);
                // every object has only one version is latest
                int objectIsLatestVersionCnt = 0;
                int objectNotLatestVersionCnt = 0;
                for (S3ObjectVersion object1Version : s3ObjectVersionsByKey.get(objectKey2)) {
                    if (object1Version.getIsLatest() != null) {
                        if (object1Version.getIsLatest()) {
                            objectIsLatestVersionCnt++;
                            Assertions.assertEquals(object1Version.getSize(), 1);
                        } else {
                            objectNotLatestVersionCnt++;
                        }
                    }
                }
                Assertions.assertEquals(objectIsLatestVersionCnt, 1);
                Assertions.assertEquals(objectNotLatestVersionCnt, 1);
            }

        } finally {
            deleteAllObjectVersions(provider(), bucket);
            candyS3.deleteBucket(bucket);
        }
    }

    @Test
    @Tag(TestTag.versioning)
    void listObjectVersionsPaginationTest() throws IOException, NoSuchAlgorithmException, InterruptedException {
        CandyS3 candyS3 = init(provider());
        String bucket = genTestBucketName("lOVPTest");
        String objectKey1 = "listObjectVersionsPaginationTest1.data";
        String objectKey2 = "listObjectVersionsPaginationTest2.data";

        try {
            // Create bucket and enable versioning
            candyS3.createBucket(new CreateBucketOptions.CreateBucketOptionsBuilder(bucket).build());
            candyS3.setBucketVersioning(bucket, true);
            Assertions.assertTrue(candyS3.isBucketVersioning(bucket));

            candyS3.putObject(bucket, objectKey1, new PutObjectOptions.PutObjectOptionsBuilder().build());
            candyS3.putObject(bucket, objectKey1, new PutObjectOptions.PutObjectOptionsBuilder()
                    .configureUploadData().withData(new byte[]{1}).endConfigureDataContent()
                    .build());
            candyS3.putObject(bucket, objectKey1, new PutObjectOptions.PutObjectOptionsBuilder()
                    .configureUploadData().withData(new byte[]{11, 12}).endConfigureDataContent()
                    .build());

            candyS3.putObject(bucket, objectKey2, new PutObjectOptions.PutObjectOptionsBuilder().build());
            candyS3.putObject(bucket, objectKey2, new PutObjectOptions.PutObjectOptionsBuilder()
                    .configureUploadData().withData(new byte[]{21}).endConfigureDataContent()
                    .build());

            List<S3ObjectVersion> listObjectVersions = new ArrayList<>();
            ListPaginationResult<S3ObjectVersion> result;
            ListObjectVersionsOptions listObjectVersionsOptions = new ListObjectVersionsOptions().maxKeys(2);

            int objectVersionsCount = 5;

            int loopTime = 0;
            int lastCount = 0;
            do {
                result = candyS3.listObjectVersions(bucket, listObjectVersionsOptions);
                listObjectVersions.addAll(result.getResults());
                listObjectVersionsOptions.keyMarker(result.getNextPaginationMarker());
                listObjectVersionsOptions.versionIdMarker(result.getNextPaginationMarker2());

                loopTime++;
                if (loopTime * 2 < objectVersionsCount) {
                    Assertions.assertTrue(listObjectVersions.size() > lastCount);
                    Assertions.assertTrue(StringUtils.isNotEmpty(result.getNextPaginationMarker()));
                    Assertions.assertTrue(StringUtils.isNotEmpty(result.getNextPaginationMarker2()));
                }
                lastCount = listObjectVersions.size();
            } while (StringUtils.isNotEmpty(result.getNextPaginationMarker()) || StringUtils.isNotEmpty(result.getNextPaginationMarker2()));

            Assertions.assertEquals(objectVersionsCount, listObjectVersions.size());
            Assertions.assertTrue(StringUtils.isEmpty(listObjectVersionsOptions.getKeyMarker()));
            Assertions.assertTrue(StringUtils.isEmpty(listObjectVersionsOptions.getVersionIdMarker()));

        } finally {
            deleteAllObjectVersions(provider(), bucket);
            candyS3.deleteBucket(bucket);
        }
    }

    @Test
    @Tag(TestTag.versioning)
    void listObjectVersionsCommonPrefixTest() throws IOException, NoSuchAlgorithmException {
        CandyS3 candyS3 = init(provider());
        String bucket = genTestBucketName("lOVCPrefix");
        try {
            candyS3.createBucket(new CreateBucketOptions.CreateBucketOptionsBuilder(bucket).build());

            candyS3.putObject(bucket, "photos/2006/January/sample.jpg",
                    new PutObjectOptions.PutObjectOptionsBuilder().build());
            candyS3.putObject(bucket, "photos/2006/February/sample.jpg",
                    new PutObjectOptions.PutObjectOptionsBuilder().build());
            candyS3.putObject(bucket, "photos/2006/March/sample.jpg",
                    new PutObjectOptions.PutObjectOptionsBuilder().build());
            candyS3.putObject(bucket, "videos/2006/March/sample.wmv",
                    new PutObjectOptions.PutObjectOptionsBuilder().build());
            candyS3.putObject(bucket, "sample.jpg",
                    new PutObjectOptions.PutObjectOptionsBuilder().build());

            ListPaginationResult<S3ObjectVersion> objectVersions = candyS3.listObjectVersions(bucket,
                    new ListObjectVersionsOptions().delimiter('/'));
            Assertions.assertEquals(objectVersions.getResults().size(), 1);
            Assertions.assertEquals(objectVersions.getResults().get(0).getKey(), "sample.jpg");
            Assertions.assertEquals(objectVersions.getCommonPrefixes().size(), 2);
            Assertions.assertEquals(objectVersions.getCommonPrefixes().get(0).getPrefix(), "photos/");
            Assertions.assertEquals(objectVersions.getCommonPrefixes().get(1).getPrefix(), "videos/");

            objectVersions = candyS3.listObjectVersions(bucket,
                    new ListObjectVersionsOptions().prefix("photos/2006/").delimiter('/'));
            Assertions.assertEquals(objectVersions.getResults().size(), 0);
            Assertions.assertEquals(objectVersions.getCommonPrefixes().size(), 3);
            Assertions.assertEquals(objectVersions.getCommonPrefixes().get(0).getPrefix(), "photos/2006/February/");
            Assertions.assertEquals(objectVersions.getCommonPrefixes().get(1).getPrefix(), "photos/2006/January/");
            Assertions.assertEquals(objectVersions.getCommonPrefixes().get(2).getPrefix(), "photos/2006/March/");

        } finally {
            deleteAllObjectVersions(provider(), bucket);
            candyS3.deleteBucket(bucket);
        }
    }

    @Test
    @Tag(TestTag.versioning)
    void getVersioningObjectTest() throws IOException, NoSuchAlgorithmException {
        CandyS3 candyS3 = init(provider());
        String bucket = genTestBucketName("getVersioningObjectTest");
        try {
            // Create bucket and enable versioning
            candyS3.createBucket(new CreateBucketOptions.CreateBucketOptionsBuilder(bucket).build());
            candyS3.setBucketVersioning(bucket, true);
            Assertions.assertTrue(candyS3.isBucketVersioning(bucket));

            String objectKey = "getVersioningObjectTest.data";
            candyS3.putObject(bucket, objectKey, new PutObjectOptions.PutObjectOptionsBuilder()
                    .configureUploadData().withData(new byte[]{1}).endConfigureDataContent().build());
            candyS3.putObject(bucket, objectKey, new PutObjectOptions.PutObjectOptionsBuilder()
                    .configureUploadData().withData(new byte[]{1, 2}).endConfigureDataContent()
                    .build());

            // GetObject without versionId will get latest version
            {
                S3Object s3ObjectLatest = candyS3.downloadObject(bucket, objectKey, new DownloadObjectOptions.DownloadObjectOptionsBuilder()
                        .configureDataOutput().toBytes().endConfigureDataOutput()
                        .build());
                Assertions.assertArrayEquals(new byte[]{1, 2}, s3ObjectLatest.getContentBytes());
            }

            ListPaginationResult<S3ObjectVersion> objectVersions = candyS3.listObjectVersions(bucket, new ListObjectVersionsOptions().prefix(objectKey));
            // GetObject with versionId to get specified version
            {
                S3Object s3ObjectV1 = candyS3.downloadObject(bucket, objectKey, new DownloadObjectOptions.DownloadObjectOptionsBuilder()
                        .versionId(objectVersions.getResults().get(1).getVersionId())
                        .configureDataOutput().toBytes().endConfigureDataOutput()
                        .build());
                Assertions.assertArrayEquals(new byte[]{1}, s3ObjectV1.getContentBytes());
            }


            // Delete object without version id, will create a deleteMarker
            candyS3.deleteObject(bucket, new DeleteObjectOptions(objectKey));

            // GetObject without versionId will get a 404 (Not Found) error.
            {
                try {
                    candyS3.downloadObject(bucket, objectKey, new DownloadObjectOptions.DownloadObjectOptionsBuilder()
                            .configureDataOutput().toBytes().endConfigureDataOutput()
                            .build());
                    Assertions.fail("Should not be here. A 404 (Not Found) error should be thrown.");
                } catch (Exception ex) {
                    Assertions.assertTrue(ex instanceof CandyS3Exception);
                    Assertions.assertEquals(CommonErrorCode.NO_SUCH_OBJECT.getCode(), ((CandyS3Exception) ex).getCode());
                }
            }

            objectVersions = candyS3.listObjectVersions(bucket, new ListObjectVersionsOptions());

            // GetObject for a deleteMarker doesn't retrieve anything because a delete marker has no data, will get a 405 (Method Not Allowed) error
            {
                try {
                    candyS3.downloadObject(bucket, objectKey, new DownloadObjectOptions.DownloadObjectOptionsBuilder()
                            .versionId(objectVersions.getDeleteMarkers().get(0).getVersionId())
                            .configureDataOutput().toBytes().endConfigureDataOutput()
                            .build());
                    Assertions.fail("Should not be here. A 405 (Method Not Allowed) error should be thrown.");
                } catch (Exception ex) {
                    Assertions.assertTrue(ex instanceof CandyS3Exception);
                    if (S3Provider.TENCENTCLOUD_COS.equals(provider())) {
                        Assertions.assertEquals(CommonErrorCode.NO_SUCH_OBJECT.getCode(), ((CandyS3Exception) ex).getCode());
                    } else {
                        Assertions.assertEquals("MethodNotAllowed", ((CandyS3Exception) ex).getParsedError().getCode());
                    }
                }
            }

            // GetObject with versionId to get specified version
            {
                S3Object s3ObjectVersionV1 = candyS3.downloadObject(bucket, objectKey, new DownloadObjectOptions.DownloadObjectOptionsBuilder()
                        .versionId(objectVersions.getResults().get(1).getVersionId())
                        .configureDataOutput().toBytes().endConfigureDataOutput()
                        .build());
                Assertions.assertArrayEquals(new byte[]{1}, s3ObjectVersionV1.getContentBytes());
            }


        } finally {
            deleteAllObjectVersions(provider(), bucket);
            candyS3.deleteBucket(bucket);
        }
    }

    @Test
    @Tag(TestTag.versioning)
    void getObjectVersionMetadataTest() throws IOException, NoSuchAlgorithmException {
        CandyS3 candyS3 = init(provider());
        String bucket = genTestBucketName("gOVMTest");
        String objectKey = "getObjectVersionMetadataTest.data";
        byte[] bytesV1 = "x".getBytes(StandardCharsets.UTF_8);
        byte[] bytesV2 = "yy".getBytes(StandardCharsets.UTF_8);
        try {
            candyS3.createBucket(new CreateBucketOptions.CreateBucketOptionsBuilder(bucket).enableObjectLock().build());

            candyS3.setBucketVersioning(bucket, true);

            Date expiresDate1 = new Date(System.currentTimeMillis() + 1000 * 3600 * 2);
            candyS3.putObject(bucket, objectKey, new PutObjectOptions.PutObjectOptionsBuilder()
                    .configureUploadData().withData(bytesV1).endConfigureDataContent()
                    .configurePutObjectHeaderOptions()
                    .cacheControl("public, max-age=1")
                    .contentDisposition("attachment; filename=\"thisisaattachment-1.html\"")
                    .contentEncoding("gzip")
                    .contentLanguage("en")
                    .contentType("text/plain")
                    .expires(expiresDate1)
                    .endConfigurePutObjectHeaderOptions()
                    .build());
            Date expiresDate2 = new Date(System.currentTimeMillis() + 1000 * 3600 * 3);
            candyS3.putObject(bucket, objectKey, new PutObjectOptions.PutObjectOptionsBuilder()
                    .configureUploadData().withData(bytesV2).endConfigureDataContent()
                    .configurePutObjectHeaderOptions()
                    .cacheControl("public, max-age=3600")
                    .contentDisposition("attachment; filename=\"thisisaattachment.html\"")
                    .contentEncoding("gzip, deflate")
                    .contentLanguage("en, zh-CN")
                    .contentType("text/html")
                    .expires(expiresDate2)
                    .endConfigurePutObjectHeaderOptions()
                    .build());

            // getObjectMetadata with versionId
            {
                ListPaginationResult<S3ObjectVersion> objectVersions = candyS3.listObjectVersions(bucket, new ListObjectVersionsOptions());
                String versionId1 = objectVersions.getResults().stream()
                        .filter(r -> r.getSize() == bytesV1.length)
                        .findFirst()
                        .get().getVersionId();
                String versionId2 = objectVersions.getResults().stream()
                        .filter(r -> r.getSize() == bytesV2.length)
                        .findFirst()
                        .get().getVersionId();

                // object current version
                {
                    S3Object s3ObjectCurVersion = candyS3.getObjectMetadata(bucket, objectKey, new DownloadObjectOptions.DownloadObjectOptionsBuilder().build());
                    S3Object.S3ObjectMetadata objectMetadataCurVersion = s3ObjectCurVersion.getObjectMetadata();

                    Assertions.assertNull(s3ObjectCurVersion.getContentBytes());
                    Assertions.assertNotNull(s3ObjectCurVersion.getObjectMetadata());

                    Assertions.assertEquals(s3ObjectCurVersion.getSize(), bytesV2.length);

                    Assertions.assertEquals(objectMetadataCurVersion.getCacheControl(), "public, max-age=3600");
                    Assertions.assertEquals(objectMetadataCurVersion.getContentDisposition(), "attachment; filename=\"thisisaattachment.html\"");
                    Assertions.assertEquals(objectMetadataCurVersion.getContentEncoding(), "gzip, deflate");
                    Assertions.assertEquals(objectMetadataCurVersion.getContentLanguage(), "en, zh-CN");
                    Assertions.assertEquals(objectMetadataCurVersion.getContentType(), "text/html");
                    Assertions.assertTrue(objectMetadataCurVersion.getExpires().after(new Date(expiresDate2.getTime() - 1000)));
                    Assertions.assertTrue(objectMetadataCurVersion.getExpires().before(new Date(expiresDate2.getTime() + 1000)));
                }

                // object version 1
                {
                    S3Object s3ObjectV1 = candyS3.getObjectMetadata(bucket, objectKey, new DownloadObjectOptions.DownloadObjectOptionsBuilder()
                            .versionId(versionId1).build());
                    S3Object.S3ObjectMetadata objectMetadataV1 = s3ObjectV1.getObjectMetadata();

                    Assertions.assertNull(s3ObjectV1.getContentBytes());
                    Assertions.assertNotNull(s3ObjectV1.getObjectMetadata());

                    Assertions.assertEquals(s3ObjectV1.getSize(), bytesV1.length);

                    Assertions.assertEquals(objectMetadataV1.getCacheControl(), "public, max-age=1");
                    Assertions.assertEquals(objectMetadataV1.getContentDisposition(), "attachment; filename=\"thisisaattachment-1.html\"");
                    Assertions.assertEquals(objectMetadataV1.getContentEncoding(), "gzip");
                    Assertions.assertEquals(objectMetadataV1.getContentLanguage(), "en");
                    Assertions.assertEquals(objectMetadataV1.getContentType(), "text/plain");
                    Assertions.assertTrue(objectMetadataV1.getExpires().after(new Date(expiresDate1.getTime() - 1000)));
                    Assertions.assertTrue(objectMetadataV1.getExpires().before(new Date(expiresDate1.getTime() + 1000)));
                }

                // object version 2 which is the latest version
                {
                    S3Object s3ObjectV2 = candyS3.getObjectMetadata(bucket, objectKey, new DownloadObjectOptions.DownloadObjectOptionsBuilder()
                            .versionId(versionId2).build());
                    S3Object.S3ObjectMetadata objectMetadataV2 = s3ObjectV2.getObjectMetadata();

                    Assertions.assertNull(s3ObjectV2.getContentBytes());
                    Assertions.assertNotNull(s3ObjectV2.getObjectMetadata());

                    Assertions.assertEquals(s3ObjectV2.getSize(), bytesV2.length);

                    Assertions.assertEquals(objectMetadataV2.getCacheControl(), "public, max-age=3600");
                    Assertions.assertEquals(objectMetadataV2.getContentDisposition(), "attachment; filename=\"thisisaattachment.html\"");
                    Assertions.assertEquals(objectMetadataV2.getContentEncoding(), "gzip, deflate");
                    Assertions.assertEquals(objectMetadataV2.getContentLanguage(), "en, zh-CN");
                    Assertions.assertEquals(objectMetadataV2.getContentType(), "text/html");
                    Assertions.assertTrue(objectMetadataV2.getExpires().after(new Date(expiresDate2.getTime() - 1000)));
                    Assertions.assertTrue(objectMetadataV2.getExpires().before(new Date(expiresDate2.getTime() + 1000)));
                }
            }

            // delete object
            candyS3.deleteObject(bucket, new DeleteObjectOptions(objectKey));

            // getObjectMetadata for deleted object
            {
                ListPaginationResult<S3ObjectVersion> objectVersions = candyS3.listObjectVersions(bucket, new ListObjectVersionsOptions());
                String versionId1 = objectVersions.getResults().stream()
                        .filter(r -> r.getSize() == bytesV1.length)
                        .findFirst()
                        .get().getVersionId();
                String versionId2 = objectVersions.getResults().stream()
                        .filter(r -> r.getSize() == bytesV2.length)
                        .findFirst()
                        .get().getVersionId();
                String versionIdDeleteMarker = objectVersions.getDeleteMarkers().get(0).getVersionId();

                {
                    // If the current version of the object is a delete marker, Amazon S3 behaves as if the object was deleted and includes x-amz-delete-marker: true in the response.
                    try {
                        candyS3.getObjectMetadata(bucket, objectKey, new DownloadObjectOptions.DownloadObjectOptionsBuilder().build());
                        Assertions.fail("Should not be here. A 404 (Not Found) error should be thrown.");
                    } catch (Exception ex) {
                        Assertions.assertTrue(ex instanceof CandyS3Exception);
                        if (S3Provider.TENCENTCLOUD_COS.equals(provider())) {
                            Assertions.assertEquals(CommonErrorCode.NO_SUCH_OBJECT.getCode(), ((CandyS3Exception) ex).getCode());
                        } else {
                            Assertions.assertEquals(CommonErrorCode.OBJECT_VERSION_IS_DELETE_MARKER.getCode(), ((CandyS3Exception) ex).getCode());
                        }
                    }
                }

                {
                    // get object with versionId which is a delete marker
                    // If the specified version is a delete marker, the response returns a 405 Method Not Allowed error and the Last-Modified: timestamp response header.
                    try {
                        candyS3.getObjectMetadata(bucket, objectKey, new DownloadObjectOptions.DownloadObjectOptionsBuilder()
                                .versionId(versionIdDeleteMarker).build());
                        Assertions.fail("Should not be here. A 405 (Method Not Allowed) error should be thrown.");
                    } catch (Exception ex) {
                        Assertions.assertTrue(ex instanceof CandyS3Exception);
                        if (S3Provider.TENCENTCLOUD_COS.equals(provider())) {
                            Assertions.assertEquals(CommonErrorCode.OBJECT_VERSION_IS_DELETE_MARKER.getCode(), ((CandyS3Exception) ex).getCode());
                        } else {
                            Assertions.assertEquals("Method Not Allowed", ((CandyS3Exception) ex).getParsedError().getMessage());
                        }
                    }
                }

                {
                    // get object with versionId which is the latest version before delete
                    S3Object s3ObjectV2 = candyS3.getObjectMetadata(bucket, objectKey, new DownloadObjectOptions.DownloadObjectOptionsBuilder()
                            .versionId(versionId2).build());
                    S3Object.S3ObjectMetadata objectMetadataV2 = s3ObjectV2.getObjectMetadata();

                    Assertions.assertNull(s3ObjectV2.getContentBytes());
                    Assertions.assertNotNull(s3ObjectV2.getObjectMetadata());

                    Assertions.assertEquals(s3ObjectV2.getSize(), bytesV2.length);

                    Assertions.assertEquals(objectMetadataV2.getCacheControl(), "public, max-age=3600");
                    Assertions.assertEquals(objectMetadataV2.getContentDisposition(), "attachment; filename=\"thisisaattachment.html\"");
                    Assertions.assertEquals(objectMetadataV2.getContentEncoding(), "gzip, deflate");
                    Assertions.assertEquals(objectMetadataV2.getContentLanguage(), "en, zh-CN");
                    Assertions.assertEquals(objectMetadataV2.getContentType(), "text/html");
                    Assertions.assertTrue(objectMetadataV2.getExpires().after(new Date(expiresDate2.getTime() - 1000)));
                    Assertions.assertTrue(objectMetadataV2.getExpires().before(new Date(expiresDate2.getTime() + 1000)));
                }

                {
                    // get object with versionId which is a history version before delete
                    S3Object s3ObjectV1 = candyS3.getObjectMetadata(bucket, objectKey, new DownloadObjectOptions.DownloadObjectOptionsBuilder()
                            .versionId(versionId1).build());
                    S3Object.S3ObjectMetadata objectMetadataV1 = s3ObjectV1.getObjectMetadata();

                    Assertions.assertNull(s3ObjectV1.getContentBytes());
                    Assertions.assertNotNull(s3ObjectV1.getObjectMetadata());

                    Assertions.assertEquals(s3ObjectV1.getSize(), bytesV1.length);

                    Assertions.assertEquals(objectMetadataV1.getCacheControl(), "public, max-age=1");
                    Assertions.assertEquals(objectMetadataV1.getContentDisposition(), "attachment; filename=\"thisisaattachment-1.html\"");
                    Assertions.assertEquals(objectMetadataV1.getContentEncoding(), "gzip");
                    Assertions.assertEquals(objectMetadataV1.getContentLanguage(), "en");
                    Assertions.assertEquals(objectMetadataV1.getContentType(), "text/plain");
                    Assertions.assertTrue(objectMetadataV1.getExpires().after(new Date(expiresDate1.getTime() - 1000)));
                    Assertions.assertTrue(objectMetadataV1.getExpires().before(new Date(expiresDate1.getTime() + 1000)));
                }

            }

        } finally {
            deleteAllObjectVersions(provider(), bucket);
            candyS3.deleteBucket(bucket);
        }
    }

    @Test
    @Tag(TestTag.versioning)
    void deleteVersioningObjectTest() throws IOException, NoSuchAlgorithmException {
        CandyS3 candyS3 = init(provider());
        String bucket = genTestBucketName("deleteVersioningObjectTest");
        try {
            // Create bucket and enable versioning
            candyS3.createBucket(new CreateBucketOptions.CreateBucketOptionsBuilder(bucket).build());
            candyS3.setBucketVersioning(bucket, true);
            Assertions.assertTrue(candyS3.isBucketVersioning(bucket));

            {
                String objectKey1 = "deleteVersioningObjectTest1.data";
                candyS3.putObject(bucket, objectKey1, new PutObjectOptions.PutObjectOptionsBuilder()
                        .configureUploadData().withData(new byte[]{1}).endConfigureDataContent().build());
                candyS3.putObject(bucket, objectKey1, new PutObjectOptions.PutObjectOptionsBuilder()
                        .configureUploadData().withData(new byte[]{1, 2}).endConfigureDataContent()
                        .build());

                ListPaginationResult<S3Object> objects = candyS3.listObjects(bucket, new ListObjectOptions());
                Assertions.assertEquals(objects.getResults().size(), 1);

                ListPaginationResult<S3ObjectVersion> objectVersions = candyS3.listObjectVersions(bucket, new ListObjectVersionsOptions());
                Assertions.assertEquals(objectVersions.getResults().size(), 2);
                Assertions.assertEquals(objectVersions.getResults().get(0).getSize(), 2);
                Assertions.assertEquals(objectVersions.getResults().get(1).getSize(), 1);
                Assertions.assertTrue(objectVersions.getDeleteMarkers().isEmpty());


                // Delete object without version id, will create a deleteMarker
                candyS3.deleteObject(bucket, new DeleteObjectOptions(objectKey1));

                // ListObject will not return the object
                objects = candyS3.listObjects(bucket, new ListObjectOptions());
                Assertions.assertEquals(objects.getResults().size(), 0);

                // ListObjectVersion will return the object's all versions and deleteMarkers
                objectVersions = candyS3.listObjectVersions(bucket, new ListObjectVersionsOptions());
                Assertions.assertEquals(objectVersions.getResults().size(), 2);
                Assertions.assertEquals(objectVersions.getResults().get(0).getSize(), 2);
                Assertions.assertEquals(objectVersions.getResults().get(1).getSize(), 1);
                Assertions.assertEquals(objectVersions.getDeleteMarkers().size(), 1);
                Assertions.assertEquals(objectVersions.getDeleteMarkers().get(0).getSize(), 0);

                // When delete the deleteMarker with versionId, the object latest version will be retained
                candyS3.deleteObject(bucket, new DeleteObjectOptions(objectKey1).versionId(objectVersions.getDeleteMarkers().get(0).getVersionId()));
                objects = candyS3.listObjects(bucket, new ListObjectOptions());
                Assertions.assertEquals(objects.getResults().size(), 1);
                Assertions.assertEquals(objects.getResults().get(0).getSize(), 2);

                objectVersions = candyS3.listObjectVersions(bucket, new ListObjectVersionsOptions());
                Assertions.assertEquals(objectVersions.getResults().size(), 2);
                Assertions.assertEquals(objectVersions.getResults().get(0).getSize(), 2);
                Assertions.assertEquals(objectVersions.getResults().get(1).getSize(), 1);
                Assertions.assertEquals(objectVersions.getDeleteMarkers().size(), 0);

                deleteAllObjectVersions(provider(), bucket);
            }

            {
                String objectKey2 = "deleteVersioningObjectTest2.data";
                candyS3.putObject(bucket, objectKey2, new PutObjectOptions.PutObjectOptionsBuilder()
                        .configureUploadData().withData(new byte[]{1}).endConfigureDataContent().build());
                candyS3.putObject(bucket, objectKey2, new PutObjectOptions.PutObjectOptionsBuilder()
                        .configureUploadData().withData(new byte[]{1, 2}).endConfigureDataContent()
                        .build());

                // Delete object without version id, will create a deleteMarker
                candyS3.deleteObject(bucket, new DeleteObjectOptions(objectKey2));

                // ListObject will not return the object
                ListPaginationResult<S3Object> objects = candyS3.listObjects(bucket, new ListObjectOptions());
                Assertions.assertEquals(objects.getResults().size(), 0);

                // ListObjectVersion will return the object's all versions and deleteMarkers
                ListPaginationResult<S3ObjectVersion> objectVersions = candyS3.listObjectVersions(bucket, new ListObjectVersionsOptions());
                Assertions.assertEquals(objectVersions.getResults().size(), 2);
                Assertions.assertEquals(objectVersions.getResults().get(0).getSize(), 2);
                Assertions.assertEquals(objectVersions.getResults().get(1).getSize(), 1);
                Assertions.assertEquals(objectVersions.getDeleteMarkers().size(), 1);
                Assertions.assertEquals(objectVersions.getDeleteMarkers().get(0).getSize(), 0);

                // When delete the deleteMarker without versionId, removes nothing, but instead adds an additional delete marker
                candyS3.deleteObject(bucket, new DeleteObjectOptions(objectKey2));
                objects = candyS3.listObjects(bucket, new ListObjectOptions());
                Assertions.assertEquals(objects.getResults().size(), 0);

                objectVersions = candyS3.listObjectVersions(bucket, new ListObjectVersionsOptions());
                Assertions.assertEquals(objectVersions.getResults().size(), 2);
                Assertions.assertEquals(objectVersions.getResults().get(0).getSize(), 2);
                Assertions.assertEquals(objectVersions.getResults().get(1).getSize(), 1);
                Assertions.assertEquals(objectVersions.getDeleteMarkers().size(), 2);
                Assertions.assertEquals(objectVersions.getDeleteMarkers().get(0).getSize(), 0);
                Assertions.assertEquals(objectVersions.getDeleteMarkers().get(1).getSize(), 0);

                deleteAllObjectVersions(provider(), bucket);
            }


            {
                String objectKey3 = "deleteVersioningObjectTest3.data";
                candyS3.putObject(bucket, objectKey3, new PutObjectOptions.PutObjectOptionsBuilder()
                        .configureUploadData().withData(new byte[]{1}).endConfigureDataContent().build());
                candyS3.putObject(bucket, objectKey3, new PutObjectOptions.PutObjectOptionsBuilder()
                        .configureUploadData().withData(new byte[]{1, 2}).endConfigureDataContent()
                        .build());

                ListPaginationResult<S3Object> objects = candyS3.listObjects(bucket, new ListObjectOptions());
                Assertions.assertEquals(objects.getResults().size(), 1);

                ListPaginationResult<S3ObjectVersion> objectVersions = candyS3.listObjectVersions(bucket, new ListObjectVersionsOptions());
                Assertions.assertEquals(objectVersions.getResults().size(), 2);
                Assertions.assertEquals(objectVersions.getResults().get(0).getSize(), 2);
                Assertions.assertEquals(objectVersions.getResults().get(1).getSize(), 1);
                Assertions.assertTrue(objectVersions.getDeleteMarkers().isEmpty());


                // Delete object with version id, will remove the version without creating a deleteMarker
                candyS3.deleteObject(bucket, new DeleteObjectOptions(objectKey3).versionId(objectVersions.getResults().get(0).getVersionId()));
                candyS3.deleteObject(bucket, new DeleteObjectOptions(objectKey3).versionId(objectVersions.getResults().get(1).getVersionId()));

                objects = candyS3.listObjects(bucket, new ListObjectOptions());
                Assertions.assertEquals(objects.getResults().size(), 0);

                objectVersions = candyS3.listObjectVersions(bucket, new ListObjectVersionsOptions());
                Assertions.assertEquals(objectVersions.getResults().size(), 0);
                Assertions.assertEquals(objectVersions.getDeleteMarkers().size(), 0);

                deleteAllObjectVersions(provider(), bucket);
            }


        } finally {
            candyS3.deleteBucket(bucket);
        }
    }

    @Test
    @Tag(TestTag.basic)
    void putDownloadEmptyObjectTest() throws IOException, NoSuchAlgorithmException {
        CandyS3 candyS3 = init(provider());
        String bucket = genTestBucketName("putDownloadEmptyObjectTest");
        String objectKey = "putDownloadEmptyObjectTest.data";
        try {
            candyS3.createBucket(new CreateBucketOptions.CreateBucketOptionsBuilder(bucket).build());
            candyS3.putObject(bucket, objectKey, new PutObjectOptions.PutObjectOptionsBuilder().build());
            ListPaginationResult<S3Object> result =
                    candyS3.listObjects(bucket, new ListObjectOptions().prefix(objectKey).maxKeys(1));
            Assertions.assertEquals(result.getResults().get(0).getKey(), objectKey);
            Assertions.assertEquals(result.getResults().get(0).getSize(), 0);

            S3Object downloadObject = candyS3.downloadObject(bucket, objectKey,
                    new DownloadObjectOptions.DownloadObjectOptionsBuilder()
                            .configureDataOutput().toBytes().endConfigureDataOutput()
                            .build());
            Assertions.assertEquals(0, downloadObject.getContentBytes().length);

        } finally {
            candyS3.deleteObject(bucket, new DeleteObjectOptions(objectKey));
            candyS3.deleteBucket(bucket);
        }
    }

    @Test
    @Tag(TestTag.basic)
    void putDownloadSmallObjectTest() throws IOException, NoSuchAlgorithmException {
        CandyS3 candyS3 = init(provider());
        String bucket = genTestBucketName("putDownloadSmallObjectTest");
        File file = new File("./temp/tempFile.data");
        String content = StringUtils.repeat("x", 1024);
        try {
            candyS3.createBucket(new CreateBucketOptions.CreateBucketOptionsBuilder(bucket).build());

            byte[] bytes = content.getBytes(StandardCharsets.UTF_8);

            {
                String objectKey1 = "putDownloadSmallObjectTest1.data";
                candyS3.putObject(bucket, objectKey1, new PutObjectOptions.PutObjectOptionsBuilder()
                        .configureUploadData().withData(bytes).endConfigureDataContent()
                        .build());
                ListPaginationResult<S3Object> result =
                        candyS3.listObjects(bucket, new ListObjectOptions().prefix(objectKey1).maxKeys(1));
                Assertions.assertEquals(result.getResults().get(0).getKey(), objectKey1);
                Assertions.assertEquals(result.getResults().get(0).getSize(), 1024);

                S3Object downloadObject1 = candyS3.downloadObject(bucket, objectKey1,
                        new DownloadObjectOptions.DownloadObjectOptionsBuilder()
                                .configureDataOutput().toBytes().endConfigureDataOutput()
                                .build());
                Assertions.assertArrayEquals(bytes, downloadObject1.getContentBytes());

                candyS3.deleteObject(bucket, new DeleteObjectOptions(objectKey1));
            }

            file.createNewFile();
            FileWriter fileWriter = new FileWriter(file);
            fileWriter.write(content);
            fileWriter.flush();
            fileWriter.close();

            {
                String objectKey4 = "putDownloadSmallObjectTest4.data";
                candyS3.putObject(bucket, objectKey4, new PutObjectOptions.PutObjectOptionsBuilder()
                        .configureUploadData().withData(file.getAbsolutePath()).endConfigureDataContent()
                        .build());
                ListPaginationResult<S3Object> result4 =
                        candyS3.listObjects(bucket, new ListObjectOptions().prefix(objectKey4).maxKeys(1));
                Assertions.assertEquals(result4.getResults().get(0).getKey(), objectKey4);
                Assertions.assertEquals(result4.getResults().get(0).getSize(), 1024);

                S3Object downloadObject4 = candyS3.downloadObject(bucket, objectKey4,
                        new DownloadObjectOptions.DownloadObjectOptionsBuilder()
                                .configureDataOutput().toBytes().endConfigureDataOutput()
                                .build());
                Assertions.assertArrayEquals(bytes, downloadObject4.getContentBytes());

                candyS3.deleteObject(bucket, new DeleteObjectOptions(objectKey4));
            }

            {
                String objectKey6 = "putDownloadSmallObjectTest6.data";
                candyS3.putObject(bucket, objectKey6, new PutObjectOptions.PutObjectOptionsBuilder()
                        .configureUploadData().withData(new FileInputStream(file)).endConfigureDataContent()
                        .build());
                ListPaginationResult<S3Object> result6 =
                        candyS3.listObjects(bucket, new ListObjectOptions().prefix(objectKey6).maxKeys(1));
                Assertions.assertEquals(result6.getResults().get(0).getKey(), objectKey6);
                Assertions.assertEquals(result6.getResults().get(0).getSize(), 1024);

                S3Object downloadObject6 = candyS3.downloadObject(bucket, objectKey6,
                        new DownloadObjectOptions.DownloadObjectOptionsBuilder()
                                .configureDataOutput().toBytes().endConfigureDataOutput()
                                .build());
                Assertions.assertArrayEquals(bytes, downloadObject6.getContentBytes());

                candyS3.deleteObject(bucket, new DeleteObjectOptions(objectKey6));
            }
        } finally {
            if (file.exists()) {
                file.delete();
            }
            candyS3.deleteBucket(bucket);
        }
    }

    @Test
    @Tag(TestTag.basic)
    void putDownloadLargeObjectTest() throws IOException, NoSuchAlgorithmException {
        CandyS3 candyS3 = init(provider());
        String bucket = genTestBucketName("putDownloadLargeObjectTest");
        File file = new File("./temp/tempFile.data");
        String content = StringUtils.repeat("x", 6 * 1024 * 1024); // 6MB
        try {
            candyS3.createBucket(new CreateBucketOptions.CreateBucketOptionsBuilder(bucket).build());

            byte[] bytes = content.getBytes(StandardCharsets.UTF_8);

            {
                // When use bytes as input, upload object directly.
                String objectKey2 = "putDownloadLargeObjectTest2.data";
                candyS3.putObject(bucket, objectKey2, new PutObjectOptions.PutObjectOptionsBuilder()
                        .configureUploadData().withData(bytes).endConfigureDataContent()
                        .build());
                ListPaginationResult<S3Object> result2 =
                        candyS3.listObjects(bucket, new ListObjectOptions().prefix(objectKey2).maxKeys(1));
                Assertions.assertEquals(result2.getResults().get(0).getKey(), objectKey2);
                Assertions.assertEquals(result2.getResults().get(0).getSize(), 6 * 1024 * 1024);

                S3Object downloadObject2 = candyS3.downloadObject(bucket, objectKey2,
                        new DownloadObjectOptions.DownloadObjectOptionsBuilder()
                                .configureDataOutput().toBytes().endConfigureDataOutput()
                                .build());
                Assertions.assertArrayEquals(bytes, downloadObject2.getContentBytes());

                candyS3.deleteObject(bucket, new DeleteObjectOptions(objectKey2));
            }

            file.createNewFile();
            FileWriter fileWriter = new FileWriter(file);
            fileWriter.write(content);
            fileWriter.flush();
            fileWriter.close();

            {
                // When use file as input and file size is greater to 5MB, use multipart upload.
                String objectKey4 = "putDownloadLargeObjectTest4.data";
                candyS3.putObject(bucket, objectKey4, new PutObjectOptions.PutObjectOptionsBuilder()
                        .configureUploadData()
                        .withData(file.getAbsolutePath())
                        .endConfigureDataContent()
                        .build());
                ListPaginationResult<S3Object> result4 =
                        candyS3.listObjects(bucket, new ListObjectOptions().prefix(objectKey4).maxKeys(1));
                Assertions.assertEquals(result4.getResults().get(0).getKey(), objectKey4);
                Assertions.assertEquals(result4.getResults().get(0).getSize(), 6 * 1024 * 1024);

                S3Object downloadObject4 = candyS3.downloadObject(bucket, objectKey4,
                        new DownloadObjectOptions.DownloadObjectOptionsBuilder()
                                .configureDataOutput().toBytes().endConfigureDataOutput()
                                .build());
                Assertions.assertArrayEquals(bytes, downloadObject4.getContentBytes());

                candyS3.deleteObject(bucket, new DeleteObjectOptions(objectKey4));
            }

            {
                // When use inputStream as input and available size is greater to 5MB, use multipart upload.
                String objectKey6 = "putDownloadLargeObjectTest6.data";
                candyS3.putObject(bucket, objectKey6, new PutObjectOptions.PutObjectOptionsBuilder()
                        .configureUploadData()
                        .withData(new FileInputStream(file))
                        .endConfigureDataContent()
                        .build());
                ListPaginationResult<S3Object> result6 =
                        candyS3.listObjects(bucket, new ListObjectOptions().prefix(objectKey6).maxKeys(1));
                Assertions.assertEquals(result6.getResults().get(0).getKey(), objectKey6);
                Assertions.assertEquals(result6.getResults().get(0).getSize(), 6 * 1024 * 1024);

                S3Object downloadObject6 = candyS3.downloadObject(bucket, objectKey6,
                        new DownloadObjectOptions.DownloadObjectOptionsBuilder()
                                .configureDataOutput().toBytes().endConfigureDataOutput()
                                .build());
                Assertions.assertArrayEquals(bytes, downloadObject6.getContentBytes());

                candyS3.deleteObject(bucket, new DeleteObjectOptions(objectKey6));
            }
        } finally {
            if (file.exists()) {
                file.delete();
            }
            candyS3.deleteBucket(bucket);
        }
    }

    @Test
    @Tag(TestTag.basic)
    void multipartUploadDownloadTest() throws IOException, NoSuchAlgorithmException {
        CandyS3 candyS3 = init(provider());
        String bucket = genTestBucketName("multipartUploadDownloadTest");
        File file = new File("./temp/tempFile.data");
        File filePart1 = new File("./temp/tempFilePart1.data");
        File filePart2 = new File("./temp/tempFilePart2.data");
        String outputFile = "./temp/tempFileOutput.data";
        String content = StringUtils.repeat("x", 6 * 1024 * 1024); // 6MB
        try {
            candyS3.createBucket(new CreateBucketOptions.CreateBucketOptionsBuilder(bucket).build());
            byte[] bytes = content.getBytes(StandardCharsets.UTF_8);

            {
                String objectKey1 = "multipartUploadDownloadTest1.data";
                String uploadId1 = candyS3.createMultipartUpload(bucket, objectKey1,
                        new CreateMultipartUploadOptions.CreateMultipartUploadOptionsBuilder().build());
                try {
                    List<S3Part> parts1 = new ArrayList<>();
                    S3Part part11 = candyS3.uploadPart(bucket, objectKey1, uploadId1, 1,
                            new UploadPartOptions.UploadPartOptionsBuilder()
                                    .configureUploadData()
                                    .withData(Arrays.copyOfRange(bytes, 0, 5 * 1024 * 1024))
                                    .endConfigureDataContent()
                                    .build());
                    parts1.add(part11);
                    S3Part part12 = candyS3.uploadPart(bucket, objectKey1, uploadId1, 2,
                            new UploadPartOptions.UploadPartOptionsBuilder()
                                    .configureUploadData()
                                    .withData(Arrays.copyOfRange(bytes, 5 * 1024 * 1024, bytes.length))
                                    .endConfigureDataContent()
                                    .build());
                    parts1.add(part12);
                    candyS3.completeMultipartUpload(bucket, objectKey1, uploadId1, parts1,
                            new CompleteMultipartUploadOptions.CompleteMultipartUploadOptionsBuilder().build());
                    ListPaginationResult<S3Object> result1 =
                            candyS3.listObjects(bucket, new ListObjectOptions().prefix(objectKey1).maxKeys(1));
                    Assertions.assertEquals(result1.getResults().get(0).getKey(), objectKey1);
                    Assertions.assertEquals(result1.getResults().get(0).getSize(), 6 * 1024 * 1024);

                    candyS3.downloadObject(bucket, objectKey1,
                            new DownloadObjectOptions.DownloadObjectOptionsBuilder()
                                    .configureDataOutput().toFile(outputFile, true).endConfigureDataOutput()
                                    .build());
                    byte[] downloadContent = Files.readAllBytes(Paths.get(outputFile));
                    Assertions.assertArrayEquals(bytes, downloadContent);
                } catch (Exception ex) {
                    candyS3.abortMultipartUpload(bucket, objectKey1, uploadId1,
                            new AbortMultipartUploadOptions());
                    throw ex;
                } finally {
                    Files.delete(Paths.get(outputFile));
                    candyS3.deleteObject(bucket, new DeleteObjectOptions(objectKey1));
                }

            }

            file.createNewFile();
            FileWriter fileWriter = new FileWriter(file);
            fileWriter.write(content);
            fileWriter.flush();
            fileWriter.close();

            filePart1.createNewFile();
            FileWriter fileWriterPart1 = new FileWriter(filePart1);
            fileWriterPart1.write(new String(Arrays.copyOfRange(bytes, 0, 5 * 1024 * 1024)));
            fileWriterPart1.flush();
            fileWriterPart1.close();

            filePart2.createNewFile();
            FileWriter fileWriterPart2 = new FileWriter(filePart2);
            fileWriterPart2.write(new String(Arrays.copyOfRange(bytes, 5 * 1024 * 1024, bytes.length)));
            fileWriterPart2.flush();
            fileWriterPart2.close();

            {
                String objectKey3 = "multipartUploadDownloadTest3.data";
                String uploadId3 = candyS3.createMultipartUpload(bucket, objectKey3,
                        new CreateMultipartUploadOptions.CreateMultipartUploadOptionsBuilder().build());
                try {
                    List<S3Part> parts3 = new ArrayList<>();

                    S3Part part31 = candyS3.uploadPart(bucket, objectKey3, uploadId3, 1,
                            new UploadPartOptions.UploadPartOptionsBuilder()
                                    .configureUploadData()
                                    .withData(filePart1.getAbsolutePath())
                                    .endConfigureDataContent()
                                    .build());
                    parts3.add(part31);
                    S3Part part32 = candyS3.uploadPart(bucket, objectKey3, uploadId3, 2,
                            new UploadPartOptions.UploadPartOptionsBuilder()
                                    .configureUploadData()
                                    .withData(filePart2.getAbsolutePath())
                                    .endConfigureDataContent()
                                    .build());
                    parts3.add(part32);
                    candyS3.completeMultipartUpload(bucket, objectKey3, uploadId3, parts3,
                            new CompleteMultipartUploadOptions.CompleteMultipartUploadOptionsBuilder().build());
                    ListPaginationResult<S3Object> result3 =
                            candyS3.listObjects(bucket, new ListObjectOptions().prefix(objectKey3).maxKeys(1));
                    Assertions.assertEquals(result3.getResults().get(0).getKey(), objectKey3);
                    Assertions.assertEquals(result3.getResults().get(0).getSize(), 6 * 1024 * 1024);

                    candyS3.downloadObject(bucket, objectKey3,
                            new DownloadObjectOptions.DownloadObjectOptionsBuilder()
                                    .configureDataOutput().toFile(outputFile, true).endConfigureDataOutput()
                                    .build());
                    byte[] downloadContent = Files.readAllBytes(Paths.get(outputFile));
                    Assertions.assertArrayEquals(bytes, downloadContent);
                } catch (Exception ex) {
                    candyS3.abortMultipartUpload(bucket, objectKey3, uploadId3,
                            new AbortMultipartUploadOptions());
                    throw ex;
                } finally {
                    Files.delete(Paths.get(outputFile));
                    candyS3.deleteObject(bucket, new DeleteObjectOptions(objectKey3));
                }
            }

            {
                String objectKey5 = "multipartUploadDownloadTest5.data";
                String uploadId5 = candyS3.createMultipartUpload(bucket, objectKey5,
                        new CreateMultipartUploadOptions.CreateMultipartUploadOptionsBuilder().build());
                try {
                    List<S3Part> parts5 = new ArrayList<>();
                    S3Part part51 = candyS3.uploadPart(bucket, objectKey5, uploadId5, 1,
                            new UploadPartOptions.UploadPartOptionsBuilder()
                                    .configureUploadData()
                                    .withData(new FileInputStream(filePart1))
                                    .endConfigureDataContent()
                                    .build());
                    parts5.add(part51);
                    S3Part part52 = candyS3.uploadPart(bucket, objectKey5, uploadId5, 2,
                            new UploadPartOptions.UploadPartOptionsBuilder()
                                    .configureUploadData()
                                    .withData(new FileInputStream(filePart2))
                                    .endConfigureDataContent()
                                    .build());
                    parts5.add(part52);
                    candyS3.completeMultipartUpload(bucket, objectKey5, uploadId5, parts5,
                            new CompleteMultipartUploadOptions.CompleteMultipartUploadOptionsBuilder().build());
                    ListPaginationResult<S3Object> result5 =
                            candyS3.listObjects(bucket, new ListObjectOptions().prefix(objectKey5).maxKeys(1));
                    Assertions.assertEquals(result5.getResults().get(0).getKey(), objectKey5);
                    Assertions.assertEquals(result5.getResults().get(0).getSize(), 6 * 1024 * 1024);

                    candyS3.downloadObject(bucket, objectKey5,
                            new DownloadObjectOptions.DownloadObjectOptionsBuilder()
                                    .configureDataOutput().toFile(outputFile, true).endConfigureDataOutput()
                                    .build());
                    byte[] downloadContent = Files.readAllBytes(Paths.get(outputFile));
                    Assertions.assertArrayEquals(bytes, downloadContent);
                } catch (Exception ex) {
                    candyS3.abortMultipartUpload(bucket, objectKey5, uploadId5,
                            new AbortMultipartUploadOptions());
                    throw ex;
                } finally {
                    Files.delete(Paths.get(outputFile));
                    candyS3.deleteObject(bucket, new DeleteObjectOptions(objectKey5));
                }
            }

        } finally {
            candyS3.deleteBucket(bucket);
        }
    }

    @Test
    @Tag(TestTag.basic)
    void multipartUploadToExistsObjectTest() throws IOException, NoSuchAlgorithmException {
        CandyS3 candyS3 = init(provider());
        String bucket = genTestBucketName("mpUToExistsOTest");
        String objectKey = "multipartUploadToExistsObjectTest.data";
        try {
            candyS3.createBucket(new CreateBucketOptions.CreateBucketOptionsBuilder(bucket).build());
            candyS3.putObject(bucket, objectKey, new PutObjectOptions.PutObjectOptionsBuilder()
                    .configureUploadData().withData(new byte[]{1}).endConfigureDataContent()
                    .build());

            String uploadId = candyS3.createMultipartUpload(bucket, objectKey,
                    new CreateMultipartUploadOptions.CreateMultipartUploadOptionsBuilder().build());
            List<S3Part> parts = new ArrayList<>();

            String content1 = StringUtils.repeat("2", 5 * 1024 * 1024); // 5MB
            S3Part part1 = candyS3.uploadPart(bucket, objectKey, uploadId, 1,
                    new UploadPartOptions.UploadPartOptionsBuilder()
                            .configureUploadData()
                            .withData(content1.getBytes(StandardCharsets.UTF_8))
                            .endConfigureDataContent()
                            .build());
            parts.add(part1);
            String content2 = "3";
            S3Part part2 = candyS3.uploadPart(bucket, objectKey, uploadId, 2,
                    new UploadPartOptions.UploadPartOptionsBuilder()
                            .configureUploadData()
                            .withData(content2.getBytes(StandardCharsets.UTF_8))
                            .endConfigureDataContent()
                            .build());
            parts.add(part2);

            candyS3.completeMultipartUpload(bucket, objectKey, uploadId, parts, new CompleteMultipartUploadOptions.CompleteMultipartUploadOptionsBuilder().build());

            ListPaginationResult<S3MultipartUpload> uploads = candyS3.listMultipartUploads(bucket, new ListMultipartUploadOptions());
            Assertions.assertEquals(uploads.getResults().size(), 0);

            ListPaginationResult<S3Object> findObjects = candyS3.listObjects(bucket, new ListObjectOptions().prefix(objectKey));
            Assertions.assertEquals(findObjects.getResults().size(), 1);

            S3Object downloadObject = candyS3.downloadObject(bucket, objectKey, new DownloadObjectOptions.DownloadObjectOptionsBuilder()
                    .configureDataOutput().toBytes().endConfigureDataOutput()
                    .build());

            byte[] expectedBytes = (content1 + content2).getBytes(StandardCharsets.UTF_8);
            Assertions.assertArrayEquals(expectedBytes, downloadObject.getContentBytes());

        } finally {
            candyS3.deleteObject(bucket, new DeleteObjectOptions(objectKey));
            candyS3.deleteBucket(bucket);
        }
    }

    @Test
    @Tag(TestTag.basic)
    void abortMultipartUploadTest() throws IOException, NoSuchAlgorithmException {
        CandyS3 candyS3 = init(provider());
        String bucket = genTestBucketName("abortMultipartUploadTest");
        String objectKey = "abortMultipartUploadTest.data";
        String content = StringUtils.repeat("x", 6 * 1024 * 1024); // 6MB
        try {
            candyS3.createBucket(new CreateBucketOptions.CreateBucketOptionsBuilder(bucket).build());
            byte[] bytes = content.getBytes(StandardCharsets.UTF_8);

            String uploadId = candyS3.createMultipartUpload(bucket, objectKey,
                    new CreateMultipartUploadOptions.CreateMultipartUploadOptionsBuilder().build());
            List<S3Part> parts = new ArrayList<>();
            S3Part part1 = candyS3.uploadPart(bucket, objectKey, uploadId, 1,
                    new UploadPartOptions.UploadPartOptionsBuilder()
                            .configureUploadData()
                            .withData(Arrays.copyOfRange(bytes, 0, 5 * 1024 * 1024))
                            .endConfigureDataContent()
                            .build());
            parts.add(part1);
            S3Part part2 = candyS3.uploadPart(bucket, objectKey, uploadId, 2,
                    new UploadPartOptions.UploadPartOptionsBuilder()
                            .configureUploadData()
                            .withData(Arrays.copyOfRange(bytes, 5 * 1024 * 1024, bytes.length))
                            .endConfigureDataContent()
                            .build());
            parts.add(part2);

            ListPaginationResult<S3MultipartUpload> uploads1 = candyS3.listMultipartUploads(bucket,
                    new ListMultipartUploadOptions());
            Assertions.assertEquals(uploads1.getResults().size(), 1);
            Assertions.assertEquals(uploads1.getResults().get(0).getKey(), objectKey);
            Assertions.assertEquals(uploads1.getResults().get(0).getUploadId(), uploadId);

            candyS3.abortMultipartUpload(bucket, objectKey, uploadId,
                    new AbortMultipartUploadOptions());

            ListPaginationResult<S3MultipartUpload> uploads2 = candyS3.listMultipartUploads(bucket,
                    new ListMultipartUploadOptions());
            Assertions.assertEquals(uploads2.getResults().size(), 0);

            // object is not created when abortMultipartUpload
            ListPaginationResult<S3Object> createdObjects = candyS3.listObjects(bucket, new ListObjectOptions().prefix(objectKey));
            Assertions.assertEquals(createdObjects.getResults().size(), 0);
        } finally {
            candyS3.deleteBucket(bucket);
        }
    }

    @Test
    @Tag(TestTag.basic)
    void abortMultipartUploadToExistsObjectTest() throws IOException, NoSuchAlgorithmException {
        CandyS3 candyS3 = init(provider());
        String bucket = genTestBucketName("mpUToExistsOTest");
        String objectKey = "multipartUploadToExistsObjectTest.data";
        try {
            candyS3.createBucket(new CreateBucketOptions.CreateBucketOptionsBuilder(bucket).build());
            candyS3.putObject(bucket, objectKey, new PutObjectOptions.PutObjectOptionsBuilder()
                    .configureUploadData().withData(new byte[]{1}).endConfigureDataContent()
                    .build());

            String uploadId = candyS3.createMultipartUpload(bucket, objectKey,
                    new CreateMultipartUploadOptions.CreateMultipartUploadOptionsBuilder().build());
            List<S3Part> parts = new ArrayList<>();
            S3Part part1 = candyS3.uploadPart(bucket, objectKey, uploadId, 1,
                    new UploadPartOptions.UploadPartOptionsBuilder()
                            .configureUploadData()
                            .withData(new byte[]{2})
                            .endConfigureDataContent()
                            .build());
            parts.add(part1);
            S3Part part2 = candyS3.uploadPart(bucket, objectKey, uploadId, 2,
                    new UploadPartOptions.UploadPartOptionsBuilder()
                            .configureUploadData()
                            .withData(new byte[]{3})
                            .endConfigureDataContent()
                            .build());
            parts.add(part2);

            candyS3.abortMultipartUpload(bucket, objectKey, uploadId, new AbortMultipartUploadOptions());

            ListPaginationResult<S3MultipartUpload> uploads = candyS3.listMultipartUploads(bucket, new ListMultipartUploadOptions());
            Assertions.assertEquals(uploads.getResults().size(), 0);

            ListPaginationResult<S3Object> findObjects = candyS3.listObjects(bucket, new ListObjectOptions().prefix(objectKey));
            Assertions.assertEquals(findObjects.getResults().size(), 1);

            S3Object downloadObject = candyS3.downloadObject(bucket, objectKey, new DownloadObjectOptions.DownloadObjectOptionsBuilder()
                    .configureDataOutput().toBytes().endConfigureDataOutput()
                    .build());
            Assertions.assertArrayEquals(new byte[]{1}, downloadObject.getContentBytes());

        } finally {
            candyS3.deleteObject(bucket, new DeleteObjectOptions(objectKey));
            candyS3.deleteBucket(bucket);
        }
    }

    @Test
    @Tag(TestTag.basic)
    void listMultipartUploadsTest() throws IOException {
        CandyS3 candyS3 = init(provider());
        String bucket = genTestBucketName("listMultipartUploadsTest");
        try {
            candyS3.createBucket(new CreateBucketOptions.CreateBucketOptionsBuilder(bucket).build());

            ListPaginationResult<S3MultipartUpload> uploads = candyS3.listMultipartUploads(bucket, new ListMultipartUploadOptions());
            int initialCnt = uploads.getResults().size();

            String objectKey1 = "listMultipartUploadsTest1.data";
            String objectKey2 = "listMultipartUploadsTest2.data";

            String objectKey1UploadId = candyS3.createMultipartUpload(bucket, objectKey1, new CreateMultipartUploadOptions.CreateMultipartUploadOptionsBuilder().build());
            String objectKey1UploadId2 = candyS3.createMultipartUpload(bucket, objectKey1, new CreateMultipartUploadOptions.CreateMultipartUploadOptionsBuilder().build());
            String objectKey2UploadId = candyS3.createMultipartUpload(bucket, objectKey2, new CreateMultipartUploadOptions.CreateMultipartUploadOptionsBuilder().build());

            uploads = candyS3.listMultipartUploads(bucket, new ListMultipartUploadOptions());
            Assertions.assertEquals(uploads.getResults().size(), initialCnt + 3);

            candyS3.abortMultipartUpload(bucket, objectKey1, objectKey1UploadId, new AbortMultipartUploadOptions());

            uploads = candyS3.listMultipartUploads(bucket, new ListMultipartUploadOptions());
            Assertions.assertEquals(uploads.getResults().size(), initialCnt + 2);

            candyS3.abortMultipartUpload(bucket, objectKey1, objectKey1UploadId2, new AbortMultipartUploadOptions());
            candyS3.abortMultipartUpload(bucket, objectKey2, objectKey2UploadId, new AbortMultipartUploadOptions());

            uploads = candyS3.listMultipartUploads(bucket, new ListMultipartUploadOptions());
            Assertions.assertEquals(uploads.getResults().size(), initialCnt);
        } finally {
            candyS3.deleteBucket(bucket);
        }
    }

    @Test
    @Tag(TestTag.basic)
    void listMultipartUploadsPaginationTest() throws IOException, NoSuchAlgorithmException {
        CandyS3 candyS3 = init(provider());
        String bucket = genTestBucketName("lMpUPTest");

        List<String> createdObjectKeys = new ArrayList<>();
        List<String> createdUploadIds = new ArrayList<>();
        List<Map<String, String>> objectKeyUploadIdMaps = new ArrayList<>();

        try {
            candyS3.createBucket(new CreateBucketOptions.CreateBucketOptionsBuilder(bucket).build());

            int initialCount = candyS3.listMultipartUploads(bucket, new ListMultipartUploadOptions()).getResults().size();


            for (int i = 0; i < 11; i++) {
                String objectKey = "listMultipartUploadsPaginationTest-" + i + ".data";
                candyS3.putObject(bucket, objectKey, new PutObjectOptions.PutObjectOptionsBuilder().build());
                createdObjectKeys.add(objectKey);
                for (int j = 0; j < 11 % 3; j++) {
                    String uploadId = candyS3.createMultipartUpload(bucket, objectKey, new CreateMultipartUploadOptions.CreateMultipartUploadOptionsBuilder().build());
                    createdUploadIds.add(uploadId);

                    Map<String, String> objectKeyUploadIdMap = new HashMap<>();
                    objectKeyUploadIdMap.put("objectKey", objectKey);
                    objectKeyUploadIdMap.put("uploadId", uploadId);
                    objectKeyUploadIdMaps.add(objectKeyUploadIdMap);
                }
            }

            int newCount = candyS3.listMultipartUploads(bucket, new ListMultipartUploadOptions()).getResults().size();
            Assertions.assertEquals(newCount, initialCount + createdUploadIds.size());

            List<S3MultipartUpload> listUploadIds = new ArrayList<>();
            ListPaginationResult<S3MultipartUpload> result;
            ListMultipartUploadOptions listMultipartUploadOptions = new ListMultipartUploadOptions().maxUploads(3);

            int loopTime = 0;
            int lastCount = 0;
            do {
                result = candyS3.listMultipartUploads(bucket, listMultipartUploadOptions);
                listUploadIds.addAll(result.getResults());
                listMultipartUploadOptions.keyMarker(result.getNextPaginationMarker());
                listMultipartUploadOptions.uploadIdMarker(result.getNextPaginationMarker2());

                loopTime++;
                if (loopTime * 3 < newCount) {
                    Assertions.assertTrue(listUploadIds.size() > lastCount);
                    Assertions.assertTrue(StringUtils.isNotEmpty(result.getNextPaginationMarker()));
                    Assertions.assertTrue(StringUtils.isNotEmpty(result.getNextPaginationMarker2()));
                }
                lastCount = listUploadIds.size();
                // Ensure the loop can break
                if (loopTime > (newCount - initialCount) * 3) {
                    break;
                }
            } while (StringUtils.isNotEmpty(result.getNextPaginationMarker()) || StringUtils.isNotEmpty(result.getNextPaginationMarker2()));

            Assertions.assertEquals(newCount, listUploadIds.size());
            Assertions.assertTrue(StringUtils.isEmpty(listMultipartUploadOptions.getKeyMarker()));
            Assertions.assertTrue(StringUtils.isEmpty(listMultipartUploadOptions.getUploadIdMarker()));

        } finally {
            for (Map<String, String> map : objectKeyUploadIdMaps) {
                candyS3.abortMultipartUpload(bucket, map.get("objectKey"), map.get("uploadId"), new AbortMultipartUploadOptions());
            }
            candyS3.deleteObjectsBatch(bucket, new DeleteObjectsBatchOptions.DeleteObjectsBatchOptionsBuilder()
                    .addDeleteObjects(createdObjectKeys)
                    .build());
            candyS3.deleteBucket(bucket);
        }
    }

    @Test
    @Tag(TestTag.basic)
    void listPartsTest() throws IOException, NoSuchAlgorithmException {
        CandyS3 candyS3 = init(provider());
        String bucket = genTestBucketName("listPartsTest");
        try {
            candyS3.createBucket(new CreateBucketOptions.CreateBucketOptionsBuilder(bucket).build());

            String objectKey = "listPartsTest1.data";

            String objectKeyUploadId = candyS3.createMultipartUpload(bucket, objectKey, new CreateMultipartUploadOptions.CreateMultipartUploadOptionsBuilder().build());

            ListPaginationResult<S3Part> s3Parts = candyS3.listParts(bucket, objectKey, new ListPartsOptions(objectKeyUploadId));
            int initialCnt = s3Parts.getResults().size();
            Assertions.assertEquals(initialCnt, 0);

            for (int i = 0; i < 3; i++) {
                candyS3.uploadPart(bucket, objectKey, objectKeyUploadId, i + 1, new UploadPartOptions.UploadPartOptionsBuilder()
                        .configureUploadData().withData(new byte[]{(byte) i}).endConfigureDataContent()
                        .build());
            }

            s3Parts = candyS3.listParts(bucket, objectKey, new ListPartsOptions(objectKeyUploadId));
            Assertions.assertEquals(s3Parts.getResults().size(), initialCnt + 3);

            candyS3.abortMultipartUpload(bucket, objectKey, objectKeyUploadId, new AbortMultipartUploadOptions());
        } finally {
            candyS3.deleteBucket(bucket);
        }
    }

    @Test
    @Tag(TestTag.basic)
    void listPartsPaginationTest() throws IOException, NoSuchAlgorithmException {
        CandyS3 candyS3 = init(provider());
        String bucket = genTestBucketName("listPartsPaginationTest");
        try {
            candyS3.createBucket(new CreateBucketOptions.CreateBucketOptionsBuilder(bucket).build());

            String objectKey = "listPartsPaginationTest.data";
            String objectKeyUploadId = candyS3.createMultipartUpload(bucket, objectKey, new CreateMultipartUploadOptions.CreateMultipartUploadOptionsBuilder().build());

            int initialCount = candyS3.listParts(bucket, objectKey, new ListPartsOptions(objectKeyUploadId)).getResults().size();

            List<Integer> createdPartNums = new ArrayList<>();
            for (int i = 0; i < 10; i++) {
                S3Part part = candyS3.uploadPart(bucket, objectKey, objectKeyUploadId, i + 1, new UploadPartOptions.UploadPartOptionsBuilder()
                        .configureUploadData().withData(new byte[]{(byte) i}).endConfigureDataContent()
                        .build());
                createdPartNums.add(part.getPartNum());
            }

            int newCount = candyS3.listParts(bucket, objectKey, new ListPartsOptions(objectKeyUploadId)).getResults().size();
            Assertions.assertEquals(newCount, initialCount + createdPartNums.size());

            List<S3Part> listParts = new ArrayList<>();
            ListPaginationResult<S3Part> result;
            ListPartsOptions listPartsOptions = new ListPartsOptions(objectKeyUploadId).maxParts(3);

            int loopTime = 0;
            int lastCount = 0;
            do {
                result = candyS3.listParts(bucket, objectKey, listPartsOptions);
                listParts.addAll(result.getResults());
                listPartsOptions.startAfterPartNumber(result.getNextPaginationMarker());

                loopTime++;
                if (loopTime * 3 < newCount) {
                    Assertions.assertTrue(listParts.size() > lastCount);
                    Assertions.assertTrue(StringUtils.isNotEmpty(result.getNextPaginationMarker()));
                }
                lastCount = listParts.size();
            } while (StringUtils.isNotEmpty(result.getNextPaginationMarker()));

            int exceptedLoopTime = newCount % 3 == 0 ? newCount / 3 : newCount / 3 + 1;
            Assertions.assertEquals(exceptedLoopTime, loopTime);
            Assertions.assertEquals(newCount, listParts.size());
            Assertions.assertNull(listPartsOptions.getStartAfterPartNumber());

            candyS3.abortMultipartUpload(bucket, objectKey, objectKeyUploadId, new AbortMultipartUploadOptions());
        } finally {
            candyS3.deleteBucket(bucket);
        }
    }

    @Test
    @Tag(TestTag.basic)
    void putObjectWithNonEnglishKeyTest() throws IOException, NoSuchAlgorithmException {
        CandyS3 candyS3 = init(provider());
        String bucket = genTestBucketName("putOWNonEnKeyTest");
        String objectKey = "我の😯file.data";
        try {
            candyS3.createBucket(new CreateBucketOptions.CreateBucketOptionsBuilder(bucket).build());
            candyS3.putObject(bucket, objectKey, new PutObjectOptions.PutObjectOptionsBuilder().build());
            ListPaginationResult<S3Object> result =
                    candyS3.listObjects(bucket, new ListObjectOptions().prefix(objectKey).maxKeys(1));
            Assertions.assertEquals(result.getResults().get(0).getKey(), objectKey);
        } finally {
            candyS3.deleteObject(bucket, new DeleteObjectOptions(objectKey));
            candyS3.deleteBucket(bucket);
        }
    }

    @Test
    @Tag(TestTag.conditional)
    void putObjectConditionalTest() throws IOException, NoSuchAlgorithmException {
        CandyS3 candyS3 = init(provider());
        String bucket = genTestBucketName("putObjectConditionalTest");
        String objectKey = "putObjectConditionalTest.data";
        try {
            candyS3.createBucket(new CreateBucketOptions.CreateBucketOptionsBuilder(bucket).build());
            candyS3.putObject(bucket, objectKey, new PutObjectOptions.PutObjectOptionsBuilder()
                    .configureUploadData().withData("x".getBytes(StandardCharsets.UTF_8)).endConfigureDataContent()
                    .configureConditionalWrite().ifNotExists().endConfigureCondition()
                    .build());
            ListPaginationResult<S3Object> result =
                    candyS3.listObjects(bucket, new ListObjectOptions().prefix(objectKey).maxKeys(1));
            Assertions.assertEquals(result.getResults().get(0).getKey(), objectKey);

            try {
                candyS3.putObject(bucket, objectKey, new PutObjectOptions.PutObjectOptionsBuilder()
                        .configureConditionalWrite().ifNotExists().endConfigureCondition()
                        .build());
                Assertions.fail("Should not be here. PreconditionFailed should be thrown.");
            } catch (Exception ex) {
                Assertions.assertTrue(ex instanceof CandyS3Exception);
                Assertions.assertEquals(CommonErrorCode.OBJECT_PRECONDITION_FAILED.getCode(), ((CandyS3Exception) ex).getCode());
                Assertions.assertEquals("PreconditionFailed", ((CandyS3Exception) ex).getParsedError().getCode());
            }

            String oldEtag = result.getResults().get(0).geteTag();

            candyS3.putObject(bucket, objectKey, new PutObjectOptions.PutObjectOptionsBuilder()
                    .configureUploadData().withData("y".getBytes(StandardCharsets.UTF_8)).endConfigureDataContent()
                    .configureConditionalWrite().ifMatch(oldEtag).endConfigureCondition()
                    .build());
            ListPaginationResult<S3Object> result2 =
                    candyS3.listObjects(bucket, new ListObjectOptions().prefix(objectKey).maxKeys(1));
            Assertions.assertNotEquals(oldEtag, result2.getResults().get(0).geteTag());
            try {
                candyS3.putObject(bucket, objectKey, new PutObjectOptions.PutObjectOptionsBuilder()
                        .configureConditionalWrite().ifMatch(oldEtag).endConfigureCondition()
                        .build());
                Assertions.fail("Should not be here. PreconditionFailed should be thrown.");
            } catch (Exception ex) {
                Assertions.assertTrue(ex instanceof CandyS3Exception);
                Assertions.assertEquals(CommonErrorCode.OBJECT_PRECONDITION_FAILED.getCode(), ((CandyS3Exception) ex).getCode());
                Assertions.assertEquals("PreconditionFailed", ((CandyS3Exception) ex).getParsedError().getCode());
            }

        } finally {
            candyS3.deleteObject(bucket, new DeleteObjectOptions(objectKey));
            candyS3.deleteBucket(bucket);
        }
    }

    @Test
    @Tag(TestTag.conditional)
    void multipartUploadConditionalTest() throws IOException, NoSuchAlgorithmException {
        CandyS3 candyS3 = init(provider());
        String bucket = genTestBucketName("mpUCTest");
        String objectKey = "multipartUploadConditionalTest.data";
        try {
            candyS3.createBucket(new CreateBucketOptions.CreateBucketOptionsBuilder(bucket).build());

            String uploadId1 = candyS3.createMultipartUpload(bucket, objectKey,
                    new CreateMultipartUploadOptions.CreateMultipartUploadOptionsBuilder().build());
            S3Part part1 = candyS3.uploadPart(bucket, objectKey, uploadId1, 1,
                    new UploadPartOptions.UploadPartOptionsBuilder()
                            .configureUploadData()
                            .withData("x".getBytes(StandardCharsets.UTF_8))
                            .endConfigureDataContent()
                            .build());
            candyS3.completeMultipartUpload(bucket, objectKey, uploadId1, Arrays.asList(part1),
                    new CompleteMultipartUploadOptions.CompleteMultipartUploadOptionsBuilder()
                            .configureCondition().ifNotExists().endConfigureCondition()
                            .build());
            ListPaginationResult<S3Object> result =
                    candyS3.listObjects(bucket, new ListObjectOptions().prefix(objectKey).maxKeys(1));
            Assertions.assertEquals(result.getResults().get(0).getKey(), objectKey);

            try {
                String uploadId2 = candyS3.createMultipartUpload(bucket, objectKey,
                        new CreateMultipartUploadOptions.CreateMultipartUploadOptionsBuilder().build());
                S3Part part2 = candyS3.uploadPart(bucket, objectKey, uploadId2, 1,
                        new UploadPartOptions.UploadPartOptionsBuilder()
                                .configureUploadData()
                                .withData("x".getBytes(StandardCharsets.UTF_8))
                                .endConfigureDataContent()
                                .build());
                candyS3.completeMultipartUpload(bucket, objectKey, uploadId2, Arrays.asList(part2),
                        new CompleteMultipartUploadOptions.CompleteMultipartUploadOptionsBuilder()
                                .configureCondition().ifNotExists().endConfigureCondition()
                                .build());
                Assertions.fail("Should not be here. PreconditionFailed should be thrown.");
            } catch (Exception ex) {
                Assertions.assertTrue(ex instanceof CandyS3Exception);
                Assertions.assertEquals(CommonErrorCode.OBJECT_PRECONDITION_FAILED.getCode(), ((CandyS3Exception) ex).getCode());
                Assertions.assertEquals("PreconditionFailed", ((CandyS3Exception) ex).getParsedError().getCode());
            }

            String oldEtag = result.getResults().get(0).geteTag();

            String uploadId3 = candyS3.createMultipartUpload(bucket, objectKey,
                    new CreateMultipartUploadOptions.CreateMultipartUploadOptionsBuilder().build());
            S3Part part3 = candyS3.uploadPart(bucket, objectKey, uploadId3, 1,
                    new UploadPartOptions.UploadPartOptionsBuilder()
                            .configureUploadData()
                            .withData("y".getBytes(StandardCharsets.UTF_8))
                            .endConfigureDataContent()
                            .build());
            candyS3.completeMultipartUpload(bucket, objectKey, uploadId3, Arrays.asList(part3),
                    new CompleteMultipartUploadOptions.CompleteMultipartUploadOptionsBuilder()
                            .configureCondition().ifMatch(oldEtag).endConfigureCondition()
                            .build());

            ListPaginationResult<S3Object> result2 =
                    candyS3.listObjects(bucket, new ListObjectOptions().prefix(objectKey).maxKeys(1));
            Assertions.assertNotEquals(oldEtag, result2.getResults().get(0).geteTag());
            try {

                String uploadId4 = candyS3.createMultipartUpload(bucket, objectKey,
                        new CreateMultipartUploadOptions.CreateMultipartUploadOptionsBuilder().build());
                S3Part part4 = candyS3.uploadPart(bucket, objectKey, uploadId4, 1,
                        new UploadPartOptions.UploadPartOptionsBuilder()
                                .configureUploadData()
                                .withData("y".getBytes(StandardCharsets.UTF_8))
                                .endConfigureDataContent()
                                .build());
                candyS3.completeMultipartUpload(bucket, objectKey, uploadId4, Arrays.asList(part4),
                        new CompleteMultipartUploadOptions.CompleteMultipartUploadOptionsBuilder()
                                .configureCondition().ifMatch(oldEtag).endConfigureCondition()
                                .build());
                Assertions.fail("Should not be here. PreconditionFailed should be thrown.");
            } catch (Exception ex) {
                Assertions.assertTrue(ex instanceof CandyS3Exception);
                Assertions.assertEquals(CommonErrorCode.OBJECT_PRECONDITION_FAILED.getCode(), ((CandyS3Exception) ex).getCode());
                Assertions.assertEquals("PreconditionFailed", ((CandyS3Exception) ex).getParsedError().getCode());
            }

        } finally {
            candyS3.deleteObject(bucket, new DeleteObjectOptions(objectKey));
            candyS3.deleteBucket(bucket);
        }
    }

    @Test
    @Tag(TestTag.basic)
    void putObjectPropertiesTest() throws IOException, NoSuchAlgorithmException {
        CandyS3 candyS3 = init(provider());
        String bucket = genTestBucketName("putObjectPropertiesTest");
        String objectKey = "putObjectPropertiesTest.data";
        byte[] bytes = "x我y".getBytes(StandardCharsets.UTF_8);
        try {
            candyS3.createBucket(new CreateBucketOptions.CreateBucketOptionsBuilder(bucket).build());

            Date expiresDate = new Date(System.currentTimeMillis() + 1000 * 60 * 2);
            candyS3.putObject(bucket, objectKey, new PutObjectOptions.PutObjectOptionsBuilder()
                    .configureUploadData().withData(bytes).endConfigureDataContent()
                    .configurePutObjectHeaderOptions()
                    .cacheControl("public, max-age=3600")
                    .contentDisposition("attachment; filename=\"thisisaattachment.html\"")
                    .contentEncoding("gzip, deflate")
                    .contentLanguage("en, zh-CN")
                    .contentType("text/html")
                    .expires(expiresDate)
                    .endConfigurePutObjectHeaderOptions()
                    .build());

            // getObjectMetadata
            {
                S3Object s3Object = candyS3.getObjectMetadata(bucket, objectKey, new DownloadObjectOptions.DownloadObjectOptionsBuilder().build());
                S3Object.S3ObjectMetadata objectMetadata = s3Object.getObjectMetadata();

                Assertions.assertNull(s3Object.getContentBytes());
                Assertions.assertNotNull(s3Object.getObjectMetadata());

                Assertions.assertEquals(s3Object.getSize(), "x我y".getBytes(StandardCharsets.UTF_8).length);

                Assertions.assertEquals(objectMetadata.getCacheControl(), "public, max-age=3600");
                Assertions.assertEquals(objectMetadata.getContentDisposition(), "attachment; filename=\"thisisaattachment.html\"");
                Assertions.assertEquals(objectMetadata.getContentEncoding(), "gzip, deflate");
                Assertions.assertEquals(objectMetadata.getContentLanguage(), "en, zh-CN");
                Assertions.assertEquals(objectMetadata.getContentType(), "text/html");
                Assertions.assertTrue(objectMetadata.getExpires().after(new Date(expiresDate.getTime() - 1000)));
                Assertions.assertTrue(objectMetadata.getExpires().before(new Date(expiresDate.getTime() + 1000)));
            }

            // getObjectMetadata with range
            {
                S3Object s3Object = candyS3.getObjectMetadata(bucket, objectKey, new DownloadObjectOptions.DownloadObjectOptionsBuilder()
                        .range(1, bytes.length - 2).build());
                S3Object.S3ObjectMetadata objectMetadata = s3Object.getObjectMetadata();

                Assertions.assertNull(s3Object.getContentBytes());
                Assertions.assertNotNull(s3Object.getObjectMetadata());

                Assertions.assertEquals(s3Object.getSize(), (bytes.length - 2) - 1 + 1);

                Assertions.assertEquals(objectMetadata.getCacheControl(), "public, max-age=3600");
                Assertions.assertEquals(objectMetadata.getContentDisposition(), "attachment; filename=\"thisisaattachment.html\"");
                Assertions.assertEquals(objectMetadata.getContentEncoding(), "gzip, deflate");
                Assertions.assertEquals(objectMetadata.getContentLanguage(), "en, zh-CN");
                Assertions.assertEquals(objectMetadata.getContentRange(), "bytes 1-" + (bytes.length - 2) + "/" + (bytes.length));
                Assertions.assertEquals(objectMetadata.getContentType(), "text/html");
                Assertions.assertTrue(objectMetadata.getExpires().after(new Date(expiresDate.getTime() - 1000)));
                Assertions.assertTrue(objectMetadata.getExpires().before(new Date(expiresDate.getTime() + 1000)));
            }

            // getObjectMetadata with condition
            {
                ListPaginationResult<S3Object> result =
                        candyS3.listObjects(bucket, new ListObjectOptions().prefix(objectKey).maxKeys(1));
                {
                    try {
                        candyS3.getObjectMetadata(bucket, objectKey,
                                new DownloadObjectOptions.DownloadObjectOptionsBuilder()
                                        .configureDownloadCondition().ifMatch("x").endConfigureCondition()
                                        .build());
                        Assertions.fail("Should not be here. PreconditionFailed should be thrown.");
                    } catch (Exception ex) {
                        Assertions.assertTrue(ex instanceof CandyS3Exception);
                        Assertions.assertEquals(CommonErrorCode.OBJECT_PRECONDITION_FAILED.getCode(), ((CandyS3Exception) ex).getCode());
                        Assertions.assertEquals("PreconditionFailed", ((CandyS3Exception) ex).getParsedError().getCode());
                    }
                    S3Object s3Object = candyS3.getObjectMetadata(bucket, objectKey,
                            new DownloadObjectOptions.DownloadObjectOptionsBuilder()
                                    .configureDownloadCondition().ifMatch(result.getResults().get(0).geteTag()).endConfigureCondition()
                                    .build());
                    S3Object.S3ObjectMetadata objectMetadata = s3Object.getObjectMetadata();

                    Assertions.assertNull(s3Object.getContentBytes());
                    Assertions.assertNotNull(s3Object.getObjectMetadata());

                    Assertions.assertEquals(s3Object.getSize(), bytes.length);

                    Assertions.assertEquals(objectMetadata.getCacheControl(), "public, max-age=3600");
                    Assertions.assertEquals(objectMetadata.getContentDisposition(), "attachment; filename=\"thisisaattachment.html\"");
                    Assertions.assertEquals(objectMetadata.getContentEncoding(), "gzip, deflate");
                    Assertions.assertEquals(objectMetadata.getContentLanguage(), "en, zh-CN");
                    Assertions.assertNull(objectMetadata.getContentRange());
                    Assertions.assertEquals(objectMetadata.getContentType(), "text/html");
                    Assertions.assertTrue(objectMetadata.getExpires().after(new Date(expiresDate.getTime() - 1000)));
                    Assertions.assertTrue(objectMetadata.getExpires().before(new Date(expiresDate.getTime() + 1000)));
                }

                {
                    try {
                        candyS3.getObjectMetadata(bucket, objectKey, new DownloadObjectOptions.DownloadObjectOptionsBuilder()
                                .configureDownloadCondition().ifNoneMatch(result.getResults().get(0).geteTag()).endConfigureCondition()
                                .build());
                        Assertions.fail("Should not be here. PreconditionFailed should be thrown.");
                    } catch (Exception ex) {
                        Assertions.assertTrue(ex instanceof CandyS3Exception);
                        Assertions.assertEquals(CommonErrorCode.OBJECT_NOT_MODIFIED.getCode(), ((CandyS3Exception) ex).getCode());
                    }
                    S3Object s3Object = candyS3.getObjectMetadata(bucket, objectKey,
                            new DownloadObjectOptions.DownloadObjectOptionsBuilder()
                                    .configureDownloadCondition().ifNoneMatch("x").endConfigureCondition()
                                    .build());
                    S3Object.S3ObjectMetadata objectMetadata = s3Object.getObjectMetadata();

                    Assertions.assertNull(s3Object.getContentBytes());
                    Assertions.assertNotNull(s3Object.getObjectMetadata());

                    Assertions.assertEquals(s3Object.getSize(), bytes.length);

                    Assertions.assertEquals(objectMetadata.getCacheControl(), "public, max-age=3600");
                    Assertions.assertEquals(objectMetadata.getContentDisposition(), "attachment; filename=\"thisisaattachment.html\"");
                    Assertions.assertEquals(objectMetadata.getContentEncoding(), "gzip, deflate");
                    Assertions.assertEquals(objectMetadata.getContentLanguage(), "en, zh-CN");
                    Assertions.assertNull(objectMetadata.getContentRange());
                    Assertions.assertEquals(objectMetadata.getContentType(), "text/html");
                    Assertions.assertTrue(objectMetadata.getExpires().after(new Date(expiresDate.getTime() - 1000)));
                    Assertions.assertTrue(objectMetadata.getExpires().before(new Date(expiresDate.getTime() + 1000)));
                }

                {
                    try {
                        candyS3.getObjectMetadata(bucket, objectKey, new DownloadObjectOptions.DownloadObjectOptionsBuilder()
                                .configureDownloadCondition().ifModifiedSince(result.getResults().get(0).getLastModified()).endConfigureCondition()
                                .build());
                        Assertions.fail("Should not be here. PreconditionFailed should be thrown.");
                    } catch (Exception ex) {
                        Assertions.assertTrue(ex instanceof CandyS3Exception);
                        Assertions.assertEquals(CommonErrorCode.OBJECT_NOT_MODIFIED.getCode(), ((CandyS3Exception) ex).getCode());
                    }
                    S3Object s3Object = candyS3.getObjectMetadata(bucket, objectKey,
                            new DownloadObjectOptions.DownloadObjectOptionsBuilder()
                                    .configureDownloadCondition().ifModifiedSince(new Date(result.getResults().get(0).getLastModified().getTime() - 1000)).endConfigureCondition()
                                    .build());
                    S3Object.S3ObjectMetadata objectMetadata = s3Object.getObjectMetadata();

                    Assertions.assertNull(s3Object.getContentBytes());
                    Assertions.assertNotNull(s3Object.getObjectMetadata());

                    Assertions.assertEquals(s3Object.getSize(), bytes.length);

                    Assertions.assertEquals(objectMetadata.getCacheControl(), "public, max-age=3600");
                    Assertions.assertEquals(objectMetadata.getContentDisposition(), "attachment; filename=\"thisisaattachment.html\"");
                    Assertions.assertEquals(objectMetadata.getContentEncoding(), "gzip, deflate");
                    Assertions.assertEquals(objectMetadata.getContentLanguage(), "en, zh-CN");
                    Assertions.assertNull(objectMetadata.getContentRange());
                    Assertions.assertEquals(objectMetadata.getContentType(), "text/html");
                    Assertions.assertTrue(objectMetadata.getExpires().after(new Date(expiresDate.getTime() - 1000)));
                    Assertions.assertTrue(objectMetadata.getExpires().before(new Date(expiresDate.getTime() + 1000)));
                }

                {
                    try {
                        candyS3.getObjectMetadata(bucket, objectKey, new DownloadObjectOptions.DownloadObjectOptionsBuilder()
                                .configureDownloadCondition().ifUnmodifiedSince(new Date(result.getResults().get(0).getLastModified().getTime() - 1000)).endConfigureCondition()
                                .build());
                        Assertions.fail("Should not be here. PreconditionFailed should be thrown.");
                    } catch (Exception ex) {
                        Assertions.assertTrue(ex instanceof CandyS3Exception);
                        Assertions.assertEquals(CommonErrorCode.OBJECT_PRECONDITION_FAILED.getCode(), ((CandyS3Exception) ex).getCode());
                    }
                    S3Object s3Object = candyS3.getObjectMetadata(bucket, objectKey,
                            new DownloadObjectOptions.DownloadObjectOptionsBuilder()
                                    .configureDownloadCondition().ifUnmodifiedSince(new Date(result.getResults().get(0).getLastModified().getTime() + 1)).endConfigureCondition()
                                    .build());
                    S3Object.S3ObjectMetadata objectMetadata = s3Object.getObjectMetadata();

                    Assertions.assertNull(s3Object.getContentBytes());
                    Assertions.assertNotNull(s3Object.getObjectMetadata());

                    Assertions.assertEquals(s3Object.getSize(), bytes.length);

                    Assertions.assertEquals(objectMetadata.getCacheControl(), "public, max-age=3600");
                    Assertions.assertEquals(objectMetadata.getContentDisposition(), "attachment; filename=\"thisisaattachment.html\"");
                    Assertions.assertEquals(objectMetadata.getContentEncoding(), "gzip, deflate");
                    Assertions.assertEquals(objectMetadata.getContentLanguage(), "en, zh-CN");
                    Assertions.assertNull(objectMetadata.getContentRange());
                    Assertions.assertEquals(objectMetadata.getContentType(), "text/html");
                    Assertions.assertTrue(objectMetadata.getExpires().after(new Date(expiresDate.getTime() - 1000)));
                    Assertions.assertTrue(objectMetadata.getExpires().before(new Date(expiresDate.getTime() + 1000)));
                }
            }

        } finally {
            deleteAllObjectVersions(provider(), bucket);
            candyS3.deleteBucket(bucket);
        }
    }

    @Test
    @Tag(TestTag.basic)
    void multipartUploadPropertiesTest() throws IOException, NoSuchAlgorithmException {
        CandyS3 candyS3 = init(provider());
        String bucket = genTestBucketName("multipartUploadPropertiesTest");
        String objectKey = "multipartUploadPropertiesTest.data";
        try {
            candyS3.createBucket(new CreateBucketOptions.CreateBucketOptionsBuilder(bucket).build());

            Date expiresDate = new Date(System.currentTimeMillis() + 1000 * 60 * 2);

            String uploadId = candyS3.createMultipartUpload(bucket, objectKey, new CreateMultipartUploadOptions.CreateMultipartUploadOptionsBuilder()
                    .configurePutObjectHeaderOptions()
                    .cacheControl("public, max-age=3600")
                    .contentDisposition("attachment; filename=\"thisisaattachment.html\"")
                    .contentEncoding("gzip, deflate")
                    .contentLanguage("en, zh-CN")
                    .contentType("text/html")
                    .expires(expiresDate)
                    .endConfigureObjectHeaderOptions()
                    .build());

            byte[] bytes1 = StringUtils.repeat('x', 6 * 1024 * 1024).getBytes(StandardCharsets.UTF_8);
            S3Part part1 = candyS3.uploadPart(bucket, objectKey, uploadId, 1, new UploadPartOptions.UploadPartOptionsBuilder()
                    .configureUploadData().withData(bytes1).endConfigureDataContent()
                    .build());

            byte[] bytes2 = new byte[]{1};
            S3Part part2 = candyS3.uploadPart(bucket, objectKey, uploadId, 2, new UploadPartOptions.UploadPartOptionsBuilder()
                    .configureUploadData().withData(bytes2).endConfigureDataContent()
                    .build());

            candyS3.completeMultipartUpload(bucket, objectKey, uploadId, Arrays.asList(part1, part2),
                    new CompleteMultipartUploadOptions.CompleteMultipartUploadOptionsBuilder()
                            .build());

            // getObjectMetadata
            {
                S3Object s3Object = candyS3.getObjectMetadata(bucket, objectKey, new DownloadObjectOptions.DownloadObjectOptionsBuilder()
                        .build());
                S3Object.S3ObjectMetadata objectMetadata = s3Object.getObjectMetadata();

                Assertions.assertNull(s3Object.getContentBytes());
                Assertions.assertNotNull(s3Object.getObjectMetadata());

                Assertions.assertEquals(s3Object.getSize(), bytes1.length + bytes2.length);

                if (S3Provider.CLOUDFLARE_R2.equals(provider())) {
                    Assertions.assertEquals(2, s3Object.getPartsCount().intValue());
                } else {
                    Assertions.assertNull(s3Object.getPartsCount());
                }

                Assertions.assertEquals(objectMetadata.getCacheControl(), "public, max-age=3600");
                Assertions.assertEquals(objectMetadata.getContentDisposition(), "attachment; filename=\"thisisaattachment.html\"");
                Assertions.assertEquals(objectMetadata.getContentEncoding(), "gzip, deflate");
                Assertions.assertEquals(objectMetadata.getContentLanguage(), "en, zh-CN");
                Assertions.assertNull(objectMetadata.getContentRange());
                Assertions.assertEquals(objectMetadata.getContentType(), "text/html");
                Assertions.assertTrue(objectMetadata.getExpires().after(new Date(expiresDate.getTime() - 1000)));
                Assertions.assertTrue(objectMetadata.getExpires().before(new Date(expiresDate.getTime() + 1000)));
            }

            // getObjectMetadata with partNumber
            {
                S3Object s3Object = candyS3.getObjectMetadata(bucket, objectKey, new DownloadObjectOptions.DownloadObjectOptionsBuilder()
                        .partNumber(2).build());
                S3Object.S3ObjectMetadata objectMetadata = s3Object.getObjectMetadata();

                Assertions.assertNull(s3Object.getContentBytes());
                Assertions.assertNotNull(s3Object.getObjectMetadata());

                Assertions.assertEquals(s3Object.getSize(), bytes2.length);

                Assertions.assertEquals(s3Object.getPartsCount(), (Integer) 2);

                Assertions.assertEquals(objectMetadata.getCacheControl(), "public, max-age=3600");
                Assertions.assertEquals(objectMetadata.getContentDisposition(), "attachment; filename=\"thisisaattachment.html\"");
                Assertions.assertEquals(objectMetadata.getContentEncoding(), "gzip, deflate");
                Assertions.assertEquals(objectMetadata.getContentLanguage(), "en, zh-CN");
                Assertions.assertEquals(objectMetadata.getContentRange(), "bytes " + bytes1.length + "-" + (bytes1.length + bytes2.length - 1) + "/" + (bytes1.length + bytes2.length));
                Assertions.assertEquals(objectMetadata.getContentType(), "text/html");
                Assertions.assertTrue(objectMetadata.getExpires().after(new Date(expiresDate.getTime() - 1000)));
                Assertions.assertTrue(objectMetadata.getExpires().before(new Date(expiresDate.getTime() + 1000)));
            }

            // getObjectMetadata with range
            {
                S3Object s3Object = candyS3.getObjectMetadata(bucket, objectKey, new DownloadObjectOptions.DownloadObjectOptionsBuilder()
                        .range(bytes1.length - 10, bytes1.length).build());
                S3Object.S3ObjectMetadata objectMetadata = s3Object.getObjectMetadata();

                Assertions.assertNull(s3Object.getContentBytes());
                Assertions.assertNotNull(s3Object.getObjectMetadata());

                Assertions.assertEquals(s3Object.getSize(), bytes1.length - (bytes1.length - 10) + 1);

                if (S3Provider.CLOUDFLARE_R2.equals(provider())) {
                    Assertions.assertEquals(2, s3Object.getPartsCount().intValue());
                } else {
                    Assertions.assertNull(s3Object.getPartsCount());
                }

                Assertions.assertEquals(objectMetadata.getCacheControl(), "public, max-age=3600");
                Assertions.assertEquals(objectMetadata.getContentDisposition(), "attachment; filename=\"thisisaattachment.html\"");
                Assertions.assertEquals(objectMetadata.getContentEncoding(), "gzip, deflate");
                Assertions.assertEquals(objectMetadata.getContentLanguage(), "en, zh-CN");
                Assertions.assertEquals(objectMetadata.getContentRange(), "bytes " + (bytes1.length - 10) + "-" + (bytes1.length) + "/" + (bytes1.length + bytes2.length));
                Assertions.assertEquals(objectMetadata.getContentType(), "text/html");
                Assertions.assertTrue(objectMetadata.getExpires().after(new Date(expiresDate.getTime() - 1000)));
                Assertions.assertTrue(objectMetadata.getExpires().before(new Date(expiresDate.getTime() + 1000)));
            }

        } finally {
            deleteAllObjectVersions(provider(), bucket);
            candyS3.deleteBucket(bucket);
        }
    }

    @Test
    @Tag(TestTag.conditional)
    void multipartUploadPropertiesWithConditionTest() throws IOException, NoSuchAlgorithmException {
        CandyS3 candyS3 = init(provider());
        String bucket = genTestBucketName("mpUploadPropWithCondTest");
        String objectKey = "mpUploadPropWithCondTest.data";
        try {
            candyS3.createBucket(new CreateBucketOptions.CreateBucketOptionsBuilder(bucket).build());

            Date expiresDate = new Date(System.currentTimeMillis() + 1000 * 60 * 2);

            String uploadId = candyS3.createMultipartUpload(bucket, objectKey, new CreateMultipartUploadOptions.CreateMultipartUploadOptionsBuilder()
                    .configurePutObjectHeaderOptions()
                    .cacheControl("public, max-age=3600")
                    .contentDisposition("attachment; filename=\"thisisaattachment.html\"")
                    .contentEncoding("gzip, deflate")
                    .contentLanguage("en, zh-CN")
                    .contentType("text/html")
                    .expires(expiresDate)
                    .endConfigureObjectHeaderOptions()
                    .build());

            byte[] bytes1 = StringUtils.repeat('x', 6 * 1024 * 1024).getBytes(StandardCharsets.UTF_8);
            S3Part part1 = candyS3.uploadPart(bucket, objectKey, uploadId, 1, new UploadPartOptions.UploadPartOptionsBuilder()
                    .configureUploadData().withData(bytes1).endConfigureDataContent()
                    .build());

            byte[] bytes2 = new byte[]{1};
            S3Part part2 = candyS3.uploadPart(bucket, objectKey, uploadId, 2, new UploadPartOptions.UploadPartOptionsBuilder()
                    .configureUploadData().withData(bytes2).endConfigureDataContent()
                    .build());

            candyS3.completeMultipartUpload(bucket, objectKey, uploadId, Arrays.asList(part1, part2),
                    new CompleteMultipartUploadOptions.CompleteMultipartUploadOptionsBuilder()
                            .build());

            // getObjectMetadata with condition
            {
                ListPaginationResult<S3Object> result =
                        candyS3.listObjects(bucket, new ListObjectOptions().prefix(objectKey).maxKeys(1));
                {
                    try {
                        candyS3.getObjectMetadata(bucket, objectKey,
                                new DownloadObjectOptions.DownloadObjectOptionsBuilder()
                                        .configureDownloadCondition().ifMatch("x").endConfigureCondition()
                                        .build());
                        Assertions.fail("Should not be here. PreconditionFailed should be thrown.");
                    } catch (Exception ex) {
                        Assertions.assertTrue(ex instanceof CandyS3Exception);
                        Assertions.assertEquals(CommonErrorCode.OBJECT_PRECONDITION_FAILED.getCode(), ((CandyS3Exception) ex).getCode());
                        Assertions.assertEquals("PreconditionFailed", ((CandyS3Exception) ex).getParsedError().getCode());
                    }
                    S3Object s3Object = candyS3.getObjectMetadata(bucket, objectKey,
                            new DownloadObjectOptions.DownloadObjectOptionsBuilder()
                                    .configureDownloadCondition().ifMatch(result.getResults().get(0).geteTag()).endConfigureCondition()
                                    .build());
                    S3Object.S3ObjectMetadata objectMetadata = s3Object.getObjectMetadata();

                    Assertions.assertNull(s3Object.getContentBytes());
                    Assertions.assertNotNull(s3Object.getObjectMetadata());

                    Assertions.assertEquals(s3Object.getSize(), bytes1.length + bytes2.length);

                    if (S3Provider.CLOUDFLARE_R2.equals(provider())) {
                        Assertions.assertEquals(2, s3Object.getPartsCount().intValue());
                    } else {
                        Assertions.assertNull(s3Object.getPartsCount());
                    }

                    Assertions.assertEquals(objectMetadata.getCacheControl(), "public, max-age=3600");
                    Assertions.assertEquals(objectMetadata.getContentDisposition(), "attachment; filename=\"thisisaattachment.html\"");
                    Assertions.assertEquals(objectMetadata.getContentEncoding(), "gzip, deflate");
                    Assertions.assertEquals(objectMetadata.getContentLanguage(), "en, zh-CN");
                    Assertions.assertNull(objectMetadata.getContentRange());
                    Assertions.assertEquals(objectMetadata.getContentType(), "text/html");
                    Assertions.assertTrue(objectMetadata.getExpires().after(new Date(expiresDate.getTime() - 1000)));
                    Assertions.assertTrue(objectMetadata.getExpires().before(new Date(expiresDate.getTime() + 1000)));
                }

                {
                    try {
                        candyS3.getObjectMetadata(bucket, objectKey, new DownloadObjectOptions.DownloadObjectOptionsBuilder()
                                .configureDownloadCondition().ifNoneMatch(result.getResults().get(0).geteTag()).endConfigureCondition()
                                .build());
                        Assertions.fail("Should not be here. PreconditionFailed should be thrown.");
                    } catch (Exception ex) {
                        Assertions.assertTrue(ex instanceof CandyS3Exception);
                        Assertions.assertEquals(CommonErrorCode.OBJECT_NOT_MODIFIED.getCode(), ((CandyS3Exception) ex).getCode());
                    }
                    S3Object s3Object = candyS3.getObjectMetadata(bucket, objectKey,
                            new DownloadObjectOptions.DownloadObjectOptionsBuilder()
                                    .configureDownloadCondition().ifNoneMatch("x").endConfigureCondition()
                                    .build());
                    S3Object.S3ObjectMetadata objectMetadata = s3Object.getObjectMetadata();

                    Assertions.assertNull(s3Object.getContentBytes());
                    Assertions.assertNotNull(s3Object.getObjectMetadata());

                    Assertions.assertEquals(s3Object.getSize(), bytes1.length + bytes2.length);

                    if (S3Provider.CLOUDFLARE_R2.equals(provider())) {
                        Assertions.assertEquals(2, s3Object.getPartsCount().intValue());
                    } else {
                        Assertions.assertNull(s3Object.getPartsCount());
                    }

                    Assertions.assertEquals(objectMetadata.getCacheControl(), "public, max-age=3600");
                    Assertions.assertEquals(objectMetadata.getContentDisposition(), "attachment; filename=\"thisisaattachment.html\"");
                    Assertions.assertEquals(objectMetadata.getContentEncoding(), "gzip, deflate");
                    Assertions.assertEquals(objectMetadata.getContentLanguage(), "en, zh-CN");
                    Assertions.assertNull(objectMetadata.getContentRange());
                    Assertions.assertEquals(objectMetadata.getContentType(), "text/html");
                    Assertions.assertTrue(objectMetadata.getExpires().after(new Date(expiresDate.getTime() - 1000)));
                    Assertions.assertTrue(objectMetadata.getExpires().before(new Date(expiresDate.getTime() + 1000)));
                }

                {
                    try {
                        candyS3.getObjectMetadata(bucket, objectKey, new DownloadObjectOptions.DownloadObjectOptionsBuilder()
                                .configureDownloadCondition().ifModifiedSince(result.getResults().get(0).getLastModified()).endConfigureCondition()
                                .build());
                        Assertions.fail("Should not be here. PreconditionFailed should be thrown.");
                    } catch (Exception ex) {
                        Assertions.assertTrue(ex instanceof CandyS3Exception);
                        Assertions.assertEquals(CommonErrorCode.OBJECT_NOT_MODIFIED.getCode(), ((CandyS3Exception) ex).getCode());
                    }
                    S3Object s3Object = candyS3.getObjectMetadata(bucket, objectKey,
                            new DownloadObjectOptions.DownloadObjectOptionsBuilder()
                                    .configureDownloadCondition().ifModifiedSince(new Date(result.getResults().get(0).getLastModified().getTime() - 1000)).endConfigureCondition()
                                    .build());
                    S3Object.S3ObjectMetadata objectMetadata = s3Object.getObjectMetadata();

                    Assertions.assertNull(s3Object.getContentBytes());
                    Assertions.assertNotNull(s3Object.getObjectMetadata());

                    Assertions.assertEquals(s3Object.getSize(), bytes1.length + bytes2.length);

                    if (S3Provider.CLOUDFLARE_R2.equals(provider())) {
                        Assertions.assertEquals(2, s3Object.getPartsCount().intValue());
                    } else {
                        Assertions.assertNull(s3Object.getPartsCount());
                    }

                    Assertions.assertEquals(objectMetadata.getCacheControl(), "public, max-age=3600");
                    Assertions.assertEquals(objectMetadata.getContentDisposition(), "attachment; filename=\"thisisaattachment.html\"");
                    Assertions.assertEquals(objectMetadata.getContentEncoding(), "gzip, deflate");
                    Assertions.assertEquals(objectMetadata.getContentLanguage(), "en, zh-CN");
                    Assertions.assertNull(objectMetadata.getContentRange());
                    Assertions.assertEquals(objectMetadata.getContentType(), "text/html");
                    Assertions.assertTrue(objectMetadata.getExpires().after(new Date(expiresDate.getTime() - 1000)));
                    Assertions.assertTrue(objectMetadata.getExpires().before(new Date(expiresDate.getTime() + 1000)));
                }

                {
                    try {
                        candyS3.getObjectMetadata(bucket, objectKey, new DownloadObjectOptions.DownloadObjectOptionsBuilder()
                                .configureDownloadCondition().ifUnmodifiedSince(new Date(result.getResults().get(0).getLastModified().getTime() - 1000)).endConfigureCondition()
                                .build());
                        Assertions.fail("Should not be here. PreconditionFailed should be thrown.");
                    } catch (Exception ex) {
                        Assertions.assertTrue(ex instanceof CandyS3Exception);
                        Assertions.assertEquals(CommonErrorCode.OBJECT_PRECONDITION_FAILED.getCode(), ((CandyS3Exception) ex).getCode());
                    }
                    S3Object s3Object = candyS3.getObjectMetadata(bucket, objectKey,
                            new DownloadObjectOptions.DownloadObjectOptionsBuilder()
                                    .configureDownloadCondition().ifUnmodifiedSince(new Date(result.getResults().get(0).getLastModified().getTime() + 1)).endConfigureCondition()
                                    .build());
                    S3Object.S3ObjectMetadata objectMetadata = s3Object.getObjectMetadata();

                    Assertions.assertNull(s3Object.getContentBytes());
                    Assertions.assertNotNull(s3Object.getObjectMetadata());

                    Assertions.assertEquals(s3Object.getSize(), bytes1.length + bytes2.length);

                    if (S3Provider.CLOUDFLARE_R2.equals(provider())) {
                        Assertions.assertEquals(2, s3Object.getPartsCount().intValue());
                    } else {
                        Assertions.assertNull(s3Object.getPartsCount());
                    }

                    Assertions.assertEquals(objectMetadata.getCacheControl(), "public, max-age=3600");
                    Assertions.assertEquals(objectMetadata.getContentDisposition(), "attachment; filename=\"thisisaattachment.html\"");
                    Assertions.assertEquals(objectMetadata.getContentEncoding(), "gzip, deflate");
                    Assertions.assertEquals(objectMetadata.getContentLanguage(), "en, zh-CN");
                    Assertions.assertNull(objectMetadata.getContentRange());
                    Assertions.assertEquals(objectMetadata.getContentType(), "text/html");
                    Assertions.assertTrue(objectMetadata.getExpires().after(new Date(expiresDate.getTime() - 1000)));
                    Assertions.assertTrue(objectMetadata.getExpires().before(new Date(expiresDate.getTime() + 1000)));
                }
            }

        } finally {
            deleteAllObjectVersions(provider(), bucket);
            candyS3.deleteBucket(bucket);
        }
    }

    @Test
    @Tag(TestTag.basic)
    void copyObjectPropertiesTest() throws IOException, NoSuchAlgorithmException, InterruptedException {
        CandyS3 candyS3 = init(provider());
        String bucket = genTestBucketName("copyObjectPropertiesTest");
        String sourceObjectKey = "source.data";
        String copyWithCopyDirectiveObjectKey = "copy1.data";
        String copyWithReplaceDirectiveObjectKey = "copy2.data";
        byte[] bytes = "x我y".getBytes(StandardCharsets.UTF_8);
        try {
            candyS3.createBucket(new CreateBucketOptions.CreateBucketOptionsBuilder(bucket).build());

            // put source object
            Date sourceExpiresDate = new Date(System.currentTimeMillis() + 1000 * 120 * 2);
            {
                candyS3.putObject(bucket, sourceObjectKey, new PutObjectOptions.PutObjectOptionsBuilder()
                        .configureUploadData().withData(bytes).endConfigureDataContent()
                        .configurePutObjectHeaderOptions()
                        .cacheControl("public, max-age=3600")
                        .contentDisposition("attachment; filename=\"thisisaattachment.html\"")
                        .contentEncoding("gzip, deflate")
                        .contentLanguage("en, zh-CN")
                        .contentType("text/html")
                        .expires(sourceExpiresDate)
                        .endConfigurePutObjectHeaderOptions()
                        .build());
            }

            Date expiresDate = new Date(System.currentTimeMillis() + 1000 * 30 * 2);

            // copy object with x-amz-metadata-directive defaults to COPY
            {
                CopyObjectOptions copyObjectOptions = new CopyObjectOptions.CopyObjectOptionsBuilder()
                        .copySource(bucket, sourceObjectKey)
                        .configureCopyObjectHeaderOptions()
                        .cacheControl("public, max-age=7200")
                        .contentDisposition("attachment; filename=\"thisisaattachment-copy.html\"")
                        .contentEncoding("gzip")
                        .contentLanguage("en")
                        .contentType("text/plain")
                        .expires(expiresDate)
                        .endConfigureCopyObjectHeaderOptions()
                        .build();

                // Cloudflare R2 does not support 'x-amz-tagging-directive'
                if (S3Provider.CLOUDFLARE_R2.equals(provider())) {
                    copyObjectOptions = new CopyObjectOptions.CopyObjectOptionsBuilder()
                            .copySource(bucket, sourceObjectKey)
                            .excludeTaggingDirective()
                            .configureCopyObjectHeaderOptions()
                            .cacheControl("public, max-age=7200")
                            .contentDisposition("attachment; filename=\"thisisaattachment-copy.html\"")
                            .contentEncoding("gzip")
                            .contentLanguage("en")
                            .contentType("text/plain")
                            .expires(expiresDate)
                            .endConfigureCopyObjectHeaderOptions()
                            .build();
                }
                candyS3.copyObject(bucket, copyWithCopyDirectiveObjectKey, copyObjectOptions);

                S3Object s3Object = candyS3.getObjectMetadata(bucket, copyWithCopyDirectiveObjectKey, new DownloadObjectOptions.DownloadObjectOptionsBuilder().build());
                S3Object.S3ObjectMetadata objectMetadata = s3Object.getObjectMetadata();

                Assertions.assertNull(s3Object.getContentBytes());
                Assertions.assertNotNull(s3Object.getObjectMetadata());

                Assertions.assertEquals(s3Object.getSize(), bytes.length);

                Assertions.assertEquals(objectMetadata.getCacheControl(), "public, max-age=3600");
                Assertions.assertEquals(objectMetadata.getContentDisposition(), "attachment; filename=\"thisisaattachment.html\"");
                Assertions.assertEquals(objectMetadata.getContentEncoding(), "gzip, deflate");
                Assertions.assertEquals(objectMetadata.getContentLanguage(), "en, zh-CN");
                Assertions.assertNull(objectMetadata.getContentRange());
                Assertions.assertEquals(objectMetadata.getContentType(), "text/html");
                Assertions.assertTrue(objectMetadata.getExpires().after(new Date(sourceExpiresDate.getTime() - 1000)));
                Assertions.assertTrue(objectMetadata.getExpires().before(new Date(sourceExpiresDate.getTime() + 1000)));
            }

            // copy object with x-amz-metadata-directive is REPLACE
            {
                CopyObjectOptions copyObjectOptions = new CopyObjectOptions.CopyObjectOptionsBuilder()
                        .copySource(bucket, sourceObjectKey)
                        .replaceTaggingDirective()
                        .configureCopyObjectHeaderOptions()
                        .replaceMetadataDirective()
                        .cacheControl("public, max-age=7200")
                        .contentDisposition("attachment; filename=\"thisisaattachment-copy.html\"")
                        .contentEncoding("gzip")
                        .contentLanguage("en")
                        .contentType("text/plain")
                        .expires(expiresDate)
                        .endConfigureCopyObjectHeaderOptions()
                        .build();
                // Cloudflare R2 does not support 'x-amz-tagging-directive'
                if (S3Provider.CLOUDFLARE_R2.equals(provider())) {
                    copyObjectOptions = new CopyObjectOptions.CopyObjectOptionsBuilder()
                            .copySource(bucket, sourceObjectKey)
                            .excludeTaggingDirective()
                            .configureCopyObjectHeaderOptions()
                            .replaceMetadataDirective()
                            .cacheControl("public, max-age=7200")
                            .contentDisposition("attachment; filename=\"thisisaattachment-copy.html\"")
                            .contentEncoding("gzip")
                            .contentLanguage("en")
                            .contentType("text/plain")
                            .expires(expiresDate)
                            .endConfigureCopyObjectHeaderOptions()
                            .build();
                }
                candyS3.copyObject(bucket, copyWithReplaceDirectiveObjectKey, copyObjectOptions);

                S3Object s3Object = candyS3.getObjectMetadata(bucket, copyWithReplaceDirectiveObjectKey, new DownloadObjectOptions.DownloadObjectOptionsBuilder().build());
                S3Object.S3ObjectMetadata objectMetadata = s3Object.getObjectMetadata();

                Assertions.assertNull(s3Object.getContentBytes());
                Assertions.assertNotNull(s3Object.getObjectMetadata());

                Assertions.assertEquals(s3Object.getSize(), bytes.length);

                Assertions.assertEquals(objectMetadata.getCacheControl(), "public, max-age=7200");
                Assertions.assertEquals(objectMetadata.getContentDisposition(), "attachment; filename=\"thisisaattachment-copy.html\"");
                Assertions.assertEquals(objectMetadata.getContentEncoding(), "gzip");
                Assertions.assertEquals(objectMetadata.getContentLanguage(), "en");
                Assertions.assertNull(objectMetadata.getContentRange());
                Assertions.assertEquals(objectMetadata.getContentType(), "text/plain");
                Assertions.assertTrue(objectMetadata.getExpires().after(new Date(expiresDate.getTime() - 1000)));
                Assertions.assertTrue(objectMetadata.getExpires().before(new Date(expiresDate.getTime() + 1000)));

            }

            Thread.sleep(30 * 1000);

        } finally {
            deleteAllObjectVersions(provider(), bucket);
            candyS3.deleteBucket(bucket);
        }
    }

    @Test
    @Tag(TestTag.objectLock)
    void putAndGetObjectLockPropertiesTest() throws IOException, NoSuchAlgorithmException, InterruptedException {
        CandyS3 candyS3 = init(provider());
        String bucket = genTestBucketName("pAGOLTest");
        String objectKey = "putAndGetObjectLockPropertiesTest.data";
        try {
            candyS3.createBucket(new CreateBucketOptions.CreateBucketOptionsBuilder(bucket).enableObjectLock().build());

            // Enable object-lock when create bucket is not supported by Tencent cloud COS
            if (S3Provider.TENCENTCLOUD_COS.equals(provider())) {
                candyS3.enableBucketObjectLock(bucket, new UpdateBucketObjectLockOptions.UpdateBucketObjectLockOptionsBuilder().buildWithoutRetention());
            }

            Date retainDate = new Date(System.currentTimeMillis() + 1000 * 30);

            // putObject with object lock
            {
                candyS3.putObject(bucket, objectKey + 1, new PutObjectOptions.PutObjectOptionsBuilder()
                        .configureUploadData().withData("x".getBytes(StandardCharsets.UTF_8)).endConfigureDataContent()
                        .configureObjectLockOptions()
                        .lockMode(ObjectRetentionMode.COMPLIANCE)
                        .retainUntilDate(retainDate)
                        .legalHold(true)
                        .endConfigureObjectLockOptions()
                        .build());

                S3Object s3Object = candyS3.getObjectMetadata(bucket, objectKey + 1, new DownloadObjectOptions.DownloadObjectOptionsBuilder().build());
                Assertions.assertNull(s3Object.getContentBytes());
                Assertions.assertNotNull(s3Object.getObjectMetadata());

                Assertions.assertEquals(s3Object.getObjectLockConfiguration().getObjectLockMode(), ObjectRetentionMode.COMPLIANCE);
                Assertions.assertTrue(s3Object.getObjectLockConfiguration().getObjectLockRetainUntilDate().after(new Date(retainDate.getTime() - 1000)));
                Assertions.assertTrue(s3Object.getObjectLockConfiguration().getObjectLockRetainUntilDate().before(new Date(retainDate.getTime() + 1000)));
                Assertions.assertTrue(s3Object.getObjectLockConfiguration().isObjectLockLegalHold());
            }
            // createMultipartUpload with object lock
            {
                String uploadId = candyS3.createMultipartUpload(bucket, objectKey + 2,
                        new CreateMultipartUploadOptions.CreateMultipartUploadOptionsBuilder()
                                .configureObjectLockOptions()
                                .lockMode(ObjectRetentionMode.COMPLIANCE)
                                .retainUntilDate(retainDate)
                                .legalHold(true)
                                .endConfigureObjectLockOptions()
                                .build());

                S3Part part1 = candyS3.uploadPart(bucket, objectKey + 2, uploadId, 1,
                        new UploadPartOptions.UploadPartOptionsBuilder()
                                .configureUploadData().withData(StringUtils.repeat("x", 5 * 1024 * 1024).getBytes(StandardCharsets.UTF_8)).endConfigureDataContent()
                                .build());
                S3Part part2 = candyS3.uploadPart(bucket, objectKey + 2, uploadId, 2,
                        new UploadPartOptions.UploadPartOptionsBuilder()
                                .configureUploadData().withData("y".getBytes(StandardCharsets.UTF_8)).endConfigureDataContent()
                                .build());
                candyS3.completeMultipartUpload(bucket, objectKey + 2, uploadId, Arrays.asList(part1, part2),
                        new CompleteMultipartUploadOptions.CompleteMultipartUploadOptionsBuilder().build());

                S3Object s3Object = candyS3.getObjectMetadata(bucket, objectKey + 2, new DownloadObjectOptions.DownloadObjectOptionsBuilder().build());
                Assertions.assertNull(s3Object.getContentBytes());
                Assertions.assertNotNull(s3Object.getObjectMetadata());

                Assertions.assertEquals(s3Object.getObjectLockConfiguration().getObjectLockMode(), ObjectRetentionMode.COMPLIANCE);
                Assertions.assertTrue(s3Object.getObjectLockConfiguration().getObjectLockRetainUntilDate().after(new Date(retainDate.getTime() - 1000)));
                Assertions.assertTrue(s3Object.getObjectLockConfiguration().getObjectLockRetainUntilDate().before(new Date(retainDate.getTime() + 1000)));
                Assertions.assertTrue(s3Object.getObjectLockConfiguration().isObjectLockLegalHold());
            }
            // copyObject with object lock
            {
                Date copyObjectretainDate = new Date(retainDate.getTime() + 30 * 1000);
                candyS3.copyObject(bucket, objectKey + 3,
                        new CopyObjectOptions.CopyObjectOptionsBuilder()
                                .copySource(bucket, objectKey + 1)
                                .configureObjectLockOptions()
                                .lockMode(ObjectRetentionMode.COMPLIANCE)
                                .retainUntilDate(copyObjectretainDate)
                                .legalHold(false)
                                .endConfigureObjectLockOptions()
                                .build());

                S3Object s3Object = candyS3.getObjectMetadata(bucket, objectKey + 3, new DownloadObjectOptions.DownloadObjectOptionsBuilder().build());
                Assertions.assertNull(s3Object.getContentBytes());
                Assertions.assertNotNull(s3Object.getObjectMetadata());

                Assertions.assertEquals(s3Object.getObjectLockConfiguration().getObjectLockMode(), ObjectRetentionMode.COMPLIANCE);
                Assertions.assertTrue(s3Object.getObjectLockConfiguration().getObjectLockRetainUntilDate().after(new Date(copyObjectretainDate.getTime() - 1000)));
                Assertions.assertTrue(s3Object.getObjectLockConfiguration().getObjectLockRetainUntilDate().before(new Date(copyObjectretainDate.getTime() + 1000)));
                Assertions.assertFalse(s3Object.getObjectLockConfiguration().isObjectLockLegalHold());
            }


            Thread.sleep(60 * 1000);
        } finally {
            deleteAllObjectVersions(provider(), bucket);
            candyS3.deleteBucket(bucket);
        }
    }

    @Test
    @Tag(TestTag.basic)
    void putAndGetObjectStorageClassTest() throws IOException, NoSuchAlgorithmException {
        CandyS3 candyS3 = init(provider());
        String bucket = genTestBucketName("putAndGetObjectStorageClassTest");
        byte[] bytes = "x我y".getBytes(StandardCharsets.UTF_8);
        try {
            candyS3.createBucket(new CreateBucketOptions.CreateBucketOptionsBuilder(bucket).build());

            // putObject and getObjectMetadata
            {
                String objectKey = "pAGOSCest1.data";
                candyS3.putObject(bucket, objectKey, new PutObjectOptions.PutObjectOptionsBuilder()
                        .configureUploadData().withData(bytes).endConfigureDataContent()
                        .storageClass(StorageClass.STANDARD_IA)
                        .build());


                S3Object s3Object = candyS3.getObjectMetadata(bucket, objectKey, new DownloadObjectOptions.DownloadObjectOptionsBuilder().build());

                Assertions.assertNull(s3Object.getContentBytes());
                Assertions.assertNotNull(s3Object.getObjectMetadata());

                Assertions.assertEquals(s3Object.getSize(), "x我y".getBytes(StandardCharsets.UTF_8).length);

                Assertions.assertEquals(s3Object.getStorageClass(), StorageClass.STANDARD_IA.name());
            }

            // multipartUpload and getObjectMetadata
            {
                String objectKey2 = "pAGOSCest2.data";
                String uploadId = candyS3.createMultipartUpload(bucket, objectKey2, new CreateMultipartUploadOptions.CreateMultipartUploadOptionsBuilder()
                        .storageClass(StorageClass.STANDARD_IA)
                        .build());

                byte[] bytes1 = StringUtils.repeat('x', 6 * 1024 * 1024).getBytes(StandardCharsets.UTF_8);
                S3Part part1 = candyS3.uploadPart(bucket, objectKey2, uploadId, 1, new UploadPartOptions.UploadPartOptionsBuilder()
                        .configureUploadData().withData(bytes1).endConfigureDataContent()
                        .build());

                byte[] bytes2 = new byte[]{1};
                S3Part part2 = candyS3.uploadPart(bucket, objectKey2, uploadId, 2, new UploadPartOptions.UploadPartOptionsBuilder()
                        .configureUploadData().withData(bytes2).endConfigureDataContent()
                        .build());

                candyS3.completeMultipartUpload(bucket, objectKey2, uploadId, Arrays.asList(part1, part2),
                        new CompleteMultipartUploadOptions.CompleteMultipartUploadOptionsBuilder()
                                .build());

                S3Object s3Object = candyS3.getObjectMetadata(bucket, objectKey2, new DownloadObjectOptions.DownloadObjectOptionsBuilder()
                        .build());

                Assertions.assertNull(s3Object.getContentBytes());
                Assertions.assertNotNull(s3Object.getObjectMetadata());

                Assertions.assertEquals(s3Object.getSize(), bytes1.length + bytes2.length);

                Assertions.assertEquals(s3Object.getStorageClass(), StorageClass.STANDARD_IA.name());
            }

            // copyObject and getObjectMetadata
            {
                String sourceObjectKey = "source.data";
                String copyObjectKey = "copy1.data";

                // put source object
                {
                    candyS3.putObject(bucket, sourceObjectKey, new PutObjectOptions.PutObjectOptionsBuilder()
                            .configureUploadData().withData(bytes).endConfigureDataContent()
                            .storageClass(StorageClass.STANDARD)
                            .build());
                }

                // copy and use other storage class
                CopyObjectOptions copyObjectOptions = new CopyObjectOptions.CopyObjectOptionsBuilder()
                        .copySource(bucket, sourceObjectKey)
                        .storageClass(StorageClass.STANDARD_IA)
                        .build();
                if (S3Provider.CLOUDFLARE_R2.equals(provider())) {
                    copyObjectOptions = new CopyObjectOptions.CopyObjectOptionsBuilder()
                            .copySource(bucket, sourceObjectKey)
                            .excludeTaggingDirective()
                            .storageClass(StorageClass.STANDARD_IA)
                            .build();
                }
                candyS3.copyObject(bucket, copyObjectKey, copyObjectOptions);

                S3Object s3Object = candyS3.getObjectMetadata(bucket, copyObjectKey, new DownloadObjectOptions.DownloadObjectOptionsBuilder().build());

                Assertions.assertNull(s3Object.getContentBytes());
                Assertions.assertNotNull(s3Object.getObjectMetadata());

                Assertions.assertEquals(s3Object.getSize(), bytes.length);

                Assertions.assertEquals(s3Object.getStorageClass(), StorageClass.STANDARD_IA.name());

            }

        } finally {
            deleteAllObjectVersions(provider(), bucket);
            candyS3.deleteBucket(bucket);
        }
    }

    @Test
    @Tag(TestTag.tagging)
    void putAndGetObjectTagTest() throws IOException, NoSuchAlgorithmException {
        CandyS3 candyS3 = init(provider());
        String bucket = genTestBucketName("putAndGetObjectTagTest");
        String objectKey = "putAndGetObjectTagTest.data";
        byte[] bytes = "x我y".getBytes(StandardCharsets.UTF_8);
        try {
            candyS3.createBucket(new CreateBucketOptions.CreateBucketOptionsBuilder(bucket).build());

            // putObject and getObjectMetadata
            {
                Map<String, String> tags = new HashMap<>();
                tags.put("test2", "yy");
                tags.put("test3", "zz");
                candyS3.putObject(bucket, objectKey, new PutObjectOptions.PutObjectOptionsBuilder()
                        .configureUploadData().withData(bytes).endConfigureDataContent()
                        .addTag("test1", "xx")
                        .addTags(tags)
                        .build());


                S3Object s3Object = candyS3.getObjectMetadata(bucket, objectKey, new DownloadObjectOptions.DownloadObjectOptionsBuilder().build());

                Assertions.assertNull(s3Object.getContentBytes());
                Assertions.assertNotNull(s3Object.getObjectMetadata());

                Assertions.assertEquals(s3Object.getSize(), "x我y".getBytes(StandardCharsets.UTF_8).length);

                Assertions.assertEquals(s3Object.getTagCount(), (Integer) 3);
            }

            // multipartUpload and getObjectMetadata
            {
                Map<String, String> tags = new HashMap<>();
                tags.put("test2", "yy");
                tags.put("test3", "zz");
                String uploadId = candyS3.createMultipartUpload(bucket, objectKey, new CreateMultipartUploadOptions.CreateMultipartUploadOptionsBuilder()
                        .addTag("test1", "xx")
                        .addTags(tags)
                        .build());

                byte[] bytes1 = StringUtils.repeat('x', 6 * 1024 * 1024).getBytes(StandardCharsets.UTF_8);
                S3Part part1 = candyS3.uploadPart(bucket, objectKey, uploadId, 1, new UploadPartOptions.UploadPartOptionsBuilder()
                        .configureUploadData().withData(bytes1).endConfigureDataContent()
                        .build());

                byte[] bytes2 = new byte[]{1};
                S3Part part2 = candyS3.uploadPart(bucket, objectKey, uploadId, 2, new UploadPartOptions.UploadPartOptionsBuilder()
                        .configureUploadData().withData(bytes2).endConfigureDataContent()
                        .build());

                candyS3.completeMultipartUpload(bucket, objectKey, uploadId, Arrays.asList(part1, part2),
                        new CompleteMultipartUploadOptions.CompleteMultipartUploadOptionsBuilder()
                                .build());

                S3Object s3Object = candyS3.getObjectMetadata(bucket, objectKey, new DownloadObjectOptions.DownloadObjectOptionsBuilder()
                        .build());

                Assertions.assertNull(s3Object.getContentBytes());
                Assertions.assertNotNull(s3Object.getObjectMetadata());

                Assertions.assertEquals(s3Object.getSize(), bytes1.length + bytes2.length);

                Assertions.assertEquals(s3Object.getTagCount(), (Integer) 3);
            }

            // copyObject and getObjectMetadata
            {
                String sourceObjectKey = "source.data";
                String copyWithCopyDirectiveObjectKey = "copy1.data";
                String copyWithReplaceDirectiveObjectKey = "copy2.data";

                // put source object
                {
                    Map<String, String> tags = new HashMap<>();
                    tags.put("test2", "yy");
                    tags.put("test3", "zz");
                    candyS3.putObject(bucket, sourceObjectKey, new PutObjectOptions.PutObjectOptionsBuilder()
                            .configureUploadData().withData(bytes).endConfigureDataContent()
                            .addTag("test1", "xx")
                            .addTags(tags)
                            .build());
                }

                Map<String, String> tags = new HashMap<>();
                tags.put("t2", "yyy");
                tags.put("t3", "zzz");
                tags.put("t4", "aaa");


                // copy object with x-amz-tagging-directive defaults to COPY
                {
                    candyS3.copyObject(bucket, copyWithCopyDirectiveObjectKey, new CopyObjectOptions.CopyObjectOptionsBuilder()
                            .copySource(bucket, sourceObjectKey)
                            .addTag("t1", "xxx")
                            .addTags(tags)
                            .build());

                    // getObjectMetadata
                    {
                        S3Object s3Object = candyS3.getObjectMetadata(bucket, copyWithCopyDirectiveObjectKey, new DownloadObjectOptions.DownloadObjectOptionsBuilder().build());

                        Assertions.assertNull(s3Object.getContentBytes());
                        Assertions.assertNotNull(s3Object.getObjectMetadata());

                        Assertions.assertEquals(s3Object.getSize(), bytes.length);

                        Assertions.assertEquals(s3Object.getTagCount(), (Integer) 3);
                    }
                }

                // copy object with x-amz-tagging-directive is REPLACE
                {
                    candyS3.copyObject(bucket, copyWithReplaceDirectiveObjectKey, new CopyObjectOptions.CopyObjectOptionsBuilder()
                            .copySource(bucket, sourceObjectKey)
                            .replaceTaggingDirective()
                            .addTag("t1", "xxx")
                            .addTags(tags)
                            .build());

                    // getObjectMetadata
                    {
                        S3Object s3Object = candyS3.getObjectMetadata(bucket, copyWithReplaceDirectiveObjectKey, new DownloadObjectOptions.DownloadObjectOptionsBuilder().build());

                        Assertions.assertNull(s3Object.getContentBytes());
                        Assertions.assertNotNull(s3Object.getObjectMetadata());

                        Assertions.assertEquals(s3Object.getSize(), bytes.length);

                        Assertions.assertEquals(s3Object.getTagCount(), (Integer) 4);
                    }
                }
            }
        } finally {
            deleteAllObjectVersions(provider(), bucket);
            candyS3.deleteBucket(bucket);
        }
    }

    @Test
    @Tag(TestTag.sse)
    void putAndGetObjectSseTest() throws IOException, NoSuchAlgorithmException {
        CandyS3 candyS3 = init(provider());
        String bucket = genTestBucketName("putAndGetObjectSseTest");
        String baseObjectKey = "putAndGetObjectSseTest.data";
        try {
            candyS3.createBucket(new CreateBucketOptions.CreateBucketOptionsBuilder(bucket).build());

            // create objects with sse
            {
                // create with putObject
                candyS3.putObject(bucket, baseObjectKey + 1, new PutObjectOptions.PutObjectOptionsBuilder()
                        .configureUploadData().withData("x".getBytes(StandardCharsets.UTF_8)).endConfigureDataContent()
                        .configureServerSideEncryptionOptions().sseAlgorithm(ServerSideEncryptionAlgorithm.AES256).endConfigureServerSideEncryptionOptions()
                        .build());

                // create with multipartUpload
                String uploadId = candyS3.createMultipartUpload(bucket, baseObjectKey + 2,
                        new CreateMultipartUploadOptions.CreateMultipartUploadOptionsBuilder()
                                .configureServerSideEncryptionOptions()
                                .sseAlgorithm(ServerSideEncryptionAlgorithm.AES256)
                                .endConfigureServerSideEncryptionOptions()
                                .build());
                S3Part part1 = candyS3.uploadPart(bucket, baseObjectKey + 2, uploadId, 1, new UploadPartOptions.UploadPartOptionsBuilder()
                        .configureUploadData()
                        .withData(StringUtils.repeat("x", 5 * 1024 * 1024)
                                .getBytes(StandardCharsets.UTF_8)).endConfigureDataContent()
                        .build());
                S3Part part2 = candyS3.uploadPart(bucket, baseObjectKey + 2, uploadId, 2, new UploadPartOptions.UploadPartOptionsBuilder()
                        .configureUploadData()
                        .withData("y".getBytes(StandardCharsets.UTF_8)).endConfigureDataContent()
                        .build());
                candyS3.completeMultipartUpload(bucket, baseObjectKey + 2, uploadId, Arrays.asList(part1, part2),
                        new CompleteMultipartUploadOptions.CompleteMultipartUploadOptionsBuilder().build());

                // create with copyObject
                CopyObjectOptions copyObjectOptions = new CopyObjectOptions.CopyObjectOptionsBuilder()
                        .copySource(bucket, baseObjectKey + 1)
                        .configureServerSideEncryptionOptions()
                        .sseAlgorithm(ServerSideEncryptionAlgorithm.AES256)
                        .endConfigureServerSideEncryptionOptions()
                        .build();
                // Cloudflare R2 does not support 'x-amz-tagging-directive'
                if (S3Provider.CLOUDFLARE_R2.equals(provider())) {
                    copyObjectOptions = new CopyObjectOptions.CopyObjectOptionsBuilder()
                            .copySource(bucket, baseObjectKey + 1)
                            .excludeTaggingDirective()
                            .configureServerSideEncryptionOptions()
                            .sseAlgorithm(ServerSideEncryptionAlgorithm.AES256)
                            .endConfigureServerSideEncryptionOptions()
                            .build();
                }
                candyS3.copyObject(bucket, baseObjectKey + 3, copyObjectOptions);
            }

            DeleteObjectsBatchOptions.DeleteObjectsBatchOptionsBuilder deleteObjectsBatchOptionsBuilder = new DeleteObjectsBatchOptions.DeleteObjectsBatchOptionsBuilder();

            // get object sse properties with getObjectMetadata
            for (int i = 1; i <= 3; i++) {
                S3Object s3Object = candyS3.getObjectMetadata(bucket, baseObjectKey + i, new DownloadObjectOptions.DownloadObjectOptionsBuilder().build());

                Assertions.assertNotNull(s3Object.getServerSideEncryptionConfiguration());
                Assertions.assertEquals(s3Object.getServerSideEncryptionConfiguration().getSseAlgorithm(), ServerSideEncryptionAlgorithm.AES256.getAlgorithm());

                Assertions.assertNotNull(s3Object.getObjectMetadata());

                deleteObjectsBatchOptionsBuilder.addDeleteObject(baseObjectKey + i);
            }

            // get object sse properties with downloadObject
            for (int i = 1; i <= 3; i++) {
                S3Object s3Object = candyS3.downloadObject(bucket, baseObjectKey + i, new DownloadObjectOptions.DownloadObjectOptionsBuilder().build());

                Assertions.assertNotNull(s3Object.getServerSideEncryptionConfiguration());
                Assertions.assertEquals(s3Object.getServerSideEncryptionConfiguration().getSseAlgorithm(), ServerSideEncryptionAlgorithm.AES256.getAlgorithm());

                Assertions.assertNotNull(s3Object.getObjectMetadata());

                deleteObjectsBatchOptionsBuilder.addDeleteObject(baseObjectKey + i);
            }

            candyS3.deleteObjectsBatch(bucket, deleteObjectsBatchOptionsBuilder.build());
        } finally {
            candyS3.deleteBucket(bucket);
        }

    }

    @Test
    @Tag(TestTag.basic)
    void copyObjectAndDownloadTest() throws IOException, NoSuchAlgorithmException {
        CandyS3 candyS3 = init(provider());
        String bucket1 = genTestBucketName("copyDownloadObjectTest1");
        String objectKey1 = "copyDownloadObjectTest1.data";
        String bucket2 = genTestBucketName("copyDownloadObjectTest2");
        String objectKey2 = "sub/copyDownloadObjectTest2.data";
        String content = StringUtils.repeat("x", 1024);
        try {
            candyS3.createBucket(new CreateBucketOptions.CreateBucketOptionsBuilder(bucket1).build());

            byte[] bytes = content.getBytes(StandardCharsets.UTF_8);

            candyS3.putObject(bucket1, objectKey1, new PutObjectOptions.PutObjectOptionsBuilder()
                    .configureUploadData().withData(bytes).endConfigureDataContent()
                    .build());
            ListPaginationResult<S3Object> result1 =
                    candyS3.listObjects(bucket1, new ListObjectOptions());
            Assertions.assertEquals(result1.getResults().get(0).getKey(), objectKey1);
            Assertions.assertEquals(result1.getResults().get(0).getSize(), 1024);

            candyS3.createBucket(new CreateBucketOptions.CreateBucketOptionsBuilder(bucket2).build());

            CopyObjectOptions copyObjectOptions = new CopyObjectOptions.CopyObjectOptionsBuilder()
                    .copySource(bucket1, objectKey1)
                    .build();
            // Cloudflare R2 does not support 'x-amz-tagging-directive'
            if (S3Provider.CLOUDFLARE_R2.equals(provider())) {
                copyObjectOptions = new CopyObjectOptions.CopyObjectOptionsBuilder()
                        .copySource(bucket1, objectKey1)
                        .excludeTaggingDirective()
                        .build();
            }
            candyS3.copyObject(bucket2, objectKey2, copyObjectOptions);
            ListPaginationResult<S3Object> result2 =
                    candyS3.listObjects(bucket2, new ListObjectOptions());
            Assertions.assertEquals(result2.getResults().get(0).getKey(), objectKey2);
            Assertions.assertEquals(result2.getResults().get(0).getSize(), 1024);

            S3Object downloadObject2 = candyS3.downloadObject(bucket2, objectKey2,
                    new DownloadObjectOptions.DownloadObjectOptionsBuilder()
                            .configureDataOutput().toBytes().endConfigureDataOutput()
                            .build());
            Assertions.assertArrayEquals(bytes, downloadObject2.getContentBytes());

        } finally {
            candyS3.deleteObject(bucket1, new DeleteObjectOptions(objectKey1));
            candyS3.deleteObject(bucket2, new DeleteObjectOptions(objectKey2));

            candyS3.deleteBucket(bucket1);
            candyS3.deleteBucket(bucket2);
        }
    }

    @Test
    @Tag(TestTag.conditional)
    void copyObjectCopySourceConditionalTest() throws IOException, NoSuchAlgorithmException {
        CandyS3 candyS3 = init(provider());
        String bucket = genTestBucketName("cOCSCondTest");
        String objectKey = "copyObjectCopySourceConditionalTest.data";
        String objectKey2 = "copyObjectCopySourceConditionalTest2.data";
        String objectKey3 = "copyObjectCopySourceConditionalTest3.data";
        String objectKey4 = "copyObjectCopySourceConditionalTest4.data";
        String objectKey5 = "copyObjectCopySourceConditionalTest5.data";
        byte[] bytes = "x".getBytes(StandardCharsets.UTF_8);
        try {

            candyS3.createBucket(new CreateBucketOptions.CreateBucketOptionsBuilder(bucket).build());
            candyS3.putObject(bucket, objectKey, new PutObjectOptions.PutObjectOptionsBuilder()
                    .configureUploadData().withData(bytes).endConfigureDataContent()
                    .build());

            ListPaginationResult<S3Object> result =
                    candyS3.listObjects(bucket, new ListObjectOptions().prefix(objectKey).maxKeys(1));
            String objectEtag = result.getResults().get(0).geteTag();
            Date objectLastModified = result.getResults().get(0).getLastModified();

            {
                try {
                    CopyObjectOptions copyObjectOptions = new CopyObjectOptions.CopyObjectOptionsBuilder()
                            .copySource(bucket, objectKey)
                            .configureCopySourceCondition()
                            .ifMatch("x").endConfigureCondition()
                            .build();
                    // Cloudflare R2 does not support 'x-amz-tagging-directive'
                    if (S3Provider.CLOUDFLARE_R2.equals(provider())) {
                        copyObjectOptions = new CopyObjectOptions.CopyObjectOptionsBuilder()
                                .copySource(bucket, objectKey)
                                .excludeTaggingDirective()
                                .configureCopySourceCondition()
                                .ifMatch("x").endConfigureCondition()
                                .build();
                    }
                    candyS3.copyObject(bucket, objectKey2, copyObjectOptions);
                    Assertions.fail("Should not be here. PreconditionFailed should be thrown.");
                } catch (Exception ex) {
                    Assertions.assertTrue(ex instanceof CandyS3Exception);
                    Assertions.assertEquals(CommonErrorCode.OBJECT_PRECONDITION_FAILED.getCode(), ((CandyS3Exception) ex).getCode());
                    Assertions.assertEquals("PreconditionFailed", ((CandyS3Exception) ex).getParsedError().getCode());
                }

                CopyObjectOptions copyObjectOptions = new CopyObjectOptions.CopyObjectOptionsBuilder()
                        .copySource(bucket, objectKey)
                        .configureCopySourceCondition().ifMatch(objectEtag).endConfigureCondition()
                        .build();
                // Cloudflare R2 does not support 'x-amz-tagging-directive'
                if (S3Provider.CLOUDFLARE_R2.equals(provider())) {
                    copyObjectOptions = new CopyObjectOptions.CopyObjectOptionsBuilder()
                            .copySource(bucket, objectKey)
                            .excludeTaggingDirective()
                            .configureCopySourceCondition().ifMatch(objectEtag).endConfigureCondition()
                            .build();
                }
                candyS3.copyObject(bucket, objectKey2, copyObjectOptions);
            }

            {
                try {
                    CopyObjectOptions copyObjectOptions = new CopyObjectOptions.CopyObjectOptionsBuilder()
                            .copySource(bucket, objectKey)
                            .configureCopySourceCondition().ifNoneMatch(objectEtag).endConfigureCondition()
                            .build();
                    // Cloudflare R2 does not support 'x-amz-tagging-directive'
                    if (S3Provider.CLOUDFLARE_R2.equals(provider())) {
                        copyObjectOptions = new CopyObjectOptions.CopyObjectOptionsBuilder()
                                .copySource(bucket, objectKey)
                                .excludeTaggingDirective()
                                .configureCopySourceCondition().ifNoneMatch(objectEtag).endConfigureCondition()
                                .build();
                    }
                    candyS3.copyObject(bucket, objectKey3, copyObjectOptions);
                    Assertions.fail("Should not be here. PreconditionFailed should be thrown.");
                } catch (Exception ex) {
                    Assertions.assertTrue(ex instanceof CandyS3Exception);
                    if (S3Provider.ALIYUN_OSS.equals(provider())) {
                        Assertions.assertEquals("Not Modified", ((CandyS3Exception) ex).getParsedError().getMessage());
                    } else {
                        Assertions.assertEquals(CommonErrorCode.OBJECT_PRECONDITION_FAILED.getCode(), ((CandyS3Exception) ex).getCode());
                    }
                }

                CopyObjectOptions copyObjectOptions = new CopyObjectOptions.CopyObjectOptionsBuilder()
                        .copySource(bucket, objectKey)
                        .configureCopySourceCondition().ifNoneMatch("x").endConfigureCondition()
                        .build();
                // Cloudflare R2 does not support 'x-amz-tagging-directive'
                if (S3Provider.CLOUDFLARE_R2.equals(provider())) {
                    copyObjectOptions = new CopyObjectOptions.CopyObjectOptionsBuilder()
                            .copySource(bucket, objectKey)
                            .excludeTaggingDirective()
                            .configureCopySourceCondition().ifNoneMatch("x").endConfigureCondition()
                            .build();
                }
                candyS3.copyObject(bucket, objectKey3, copyObjectOptions);
            }

            {
                try {
                    CopyObjectOptions copyObjectOptions = new CopyObjectOptions.CopyObjectOptionsBuilder()
                            .copySource(bucket, objectKey)
                            .configureCopySourceCondition().ifModifiedSince(objectLastModified).endConfigureCondition()
                            .build();
                    // Cloudflare R2 does not support 'x-amz-tagging-directive'
                    if (S3Provider.CLOUDFLARE_R2.equals(provider())) {
                        copyObjectOptions = new CopyObjectOptions.CopyObjectOptionsBuilder()
                                .copySource(bucket, objectKey)
                                .excludeTaggingDirective()
                                .configureCopySourceCondition().ifModifiedSince(objectLastModified).endConfigureCondition()
                                .build();
                    }
                    candyS3.copyObject(bucket, objectKey4, copyObjectOptions);
                    Assertions.fail("Should not be here. PreconditionFailed should be thrown.");
                } catch (Exception ex) {
                    Assertions.assertTrue(ex instanceof CandyS3Exception);
                    if (S3Provider.ALIYUN_OSS.equals(provider())) {
                        Assertions.assertEquals("Not Modified", ((CandyS3Exception) ex).getParsedError().getMessage());
                    } else {
                        Assertions.assertEquals(CommonErrorCode.OBJECT_PRECONDITION_FAILED.getCode(), ((CandyS3Exception) ex).getCode());
                    }
                }

                CopyObjectOptions copyObjectOptions = new CopyObjectOptions.CopyObjectOptionsBuilder()
                        .copySource(bucket, objectKey)
                        .configureCopySourceCondition()
                        .ifModifiedSince(new Date(objectLastModified.getTime() - 1000))
                        .endConfigureCondition()
                        .build();
                // Cloudflare R2 does not support 'x-amz-tagging-directive'
                if (S3Provider.CLOUDFLARE_R2.equals(provider())) {
                    copyObjectOptions = new CopyObjectOptions.CopyObjectOptionsBuilder()
                            .copySource(bucket, objectKey)
                            .excludeTaggingDirective()
                            .configureCopySourceCondition()
                            .ifModifiedSince(new Date(objectLastModified.getTime() - 1000))
                            .endConfigureCondition()
                            .build();
                }
                candyS3.copyObject(bucket, objectKey4, copyObjectOptions);
            }

            {
                try {
                    CopyObjectOptions copyObjectOptions = new CopyObjectOptions.CopyObjectOptionsBuilder()
                            .copySource(bucket, objectKey)
                            .configureCopySourceCondition()
                            .ifUnmodifiedSince(new Date(objectLastModified.getTime() - 1000))
                            .endConfigureCondition()
                            .build();
                    // Cloudflare R2 does not support 'x-amz-tagging-directive'
                    if (S3Provider.CLOUDFLARE_R2.equals(provider())) {
                        copyObjectOptions = new CopyObjectOptions.CopyObjectOptionsBuilder()
                                .copySource(bucket, objectKey)
                                .excludeTaggingDirective()
                                .configureCopySourceCondition()
                                .ifUnmodifiedSince(new Date(objectLastModified.getTime() - 1000))
                                .endConfigureCondition()
                                .build();
                    }
                    candyS3.copyObject(bucket, objectKey5, copyObjectOptions);
                    Assertions.fail("Should not be here. PreconditionFailed should be thrown.");
                } catch (Exception ex) {
                    Assertions.assertTrue(ex instanceof CandyS3Exception);
                    Assertions.assertEquals(CommonErrorCode.OBJECT_PRECONDITION_FAILED.getCode(), ((CandyS3Exception) ex).getCode());
                    Assertions.assertEquals("PreconditionFailed", ((CandyS3Exception) ex).getParsedError().getCode());
                }

                CopyObjectOptions copyObjectOptions = new CopyObjectOptions.CopyObjectOptionsBuilder()
                        .copySource(bucket, objectKey)
                        .configureCopySourceCondition()
                        .ifUnmodifiedSince(new Date(objectLastModified.getTime() + 1))
                        .endConfigureCondition()
                        .build();
                // Cloudflare R2 does not support 'x-amz-tagging-directive'
                if (S3Provider.CLOUDFLARE_R2.equals(provider())) {
                    copyObjectOptions = new CopyObjectOptions.CopyObjectOptionsBuilder()
                            .copySource(bucket, objectKey)
                            .excludeTaggingDirective()
                            .configureCopySourceCondition()
                            .ifUnmodifiedSince(new Date(objectLastModified.getTime() + 1))
                            .endConfigureCondition()
                            .build();
                }
                candyS3.copyObject(bucket, objectKey5, copyObjectOptions);
            }

        } finally {
            candyS3.deleteObject(bucket, new DeleteObjectOptions(objectKey));
            candyS3.deleteObject(bucket, new DeleteObjectOptions(objectKey2));
            candyS3.deleteObject(bucket, new DeleteObjectOptions(objectKey3));
            candyS3.deleteObject(bucket, new DeleteObjectOptions(objectKey4));
            candyS3.deleteObject(bucket, new DeleteObjectOptions(objectKey5));

            candyS3.deleteBucket(bucket);
        }
    }

    @Test
    @Tag(TestTag.conditional)
    void copyObjectCopySourceUnionConditionTest() throws IOException, NoSuchAlgorithmException {
        CandyS3 candyS3 = init(provider());
        String bucket = genTestBucketName("cOCSUnionCondTest");
        String objectKey = "copyObjectCopySourceUnionConditionTest.data";
        String objectKey6 = "copyObjectCopySourceUnionConditionTest1.data";
        String objectKey7 = "copyObjectCopySourceUnionConditionTest2.data";
        byte[] bytes = "x".getBytes(StandardCharsets.UTF_8);
        try {

            candyS3.createBucket(new CreateBucketOptions.CreateBucketOptionsBuilder(bucket).build());
            candyS3.putObject(bucket, objectKey, new PutObjectOptions.PutObjectOptionsBuilder()
                    .configureUploadData().withData(bytes).endConfigureDataContent()
                    .build());

            ListPaginationResult<S3Object> result =
                    candyS3.listObjects(bucket, new ListObjectOptions().prefix(objectKey).maxKeys(1));
            String objectEtag = result.getResults().get(0).geteTag();
            Date objectLastModified = result.getResults().get(0).getLastModified();


            {
                // If both the x-amz-copy-source-if-match and x-amz-copy-source-if-unmodified-since headers are present in the request and evaluate as follows,
                // x-amz-copy-source-if-match condition evaluates to true and x-amz-copy-source-if-unmodified-since condition evaluates to false
                // Amazon S3 returns 200 OK and copies the data
                CopyObjectOptions copyObjectOptions = new CopyObjectOptions.CopyObjectOptionsBuilder()
                        .copySource(bucket, objectKey)
                        .configureCopySourceCondition()
                        .ifMatch(objectEtag) // true
                        .ifUnmodifiedSince(new Date(objectLastModified.getTime() - 1000)) // false
                        .endConfigureCondition()
                        .build();
                // Cloudflare R2 does not support 'x-amz-tagging-directive'
                if (S3Provider.CLOUDFLARE_R2.equals(provider())) {
                    copyObjectOptions = new CopyObjectOptions.CopyObjectOptionsBuilder()
                            .copySource(bucket, objectKey)
                            .excludeTaggingDirective()
                            .configureCopySourceCondition()
                            .ifMatch(objectEtag) // true
                            .ifUnmodifiedSince(new Date(objectLastModified.getTime() - 1000)) // false
                            .endConfigureCondition()
                            .build();
                }
                candyS3.copyObject(bucket, objectKey6, copyObjectOptions);
            }

            {
                // If both the x-amz-copy-source-if-none-match and x-amz-copy-source-if-modified-since headers are present in the request and evaluate as follows,
                // x-amz-copy-source-if-none-match condition evaluates to false and x-amz-copy-source-if-modified-since condition evaluates to true
                // Amazon S3 returns the 412 Precondition Failed response code
                try {
                    CopyObjectOptions copyObjectOptions = new CopyObjectOptions.CopyObjectOptionsBuilder()
                            .copySource(bucket, objectKey)
                            .configureCopySourceCondition()
                            .ifNoneMatch(objectEtag) // false
                            .ifModifiedSince(new Date(objectLastModified.getTime() - 1000)) // true
                            .endConfigureCondition()
                            .build();
                    // Cloudflare R2 does not support 'x-amz-tagging-directive'
                    if (S3Provider.CLOUDFLARE_R2.equals(provider())) {
                        copyObjectOptions = new CopyObjectOptions.CopyObjectOptionsBuilder()
                                .copySource(bucket, objectKey)
                                .excludeTaggingDirective()
                                .configureCopySourceCondition()
                                .ifNoneMatch(objectEtag) // false
                                .ifModifiedSince(new Date(objectLastModified.getTime() - 1000)) // true
                                .endConfigureCondition()
                                .build();
                    }
                    candyS3.copyObject(bucket, objectKey7, copyObjectOptions);
                    Assertions.fail("Should not be here. PreconditionFailed should be thrown.");
                } catch (Exception ex) {
                    Assertions.assertTrue(ex instanceof CandyS3Exception);
                    Assertions.assertEquals(CommonErrorCode.OBJECT_PRECONDITION_FAILED.getCode(), ((CandyS3Exception) ex).getCode());
                }
            }

        } finally {
            candyS3.deleteObject(bucket, new DeleteObjectOptions(objectKey));
            candyS3.deleteObject(bucket, new DeleteObjectOptions(objectKey6));
            candyS3.deleteObject(bucket, new DeleteObjectOptions(objectKey7));

            candyS3.deleteBucket(bucket);
        }
    }

    @Test
    @Tag(TestTag.conditional)
    void copyObjectWriteTargetConditionalTest() throws IOException, NoSuchAlgorithmException {
        CandyS3 candyS3 = init(provider());
        String bucket = genTestBucketName("cOWTCondTest");
        String sourceKey = "source.data";
        String existsTargetKey = "existsTarget.data";
        String notExistsTargetKey = "notexistsTarget.data";
        byte[] bytes = "x".getBytes(StandardCharsets.UTF_8);
        try {

            candyS3.createBucket(new CreateBucketOptions.CreateBucketOptionsBuilder(bucket).build());
            candyS3.putObject(bucket, sourceKey, new PutObjectOptions.PutObjectOptionsBuilder()
                    .configureUploadData().withData(bytes).endConfigureDataContent()
                    .build());
            candyS3.putObject(bucket, existsTargetKey, new PutObjectOptions.PutObjectOptionsBuilder()
                    .configureUploadData().withData(bytes).endConfigureDataContent()
                    .build());

            ListPaginationResult<S3Object> result =
                    candyS3.listObjects(bucket, new ListObjectOptions().prefix(existsTargetKey).maxKeys(1));
            String existsTargetObjectEtag = result.getResults().get(0).geteTag();

            // write to non-existing object key and use if-none-match condition
            {
                CopyObjectOptions copyObjectOptions = new CopyObjectOptions.CopyObjectOptionsBuilder()
                        .copySource(bucket, sourceKey)
                        .configureTargetWriteCondition()
                        .ifNotExists()
                        .endConfigureCondition()
                        .build();
                // Cloudflare R2 does not support 'x-amz-tagging-directive'
                if (S3Provider.CLOUDFLARE_R2.equals(provider())) {
                    copyObjectOptions = new CopyObjectOptions.CopyObjectOptionsBuilder()
                            .copySource(bucket, sourceKey)
                            .excludeTaggingDirective()
                            .configureTargetWriteCondition().ifNotExists().endConfigureCondition()
                            .build();
                }
                candyS3.copyObject(bucket, notExistsTargetKey, copyObjectOptions);
            }

            // write to existing object key and use if-none-match condition, should throw PreconditionFailed
            {
                try {
                    CopyObjectOptions copyObjectOptions = new CopyObjectOptions.CopyObjectOptionsBuilder()
                            .copySource(bucket, sourceKey)
                            .configureTargetWriteCondition().ifNotExists().endConfigureCondition()
                            .build();
                    // Cloudflare R2 does not support 'x-amz-tagging-directive'
                    if (S3Provider.CLOUDFLARE_R2.equals(provider())) {
                        copyObjectOptions = new CopyObjectOptions.CopyObjectOptionsBuilder()
                                .copySource(bucket, sourceKey)
                                .excludeTaggingDirective()
                                .configureTargetWriteCondition().ifNotExists().endConfigureCondition()
                                .build();
                    }
                    candyS3.copyObject(bucket, existsTargetKey, copyObjectOptions);
                    Assertions.fail("Should not be here. PreconditionFailed should be thrown.");
                } catch (Exception ex) {
                    Assertions.assertTrue(ex instanceof CandyS3Exception);
                    Assertions.assertEquals(CommonErrorCode.OBJECT_PRECONDITION_FAILED.getCode(), ((CandyS3Exception) ex).getCode());
                    Assertions.assertEquals("PreconditionFailed", ((CandyS3Exception) ex).getParsedError().getCode());
                }
            }

            // write to existing object key and use if-match condition
            {
                CopyObjectOptions copyObjectOptions = new CopyObjectOptions.CopyObjectOptionsBuilder()
                        .copySource(bucket, sourceKey)
                        .configureTargetWriteCondition().ifMatch(existsTargetObjectEtag).endConfigureCondition()
                        .build();
                // Cloudflare R2 does not support 'x-amz-tagging-directive'
                if (S3Provider.CLOUDFLARE_R2.equals(provider())) {
                    copyObjectOptions = new CopyObjectOptions.CopyObjectOptionsBuilder()
                            .copySource(bucket, sourceKey)
                            .configureTargetWriteCondition().ifMatch(existsTargetObjectEtag).endConfigureCondition()
                            .excludeTaggingDirective()
                            .build();
                }
                candyS3.copyObject(bucket, existsTargetKey, copyObjectOptions);
            }

            // write to existing object key and use if-match condition, but the ETag values do not match, should throw PreconditionFailed
            {
                try {
                    CopyObjectOptions copyObjectOptions = new CopyObjectOptions.CopyObjectOptionsBuilder()
                            .copySource(bucket, sourceKey)
                            .configureTargetWriteCondition().ifMatch("x").endConfigureCondition()
                            .build();
                    // Cloudflare R2 does not support 'x-amz-tagging-directive'
                    if (S3Provider.CLOUDFLARE_R2.equals(provider())) {
                        copyObjectOptions = new CopyObjectOptions.CopyObjectOptionsBuilder()
                                .copySource(bucket, sourceKey)
                                .excludeTaggingDirective()
                                .configureTargetWriteCondition().ifMatch("x").endConfigureCondition()
                                .build();
                    }
                    candyS3.copyObject(bucket, existsTargetKey, copyObjectOptions);
                    Assertions.fail("Should not be here. PreconditionFailed should be thrown.");
                } catch (Exception ex) {
                    Assertions.assertTrue(ex instanceof CandyS3Exception);
                    Assertions.assertEquals(CommonErrorCode.OBJECT_PRECONDITION_FAILED.getCode(), ((CandyS3Exception) ex).getCode());
                    Assertions.assertEquals("PreconditionFailed", ((CandyS3Exception) ex).getParsedError().getCode());
                }
            }

        } finally {
            candyS3.deleteObject(bucket, new DeleteObjectOptions(sourceKey));
            candyS3.deleteObject(bucket, new DeleteObjectOptions(existsTargetKey));
            candyS3.deleteObject(bucket, new DeleteObjectOptions(notExistsTargetKey));

            candyS3.deleteBucket(bucket);
        }
    }

    @Test
    @Tag(TestTag.versioning)
    void copyObjectVersionTest() throws IOException, NoSuchAlgorithmException {
        CandyS3 candyS3 = init(provider());
        String bucket = genTestBucketName("copyObjectVersionTest");
        String objectKey1 = "copyObjectVersionTest1.data";
        String objectKey2 = "copyObjectVersionTest2.data";
        String content1 = StringUtils.repeat("x", 1024);
        String content2 = StringUtils.repeat("y", 512);
        try {
            candyS3.createBucket(new CreateBucketOptions.CreateBucketOptionsBuilder(bucket).build());
            candyS3.setBucketVersioning(bucket, true);

            candyS3.putObject(bucket, objectKey1, new PutObjectOptions.PutObjectOptionsBuilder()
                    .configureUploadData().withData(content1.getBytes(StandardCharsets.UTF_8)).endConfigureDataContent()
                    .build());
            candyS3.putObject(bucket, objectKey1, new PutObjectOptions.PutObjectOptionsBuilder()
                    .configureUploadData().withData(content2.getBytes(StandardCharsets.UTF_8)).endConfigureDataContent()
                    .build());

            ListPaginationResult<S3ObjectVersion> objectVersions = candyS3.listObjectVersions(bucket, new ListObjectVersionsOptions());

            candyS3.copyObject(bucket, objectKey2, new CopyObjectOptions.CopyObjectOptionsBuilder()
                    .copySource(bucket, objectKey1)
                    .copySourceVersionId(objectVersions.getResults().get(1).getVersionId())
                    .build());

            S3Object downloadObject2 = candyS3.downloadObject(bucket, objectKey2,
                    new DownloadObjectOptions.DownloadObjectOptionsBuilder()
                            .configureDataOutput().toBytes().endConfigureDataOutput()
                            .build());
            Assertions.assertArrayEquals(content1.getBytes(StandardCharsets.UTF_8), downloadObject2.getContentBytes());

        } finally {
            deleteAllObjectVersions(provider(), bucket);
            candyS3.deleteBucket(bucket);
        }
    }

    @Test
    @Tag(TestTag.basic)
    void copyPartTest() throws IOException, NoSuchAlgorithmException {
        CandyS3 candyS3 = init(provider());
        String bucket = genTestBucketName("copyPartTest");
        try {
            candyS3.createBucket(new CreateBucketOptions.CreateBucketOptionsBuilder(bucket).build());

            String sourceObjectKey1 = "sourceObjectKey1.data";
            String sourceContent1 = "123456";
            candyS3.putObject(bucket, sourceObjectKey1, new PutObjectOptions.PutObjectOptionsBuilder()
                    .configureUploadData().withData(sourceContent1.getBytes(StandardCharsets.UTF_8)).endConfigureDataContent()
                    .build());

            String sourceObjectKey2 = "sub/sourceObjectKey2.data";
            String sourceContent2 = StringUtils.repeat('y', 6 * 1024 * 1024) + "7890";
            candyS3.putObject(bucket, sourceObjectKey2, new PutObjectOptions.PutObjectOptionsBuilder()
                    .configureUploadData().withData(sourceContent2.getBytes(StandardCharsets.UTF_8)).endConfigureDataContent()
                    .build());

            String objectKey = "copyPartTest.data";
            String uploadId = candyS3.createMultipartUpload(bucket, objectKey,
                    new CreateMultipartUploadOptions.CreateMultipartUploadOptionsBuilder().build());

            List<S3Part> parts = new ArrayList<>();

            String part1Content = StringUtils.repeat('x', 6 * 1024 * 1024);
            S3Part part1 = candyS3.uploadPart(bucket, objectKey, uploadId, parts.size() + 1, new UploadPartOptions.UploadPartOptionsBuilder()
                    .configureUploadData().withData(part1Content.getBytes(StandardCharsets.UTF_8)).endConfigureDataContent()
                    .build());
            parts.add(part1);

            String part2Etag = candyS3.copyPart(bucket, objectKey, uploadId, parts.size() + 1, new CopyPartOptions.CopyPartOptionsBuilder()
                    .copySource(bucket, sourceObjectKey2)
                    .copySourceRange(1024 * 1024 - 1, sourceContent2.length() - 1)
                    .build());
            S3Part part2 = new S3Part();
            part2.setPartNum(parts.size() + 1);
            part2.setEtag(part2Etag);
            parts.add(part2);

            String part3Etag = candyS3.copyPart(bucket, objectKey, uploadId, parts.size() + 1, new CopyPartOptions.CopyPartOptionsBuilder()
                    .copySource(bucket, sourceObjectKey1)
                    .build());
            S3Part part3 = new S3Part();
            part3.setPartNum(parts.size() + 1);
            part3.setEtag(part3Etag);
            parts.add(part3);

            candyS3.completeMultipartUpload(bucket, objectKey, uploadId, parts, new CompleteMultipartUploadOptions.CompleteMultipartUploadOptionsBuilder()
                    .build());

            String outputFile = "./temp/output.data";
            candyS3.downloadObject(bucket, objectKey, new DownloadObjectOptions.DownloadObjectOptionsBuilder()
                    .configureDataOutput().toFile(outputFile, true).endConfigureDataOutput()
                    .build());

            String exceptContent = part1Content + sourceContent2.substring(1024 * 1024 - 1) + sourceContent1;
            byte[] actualContent = Files.readAllBytes(Paths.get(outputFile));
            Assertions.assertArrayEquals(exceptContent.getBytes(StandardCharsets.UTF_8), actualContent);

            candyS3.deleteObjectsBatch(bucket, new DeleteObjectsBatchOptions.DeleteObjectsBatchOptionsBuilder()
                    .addDeleteObject(sourceObjectKey1)
                    .addDeleteObject(sourceObjectKey2)
                    .addDeleteObject(objectKey)
                    .build());
        } finally {
            candyS3.deleteBucket(bucket);
        }
    }

    @Test
    @Tag(TestTag.conditional)
    void copyPartConditionalTest() throws IOException, NoSuchAlgorithmException {
        CandyS3 candyS3 = init(provider());
        String bucket = genTestBucketName("copyPartConditionalTest");
        try {
            candyS3.createBucket(new CreateBucketOptions.CreateBucketOptionsBuilder(bucket).build());

            String sourceObjectKey = "sourceObjectKey.data";
            String sourceContent = StringUtils.repeat("1", 5 * 1024 * 1024);
            candyS3.putObject(bucket, sourceObjectKey, new PutObjectOptions.PutObjectOptionsBuilder()
                    .configureUploadData().withData(sourceContent.getBytes(StandardCharsets.UTF_8)).endConfigureDataContent()
                    .build());

            String objectKey = "copyPartConditionalTest.data";
            String uploadId = candyS3.createMultipartUpload(bucket, objectKey,
                    new CreateMultipartUploadOptions.CreateMultipartUploadOptionsBuilder().build());

            List<S3Part> parts = new ArrayList<>();

            String part1Content = StringUtils.repeat('x', 6 * 1024 * 1024);
            S3Part part1 = candyS3.uploadPart(bucket, objectKey, uploadId, parts.size() + 1, new UploadPartOptions.UploadPartOptionsBuilder()
                    .configureUploadData().withData(part1Content.getBytes(StandardCharsets.UTF_8)).endConfigureDataContent()
                    .build());
            parts.add(part1);

            {

                List<S3Object> s3Objects = candyS3.listObjects(bucket, new ListObjectOptions().prefix(sourceObjectKey).maxKeys(1)).getResults();
                S3Object sourceObject = s3Objects.get(0);

                {
                    try {
                        candyS3.copyPart(bucket, objectKey, uploadId, parts.size() + 1, new CopyPartOptions.CopyPartOptionsBuilder()
                                .copySource(bucket, sourceObjectKey)
                                .configureCopySourceCondition()
                                .ifMatch("x").endConfigureCondition()
                                .build());
                        Assertions.fail("Should not be here. PreconditionFailed should be thrown.");
                    } catch (Exception ex) {
                        Assertions.assertTrue(ex instanceof CandyS3Exception);
                        Assertions.assertEquals(CommonErrorCode.OBJECT_PRECONDITION_FAILED.getCode(), ((CandyS3Exception) ex).getCode());
                        Assertions.assertEquals("PreconditionFailed", ((CandyS3Exception) ex).getParsedError().getCode());
                    }

                    String partEtag = candyS3.copyPart(bucket, objectKey, uploadId, parts.size() + 1, new CopyPartOptions.CopyPartOptionsBuilder()
                            .copySource(bucket, sourceObjectKey)
                            .configureCopySourceCondition()
                            .ifMatch(sourceObject.geteTag())
                            .endConfigureCondition()
                            .build());
                    S3Part part = new S3Part();
                    part.setPartNum(parts.size() + 1);
                    part.setEtag(partEtag);
                    parts.add(part);
                }

                {
                    try {
                        candyS3.copyPart(bucket, objectKey, uploadId, parts.size() + 1, new CopyPartOptions.CopyPartOptionsBuilder()
                                .copySource(bucket, sourceObjectKey)
                                .configureCopySourceCondition().ifNoneMatch(sourceObject.geteTag()).endConfigureCondition()
                                .build());
                        Assertions.fail("Should not be here. PreconditionFailed should be thrown.");
                    } catch (Exception ex) {
                        Assertions.assertTrue(ex instanceof CandyS3Exception);
                        if (S3Provider.ALIYUN_OSS.equals(provider())) {
                            Assertions.assertEquals("Not Modified", ((CandyS3Exception) ex).getParsedError().getMessage());
                        } else {
                            Assertions.assertEquals(CommonErrorCode.OBJECT_PRECONDITION_FAILED.getCode(), ((CandyS3Exception) ex).getCode());
                            Assertions.assertEquals("PreconditionFailed", ((CandyS3Exception) ex).getParsedError().getCode());
                        }
                    }

                    String partEtag = candyS3.copyPart(bucket, objectKey, uploadId, parts.size() + 1, new CopyPartOptions.CopyPartOptionsBuilder()
                            .copySource(bucket, sourceObjectKey)
                            .configureCopySourceCondition()
                            .ifNoneMatch("x")
                            .endConfigureCondition()
                            .build());
                    S3Part part = new S3Part();
                    part.setPartNum(parts.size() + 1);
                    part.setEtag(partEtag);
                    parts.add(part);
                }

                {
                    try {
                        candyS3.copyPart(bucket, objectKey, uploadId, parts.size() + 1, new CopyPartOptions.CopyPartOptionsBuilder()
                                .copySource(bucket, sourceObjectKey)
                                .configureCopySourceCondition().ifModifiedSince(sourceObject.getLastModified()).endConfigureCondition()
                                .build());
                        Assertions.fail("Should not be here. PreconditionFailed should be thrown.");
                    } catch (Exception ex) {
                        Assertions.assertTrue(ex instanceof CandyS3Exception);
                        if (S3Provider.ALIYUN_OSS.equals(provider())) {
                            Assertions.assertEquals("Not Modified", ((CandyS3Exception) ex).getParsedError().getMessage());
                        } else {
                            Assertions.assertEquals(CommonErrorCode.OBJECT_PRECONDITION_FAILED.getCode(), ((CandyS3Exception) ex).getCode());
                            Assertions.assertEquals("PreconditionFailed", ((CandyS3Exception) ex).getParsedError().getCode());
                        }
                    }
                    String partEtag = candyS3.copyPart(bucket, objectKey, uploadId, parts.size() + 1, new CopyPartOptions.CopyPartOptionsBuilder()
                            .copySource(bucket, sourceObjectKey)
                            .configureCopySourceCondition()
                            .ifModifiedSince(new Date(sourceObject.getLastModified().getTime() - 1000))
                            .endConfigureCondition()
                            .build());

                    S3Part part = new S3Part();
                    part.setPartNum(parts.size() + 1);
                    part.setEtag(partEtag);
                    parts.add(part);
                }

                {
                    try {
                        candyS3.copyPart(bucket, objectKey, uploadId, parts.size() + 1, new CopyPartOptions.CopyPartOptionsBuilder()
                                .copySource(bucket, sourceObjectKey)
                                .configureCopySourceCondition()
                                .ifUnmodifiedSince(new Date(sourceObject.getLastModified().getTime() - 1000))
                                .endConfigureCondition()
                                .build());
                        Assertions.fail("Should not be here. PreconditionFailed should be thrown.");
                    } catch (Exception ex) {
                        Assertions.assertTrue(ex instanceof CandyS3Exception);
                        Assertions.assertEquals(CommonErrorCode.OBJECT_PRECONDITION_FAILED.getCode(), ((CandyS3Exception) ex).getCode());
                        Assertions.assertEquals("PreconditionFailed", ((CandyS3Exception) ex).getParsedError().getCode());
                    }
                    String partEtag = candyS3.copyPart(bucket, objectKey, uploadId, parts.size() + 1, new CopyPartOptions.CopyPartOptionsBuilder()
                            .copySource(bucket, sourceObjectKey)
                            .configureCopySourceCondition()
                            .ifUnmodifiedSince(new Date(sourceObject.getLastModified().getTime() + 1))
                            .endConfigureCondition()
                            .build());

                    S3Part part = new S3Part();
                    part.setPartNum(parts.size() + 1);
                    part.setEtag(partEtag);
                    parts.add(part);
                }

            }

            candyS3.completeMultipartUpload(bucket, objectKey, uploadId, parts, new CompleteMultipartUploadOptions.CompleteMultipartUploadOptionsBuilder()
                    .build());

            String outputFile = "./temp/output.txt";
            candyS3.downloadObject(bucket, objectKey, new DownloadObjectOptions.DownloadObjectOptionsBuilder()
                    .configureDataOutput().toFile(outputFile, true).endConfigureDataOutput()
                    .build());

            String exceptContent = part1Content + sourceContent + sourceContent + sourceContent + sourceContent;
            byte[] actualContent = Files.readAllBytes(Paths.get(outputFile));
            Assertions.assertArrayEquals(exceptContent.getBytes(StandardCharsets.UTF_8), actualContent);

            Files.delete(Paths.get(outputFile));

            candyS3.deleteObjectsBatch(bucket, new DeleteObjectsBatchOptions.DeleteObjectsBatchOptionsBuilder()
                    .addDeleteObject(sourceObjectKey)
                    .addDeleteObject(objectKey)
                    .build());
        } finally {
            candyS3.deleteBucket(bucket);
        }
    }

    @Test
    @Tag(TestTag.conditional)
    void copyPartWithUnionConditionTest() throws IOException, NoSuchAlgorithmException {
        CandyS3 candyS3 = init(provider());
        String bucket = genTestBucketName("copyPartWithUnionConditionTest");
        try {
            candyS3.createBucket(new CreateBucketOptions.CreateBucketOptionsBuilder(bucket).build());

            String sourceObjectKey = "sourceObjectKey.data";
            String sourceContent = StringUtils.repeat("1", 5 * 1024 * 1024);
            candyS3.putObject(bucket, sourceObjectKey, new PutObjectOptions.PutObjectOptionsBuilder()
                    .configureUploadData().withData(sourceContent.getBytes(StandardCharsets.UTF_8)).endConfigureDataContent()
                    .build());

            String objectKey = "copyPartWithUnionConditionTest.data";
            String uploadId = candyS3.createMultipartUpload(bucket, objectKey,
                    new CreateMultipartUploadOptions.CreateMultipartUploadOptionsBuilder().build());

            List<S3Part> parts = new ArrayList<>();

            String part1Content = StringUtils.repeat('x', 6 * 1024 * 1024);
            S3Part part1 = candyS3.uploadPart(bucket, objectKey, uploadId, parts.size() + 1, new UploadPartOptions.UploadPartOptionsBuilder()
                    .configureUploadData().withData(part1Content.getBytes(StandardCharsets.UTF_8)).endConfigureDataContent()
                    .build());
            parts.add(part1);

            {

                List<S3Object> s3Objects = candyS3.listObjects(bucket, new ListObjectOptions().prefix(sourceObjectKey).maxKeys(1)).getResults();
                S3Object sourceObject = s3Objects.get(0);

                {
                    // If both the x-amz-copy-source-if-match and x-amz-copy-source-if-unmodified-since headers are present in the request and evaluate as follows,
                    // x-amz-copy-source-if-match condition evaluates to true and x-amz-copy-source-if-unmodified-since condition evaluates to false
                    // Amazon S3 returns 200 OK and copies the data
                    String partEtag = candyS3.copyPart(bucket, objectKey, uploadId, parts.size() + 1, new CopyPartOptions.CopyPartOptionsBuilder()
                            .copySource(bucket, sourceObjectKey)
                            .configureCopySourceCondition()
                            .ifMatch(sourceObject.geteTag()) // true
                            .ifUnmodifiedSince(new Date(sourceObject.getLastModified().getTime() - 1000)) // false
                            .endConfigureCondition()
                            .build());

                    S3Part part = new S3Part();
                    part.setPartNum(parts.size() + 1);
                    part.setEtag(partEtag);
                    parts.add(part);
                }

                {
                    // If both the x-amz-copy-source-if-none-match and x-amz-copy-source-if-modified-since headers are present in the request and evaluate as follows,
                    // x-amz-copy-source-if-none-match condition evaluates to false and x-amz-copy-source-if-modified-since condition evaluates to true
                    // Amazon S3 returns the 412 Precondition Failed response code
                    try {
                        candyS3.copyPart(bucket, objectKey, uploadId, parts.size() + 1, new CopyPartOptions.CopyPartOptionsBuilder()
                                .copySource(bucket, sourceObjectKey)
                                .configureCopySourceCondition()
                                .ifNoneMatch(sourceObject.geteTag()) // false
                                .ifModifiedSince(new Date(sourceObject.getLastModified().getTime() - 1000)) // true
                                .endConfigureCondition()
                                .build());
                        Assertions.fail("Should not be here. PreconditionFailed should be thrown.");
                    } catch (Exception ex) {
                        Assertions.assertTrue(ex instanceof CandyS3Exception);
                        Assertions.assertEquals(CommonErrorCode.OBJECT_PRECONDITION_FAILED.getCode(), ((CandyS3Exception) ex).getCode());
                        Assertions.assertEquals("PreconditionFailed", ((CandyS3Exception) ex).getParsedError().getCode());
                    }
                }

            }

            candyS3.completeMultipartUpload(bucket, objectKey, uploadId, parts, new CompleteMultipartUploadOptions.CompleteMultipartUploadOptionsBuilder()
                    .build());

            String outputFile = "./temp/output.txt";
            candyS3.downloadObject(bucket, objectKey, new DownloadObjectOptions.DownloadObjectOptionsBuilder()
                    .configureDataOutput().toFile(outputFile, true).endConfigureDataOutput()
                    .build());

            String exceptContent = part1Content + sourceContent;
            byte[] actualContent = Files.readAllBytes(Paths.get(outputFile));
            Assertions.assertArrayEquals(exceptContent.getBytes(StandardCharsets.UTF_8), actualContent);

            Files.delete(Paths.get(outputFile));

            candyS3.deleteObjectsBatch(bucket, new DeleteObjectsBatchOptions.DeleteObjectsBatchOptionsBuilder()
                    .addDeleteObject(sourceObjectKey)
                    .addDeleteObject(objectKey)
                    .build());
        } finally {
            candyS3.deleteBucket(bucket);
        }
    }


    @Test
    @Tag(TestTag.versioning)
    void copyPartVersionTest() throws IOException, NoSuchAlgorithmException {
        CandyS3 candyS3 = init(provider());
        String bucket = genTestBucketName("copyPartVersionTest");
        try {
            candyS3.createBucket(new CreateBucketOptions.CreateBucketOptionsBuilder(bucket).build());
            candyS3.setBucketVersioning(bucket, true);

            String sourceObjectKey = "sourceObjectKey.data";
            String sourceContentV1 = StringUtils.repeat("x", 5 * 1024 * 1024);
            candyS3.putObject(bucket, sourceObjectKey, new PutObjectOptions.PutObjectOptionsBuilder()
                    .configureUploadData().withData(sourceContentV1.getBytes(StandardCharsets.UTF_8)).endConfigureDataContent()
                    .build());
            String sourceContentV2 = StringUtils.repeat("y", 5 * 1024 * 1024);
            candyS3.putObject(bucket, sourceObjectKey, new PutObjectOptions.PutObjectOptionsBuilder()
                    .configureUploadData().withData(sourceContentV2.getBytes(StandardCharsets.UTF_8)).endConfigureDataContent()
                    .build());

            List<S3ObjectVersion> sourceObjectVersions = candyS3.listObjectVersions(bucket, new ListObjectVersionsOptions()
                            .prefix(sourceObjectKey))
                    .getResults();

            String objectKey = "copyPartVersionTest.data";
            String uploadId = candyS3.createMultipartUpload(bucket, objectKey,
                    new CreateMultipartUploadOptions.CreateMultipartUploadOptionsBuilder().build());

            List<S3Part> parts = new ArrayList<>();

            String part1Content = StringUtils.repeat('z', 6 * 1024 * 1024);
            S3Part part1 = candyS3.uploadPart(bucket, objectKey, uploadId, 1, new UploadPartOptions.UploadPartOptionsBuilder()
                    .configureUploadData().withData(part1Content.getBytes(StandardCharsets.UTF_8)).endConfigureDataContent()
                    .build());
            parts.add(part1);

            Assertions.assertFalse(sourceObjectVersions.get(1).getIsLatest());
            String part2Etag = candyS3.copyPart(bucket, objectKey, uploadId, 2, new CopyPartOptions.CopyPartOptionsBuilder()
                    .copySource(bucket, sourceObjectKey, sourceObjectVersions.get(1).getVersionId())
                    .build());
            S3Part part2 = new S3Part();
            part2.setPartNum(2);
            part2.setEtag(part2Etag);
            parts.add(part2);

            Assertions.assertTrue(sourceObjectVersions.get(0).getIsLatest());
            String part3Etag = candyS3.copyPart(bucket, objectKey, uploadId, 3, new CopyPartOptions.CopyPartOptionsBuilder()
                    .copySource(bucket, sourceObjectKey)
                    .build());
            S3Part part3 = new S3Part();
            part3.setPartNum(3);
            part3.setEtag(part3Etag);
            parts.add(part3);

            candyS3.completeMultipartUpload(bucket, objectKey, uploadId, parts, new CompleteMultipartUploadOptions.CompleteMultipartUploadOptionsBuilder()
                    .build());

            S3Object downloadObject = candyS3.downloadObject(bucket, objectKey, new DownloadObjectOptions.DownloadObjectOptionsBuilder()
                    .configureDataOutput().toBytes().endConfigureDataOutput()
                    .build());

            String exceptContent = part1Content + sourceContentV1 + sourceContentV2;
            Assertions.assertArrayEquals(exceptContent.getBytes(StandardCharsets.UTF_8), downloadObject.getContentBytes());

        } finally {
            deleteAllObjectVersions(provider(), bucket);
            candyS3.deleteBucket(bucket);
        }
    }


    @Test
    @Tag(TestTag.basic)
    void downloadObjectTest() throws IOException, NoSuchAlgorithmException {
        CandyS3 candyS3 = init(provider());
        String bucket = genTestBucketName("downloadObjectTest");
        String objectKey = "downloadObjectTest.data";
        String content = StringUtils.repeat("x", 6 * 1024 * 1024); // 6MB
        try {
            candyS3.createBucket(new CreateBucketOptions.CreateBucketOptionsBuilder(bucket).build());

            candyS3.putObject(bucket, objectKey, new PutObjectOptions.PutObjectOptionsBuilder()
                    .configureUploadData()
                    .withData(content.getBytes(StandardCharsets.UTF_8))
                    .endConfigureDataContent()
                    .build());

            {
                String downloadToFile = "./temp/downloadToFile.data";
                // Download object and write to local file.
                S3Object s3Object = candyS3.downloadObject(bucket, objectKey,
                        new DownloadObjectOptions.DownloadObjectOptionsBuilder()
                                .configureDataOutput().toFile(downloadToFile, false).endConfigureDataOutput()
                                .build());
                Assertions.assertNull(s3Object.getContentBytes());

                byte[] downloadToFileBytes = Files.readAllBytes(Paths.get(downloadToFile));
                Files.deleteIfExists(Paths.get(downloadToFile));
                Assertions.assertArrayEquals(content.getBytes(StandardCharsets.UTF_8), downloadToFileBytes);
            }

            {
                ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
                // Download object and write to outStream.
                S3Object s3Object = candyS3.downloadObject(bucket, objectKey,
                        new DownloadObjectOptions.DownloadObjectOptionsBuilder()
                                .configureDataOutput().toStream(outputStream).endConfigureDataOutput()
                                .build());
                Assertions.assertNull(s3Object.getContentBytes());
                Assertions.assertArrayEquals(content.getBytes(StandardCharsets.UTF_8), outputStream.toByteArray());
            }

            {
                // Download object and record to byte array.
                S3Object s3Object = candyS3.downloadObject(bucket, objectKey,
                        new DownloadObjectOptions.DownloadObjectOptionsBuilder()
                                .configureDataOutput().toBytes().endConfigureDataOutput()
                                .build());
                Assertions.assertArrayEquals(content.getBytes(StandardCharsets.UTF_8), s3Object.getContentBytes());
            }

        } finally {
            candyS3.deleteObject(bucket, new DeleteObjectOptions(objectKey));
            candyS3.deleteBucket(bucket);
        }
    }

    @Test
    @Tag(TestTag.basic)
    void downloadObjectRangeTest() throws IOException, NoSuchAlgorithmException {
        CandyS3 candyS3 = init(provider());
        String bucket = genTestBucketName("downloadObjectRangeTest");
        String objectKey = "downloadObjectRangeTest.data";
        String content = "0123456789";
        try {
            candyS3.createBucket(new CreateBucketOptions.CreateBucketOptionsBuilder(bucket).build());

            candyS3.putObject(bucket, objectKey, new PutObjectOptions.PutObjectOptionsBuilder()
                    .configureUploadData()
                    .withData(content.getBytes(StandardCharsets.UTF_8))
                    .endConfigureDataContent()
                    .build());

            {
                String downloadRangeToFile1 = "./temp/downloadRangeToFile1.data";
                // Download object range and write to local file.
                S3Object s3ObjectRange = candyS3.downloadObject(bucket, objectKey,
                        new DownloadObjectOptions.DownloadObjectOptionsBuilder()
                                .range(2, 5)
                                .configureDataOutput().toFile(downloadRangeToFile1, false).endConfigureDataOutput()
                                .build());

                byte[] downloadRangeToFileBytes1 = Files.readAllBytes(Paths.get(downloadRangeToFile1));
                Files.deleteIfExists(Paths.get(downloadRangeToFile1));
                Assertions.assertEquals("bytes 2-5/10", s3ObjectRange.getObjectMetadata().getContentRange());
                Assertions.assertArrayEquals(Arrays.copyOfRange(content.getBytes(StandardCharsets.UTF_8), 2, 6), downloadRangeToFileBytes1);
            }
            {
                String downloadRangeToFile2 = "./temp/downloadRangeToFile2.data";
                // Download object range and write to local file.
                S3Object s3ObjectRange = candyS3.downloadObject(bucket, objectKey,
                        new DownloadObjectOptions.DownloadObjectOptionsBuilder()
                                .range(2)
                                .configureDataOutput().toFile(downloadRangeToFile2, false).endConfigureDataOutput()
                                .build());

                byte[] downloadRangeToFileBytes2 = Files.readAllBytes(Paths.get(downloadRangeToFile2));
                Files.deleteIfExists(Paths.get(downloadRangeToFile2));
                Assertions.assertEquals("bytes 2-9/10", s3ObjectRange.getObjectMetadata().getContentRange());
                Assertions.assertArrayEquals(Arrays.copyOfRange(content.getBytes(StandardCharsets.UTF_8), 2, content.getBytes().length), downloadRangeToFileBytes2);
            }

            {
                ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
                // Download object range and write to outStream.
                S3Object s3ObjectRange = candyS3.downloadObject(bucket, objectKey,
                        new DownloadObjectOptions.DownloadObjectOptionsBuilder()
                                .range(2, 5)
                                .configureDataOutput().toStream(outputStream).endConfigureDataOutput()
                                .build());
                Assertions.assertEquals("bytes 2-5/10", s3ObjectRange.getObjectMetadata().getContentRange());
                Assertions.assertArrayEquals(Arrays.copyOfRange(content.getBytes(StandardCharsets.UTF_8), 2, 6), outputStream.toByteArray());
            }
            {
                ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
                // Download object range and write to outStream.
                S3Object s3ObjectRange = candyS3.downloadObject(bucket, objectKey,
                        new DownloadObjectOptions.DownloadObjectOptionsBuilder()
                                .range(2)
                                .configureDataOutput().toStream(outputStream).endConfigureDataOutput()
                                .build());
                Assertions.assertEquals("bytes 2-9/10", s3ObjectRange.getObjectMetadata().getContentRange());
                Assertions.assertArrayEquals(Arrays.copyOfRange(content.getBytes(StandardCharsets.UTF_8), 2, content.getBytes().length), outputStream.toByteArray());
            }

            {
                // Download object range and record to byte array.
                S3Object s3ObjectRange = candyS3.downloadObject(bucket, objectKey,
                        new DownloadObjectOptions.DownloadObjectOptionsBuilder()
                                .range(2, 5)
                                .configureDataOutput().toBytes().endConfigureDataOutput()
                                .build());
                Assertions.assertEquals("bytes 2-5/10", s3ObjectRange.getObjectMetadata().getContentRange());
                Assertions.assertArrayEquals(Arrays.copyOfRange(content.getBytes(StandardCharsets.UTF_8), 2, 6), s3ObjectRange.getContentBytes());
            }
            {
                // Download object range and record to byte array.
                S3Object s3ObjectRange = candyS3.downloadObject(bucket, objectKey,
                        new DownloadObjectOptions.DownloadObjectOptionsBuilder()
                                .range(2)
                                .configureDataOutput().toBytes().endConfigureDataOutput()
                                .build());
                Assertions.assertEquals("bytes 2-9/10", s3ObjectRange.getObjectMetadata().getContentRange());
                Assertions.assertArrayEquals(Arrays.copyOfRange(content.getBytes(StandardCharsets.UTF_8), 2, content.getBytes().length), s3ObjectRange.getContentBytes());
            }

        } finally {
            candyS3.deleteObject(bucket, new DeleteObjectOptions(objectKey));
            candyS3.deleteBucket(bucket);
        }
    }

    @Test
    @Tag(TestTag.basic)
    void downloadObjectOverwriteTest() throws IOException, NoSuchAlgorithmException {
        CandyS3 candyS3 = init(provider());
        String bucket = genTestBucketName("downloadObjectOverwriteTest");
        String objectKey = "downloadObjectOverwriteTest.data";
        String content = StringUtils.repeat("x", 6 * 1024 * 1024); // 6MB
        try {
            candyS3.createBucket(new CreateBucketOptions.CreateBucketOptionsBuilder(bucket).build());

            candyS3.putObject(bucket, objectKey, new PutObjectOptions.PutObjectOptionsBuilder()
                    .configureUploadData().withData(content.getBytes(StandardCharsets.UTF_8)).endConfigureDataContent()
                    .build());

            String downloadToFileExists = "./temp/downloadToFileExists.data";

            {
                File file = new File(downloadToFileExists);
                Files.deleteIfExists(Paths.get(downloadToFileExists));
                file.createNewFile();
                Assertions.assertTrue(file.exists());

                S3Object s3Object = candyS3.downloadObject(bucket, objectKey,
                        new DownloadObjectOptions.DownloadObjectOptionsBuilder()
                                .configureDataOutput()
                                .toFile(downloadToFileExists, true)
                                .endConfigureDataOutput()
                                .build());
                Assertions.assertNull(s3Object.getContentBytes());

                byte[] downloadToFileBytes = Files.readAllBytes(Paths.get(downloadToFileExists));
                Files.deleteIfExists(Paths.get(downloadToFileExists));
                Assertions.assertArrayEquals(content.getBytes(StandardCharsets.UTF_8), downloadToFileBytes);
            }

            {
                File file = new File(downloadToFileExists);
                Files.deleteIfExists(Paths.get(downloadToFileExists));
                file.createNewFile();
                Assertions.assertTrue(file.exists());

                try {
                    candyS3.downloadObject(bucket, objectKey,
                            new DownloadObjectOptions.DownloadObjectOptionsBuilder()
                                    .configureDataOutput()
                                    .toFile(downloadToFileExists, false)
                                    .endConfigureDataOutput()
                                    .build());
                } catch (Exception ex) {
                    Assertions.assertTrue(ex instanceof CandyS3Exception);
                    Assertions.assertEquals(CommonErrorCode.OUTPUT_FILE_ALREADY_EXISTS.getCode(), ((CandyS3Exception) ex).getCode());
                }
            }

        } finally {
            candyS3.deleteObject(bucket, new DeleteObjectOptions(objectKey));
            candyS3.deleteBucket(bucket);
        }
    }

    @Test
    @Tag(TestTag.conditional)
    void downloadObjectConditionalTest() throws IOException, NoSuchAlgorithmException {
        CandyS3 candyS3 = init(provider());
        String bucket = genTestBucketName("downloadObjectConditionalTest");
        String objectKey = "downloadObjectConditionalTest.data";
        byte[] bytes = "x".getBytes(StandardCharsets.UTF_8);
        try {
            candyS3.createBucket(new CreateBucketOptions.CreateBucketOptionsBuilder(bucket).build());
            candyS3.putObject(bucket, objectKey, new PutObjectOptions.PutObjectOptionsBuilder()
                    .configureUploadData().withData(bytes).endConfigureDataContent()
                    .build());

            ListPaginationResult<S3Object> result =
                    candyS3.listObjects(bucket, new ListObjectOptions().prefix(objectKey).maxKeys(1));
            String objectEtag = result.getResults().get(0).geteTag();
            Date objectLastModified = result.getResults().get(0).getLastModified();
            {
                try {
                    candyS3.downloadObject(bucket, objectKey,
                            new DownloadObjectOptions.DownloadObjectOptionsBuilder()
                                    .configureDataOutput().toBytes().endConfigureDataOutput()
                                    .configureDownloadCondition().ifMatch("x").endConfigureCondition()
                                    .build());
                    Assertions.fail("Should not be here. PreconditionFailed should be thrown.");
                } catch (Exception ex) {
                    Assertions.assertTrue(ex instanceof CandyS3Exception);
                    Assertions.assertEquals(CommonErrorCode.OBJECT_PRECONDITION_FAILED.getCode(), ((CandyS3Exception) ex).getCode());
                    Assertions.assertEquals("PreconditionFailed", ((CandyS3Exception) ex).getParsedError().getCode());
                }
                S3Object s3Object = candyS3.downloadObject(bucket, objectKey,
                        new DownloadObjectOptions.DownloadObjectOptionsBuilder()
                                .configureDataOutput().toBytes().endConfigureDataOutput()
                                .configureDownloadCondition().ifMatch(objectEtag).endConfigureCondition()
                                .build());
                Assertions.assertArrayEquals(bytes, s3Object.getContentBytes());
            }

            {
                try {
                    candyS3.downloadObject(bucket, objectKey, new DownloadObjectOptions.DownloadObjectOptionsBuilder()
                            .configureDataOutput().toBytes().endConfigureDataOutput()
                            .configureDownloadCondition().ifNoneMatch(objectEtag).endConfigureCondition()
                            .build());
                    Assertions.fail("Should not be here. PreconditionFailed should be thrown.");
                } catch (Exception ex) {
                    Assertions.assertTrue(ex instanceof CandyS3Exception);
                    Assertions.assertEquals(CommonErrorCode.OBJECT_NOT_MODIFIED.getCode(), ((CandyS3Exception) ex).getCode());
                }
                S3Object s3Object = candyS3.downloadObject(bucket, objectKey,
                        new DownloadObjectOptions.DownloadObjectOptionsBuilder()
                                .configureDataOutput().toBytes().endConfigureDataOutput()
                                .configureDownloadCondition().ifNoneMatch("x").endConfigureCondition()
                                .build());
                Assertions.assertArrayEquals(bytes, s3Object.getContentBytes());
            }

            {
                try {
                    candyS3.downloadObject(bucket, objectKey, new DownloadObjectOptions.DownloadObjectOptionsBuilder()
                            .configureDataOutput().toBytes().endConfigureDataOutput()
                            .configureDownloadCondition().ifModifiedSince(objectLastModified).endConfigureCondition()
                            .build());
                    Assertions.fail("Should not be here. PreconditionFailed should be thrown.");
                } catch (Exception ex) {
                    Assertions.assertTrue(ex instanceof CandyS3Exception);
                    Assertions.assertEquals(CommonErrorCode.OBJECT_NOT_MODIFIED.getCode(), ((CandyS3Exception) ex).getCode());
                }
                S3Object s3Object = candyS3.downloadObject(bucket, objectKey,
                        new DownloadObjectOptions.DownloadObjectOptionsBuilder()
                                .configureDataOutput().toBytes().endConfigureDataOutput()
                                .configureDownloadCondition()
                                .ifModifiedSince(new Date(objectLastModified.getTime() - 1000))
                                .endConfigureCondition()
                                .build());
                Assertions.assertArrayEquals(bytes, s3Object.getContentBytes());
            }

            {
                try {
                    candyS3.downloadObject(bucket, objectKey, new DownloadObjectOptions.DownloadObjectOptionsBuilder()
                            .configureDataOutput().toBytes().endConfigureDataOutput()
                            .configureDownloadCondition()
                            .ifUnmodifiedSince(new Date(objectLastModified.getTime() - 1000))
                            .endConfigureCondition()
                            .build());
                    Assertions.fail("Should not be here. PreconditionFailed should be thrown.");
                } catch (Exception ex) {
                    Assertions.assertTrue(ex instanceof CandyS3Exception);
                    Assertions.assertEquals(CommonErrorCode.OBJECT_PRECONDITION_FAILED.getCode(), ((CandyS3Exception) ex).getCode());
                    Assertions.assertEquals("PreconditionFailed", ((CandyS3Exception) ex).getParsedError().getCode());
                }
                S3Object s3Object = candyS3.downloadObject(bucket, objectKey,
                        new DownloadObjectOptions.DownloadObjectOptionsBuilder()
                                .configureDataOutput().toBytes().endConfigureDataOutput()
                                .configureDownloadCondition()
                                .ifUnmodifiedSince(new Date(objectLastModified.getTime() + 1))
                                .endConfigureCondition()
                                .build());
                Assertions.assertArrayEquals(bytes, s3Object.getContentBytes());
            }

        } finally {
            candyS3.deleteObject(bucket, new DeleteObjectOptions(objectKey));
            candyS3.deleteBucket(bucket);
        }
    }

    @Test
    @Tag(TestTag.conditional)
    void downloadObjectUnionConditionTest() throws IOException, NoSuchAlgorithmException {
        CandyS3 candyS3 = init(provider());
        String bucket = genTestBucketName("dOUConditionTest");
        String objectKey = "downloadObjectUnionConditionTest.data";
        byte[] bytes = "x".getBytes(StandardCharsets.UTF_8);
        try {
            candyS3.createBucket(new CreateBucketOptions.CreateBucketOptionsBuilder(bucket).build());
            candyS3.putObject(bucket, objectKey, new PutObjectOptions.PutObjectOptionsBuilder()
                    .configureUploadData().withData(bytes).endConfigureDataContent()
                    .build());

            ListPaginationResult<S3Object> result =
                    candyS3.listObjects(bucket, new ListObjectOptions().prefix(objectKey).maxKeys(1));
            String objectEtag = result.getResults().get(0).geteTag();
            Date objectLastModified = result.getResults().get(0).getLastModified();


            {
                // If both of the If-Match and If-Unmodified-Since headers are present in the request as follows:
                // If-Match condition evaluates to true, and If-Unmodified-Since condition evaluates to false
                // then, S3 returns 200 OK and the data requested.
                S3Object s3Object = candyS3.downloadObject(bucket, objectKey,
                        new DownloadObjectOptions.DownloadObjectOptionsBuilder()
                                .configureDataOutput().toBytes().endConfigureDataOutput()
                                .configureDownloadCondition()
                                .ifMatch(objectEtag) // true
                                .ifUnmodifiedSince(new Date(objectLastModified.getTime() - 1000)) // false
                                .endConfigureCondition()
                                .build());
                Assertions.assertArrayEquals(bytes, s3Object.getContentBytes());
            }

            {
                // If both of the If-None-Match and If-Modified-Since headers are present in the request as follows:
                // If-None-Match condition evaluates to false, and If-Modified-Since condition evaluates to true
                // then, S3 returns 304 Not Modified status code.
                try {
                    candyS3.downloadObject(bucket, objectKey, new DownloadObjectOptions.DownloadObjectOptionsBuilder()
                            .configureDataOutput().toBytes().endConfigureDataOutput()
                            .configureDownloadCondition()
                            .ifNoneMatch(objectEtag) // false
                            .ifModifiedSince(new Date(objectLastModified.getTime() - 1000)) // true
                            .endConfigureCondition()
                            .build());
                    Assertions.fail("Should not be here. PreconditionFailed should be thrown.");
                } catch (Exception ex) {
                    Assertions.assertTrue(ex instanceof CandyS3Exception);
                    Assertions.assertEquals(CommonErrorCode.OBJECT_NOT_MODIFIED.getCode(), ((CandyS3Exception) ex).getCode());
                }
            }

        } finally {
            candyS3.deleteObject(bucket, new DeleteObjectOptions(objectKey));
            candyS3.deleteBucket(bucket);
        }
    }

    @Test
    @Tag(TestTag.basic)
    void downloadObjectPartTest() throws IOException, NoSuchAlgorithmException {
        CandyS3 candyS3 = init(provider());
        String bucket = genTestBucketName("downloadObjectPartTest");
        String objectKey = "downloadObjectPartTest.data";
        try {
            candyS3.createBucket(new CreateBucketOptions.CreateBucketOptionsBuilder(bucket).build());

            String uploadId = candyS3.createMultipartUpload(bucket, objectKey, new CreateMultipartUploadOptions.CreateMultipartUploadOptionsBuilder().build());
            String part1Content = StringUtils.repeat("1", 5 * 1024 * 1024);
            S3Part part1 = candyS3.uploadPart(bucket, objectKey, uploadId, 1, new UploadPartOptions.UploadPartOptionsBuilder()
                    .configureUploadData()
                    .withData(part1Content.getBytes(StandardCharsets.UTF_8))
                    .endConfigureDataContent()
                    .build());
            String part2Content = "xyz";
            S3Part part2 = candyS3.uploadPart(bucket, objectKey, uploadId, 2, new UploadPartOptions.UploadPartOptionsBuilder()
                    .configureUploadData()
                    .withData(part2Content.getBytes(StandardCharsets.UTF_8))
                    .endConfigureDataContent()
                    .build());
            candyS3.completeMultipartUpload(bucket, objectKey, uploadId, Arrays.asList(part1, part2),
                    new CompleteMultipartUploadOptions.CompleteMultipartUploadOptionsBuilder().build());

            {
                String downloadRangeToFile1 = "./temp/downloadObjectPartTest1.data";
                // Download object part and write to local file.
                S3Object s3ObjectPart = candyS3.downloadObject(bucket, objectKey,
                        new DownloadObjectOptions.DownloadObjectOptionsBuilder()
                                .partNumber(2)
                                .configureDataOutput().toFile(downloadRangeToFile1, false).endConfigureDataOutput()
                                .build());

                byte[] downloadRangeToFileBytes1 = Files.readAllBytes(Paths.get(downloadRangeToFile1));
                Files.deleteIfExists(Paths.get(downloadRangeToFile1));
                Assertions.assertEquals("bytes " + (part1Content.getBytes().length)
                                + "-" + (part1Content.getBytes().length + 2)
                                + "/" + (part1Content.getBytes().length + part2Content.getBytes().length),
                        s3ObjectPart.getObjectMetadata().getContentRange());
                Assertions.assertArrayEquals(Arrays.copyOfRange((part1Content + part2Content).getBytes(StandardCharsets.UTF_8),
                        part1Content.getBytes().length, part1Content.getBytes().length + 2 + 1), downloadRangeToFileBytes1);
            }

            {
                ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
                // Download object part and write to outStream.
                S3Object s3ObjectPart = candyS3.downloadObject(bucket, objectKey,
                        new DownloadObjectOptions.DownloadObjectOptionsBuilder()
                                .configureDataOutput().toStream(outputStream).endConfigureDataOutput()
                                .partNumber(2)
                                .build());
                Assertions.assertEquals("bytes " + (part1Content.getBytes().length)
                                + "-" + (part1Content.getBytes().length + 2)
                                + "/" + (part1Content.getBytes().length + part2Content.getBytes().length),
                        s3ObjectPart.getObjectMetadata().getContentRange());
                Assertions.assertArrayEquals(Arrays.copyOfRange((part1Content + part2Content).getBytes(StandardCharsets.UTF_8),
                        part1Content.getBytes().length, part1Content.getBytes().length + 2 + 1), outputStream.toByteArray());
            }

            {
                // Download object part and record to byte array.
                S3Object s3ObjectPart = candyS3.downloadObject(bucket, objectKey,
                        new DownloadObjectOptions.DownloadObjectOptionsBuilder()
                                .configureDataOutput().toBytes().endConfigureDataOutput()
                                .partNumber(2)
                                .build());
                Assertions.assertEquals("bytes " + (part1Content.getBytes().length)
                                + "-" + (part1Content.getBytes().length + 2)
                                + "/" + (part1Content.getBytes().length + part2Content.getBytes().length),
                        s3ObjectPart.getObjectMetadata().getContentRange());
                Assertions.assertArrayEquals(Arrays.copyOfRange((part1Content + part2Content).getBytes(StandardCharsets.UTF_8),
                        part1Content.getBytes().length, part1Content.getBytes().length + 2 + 1), s3ObjectPart.getContentBytes());
            }

        } finally {
            candyS3.deleteObject(bucket, new DeleteObjectOptions(objectKey));
            candyS3.deleteBucket(bucket);
        }
    }

    @Test
    @Tag(TestTag.basic)
    void downloadObjectMetadataTest() throws IOException, NoSuchAlgorithmException, InterruptedException {
        CandyS3 candyS3 = init(provider());
        String bucket = genTestBucketName("downloadObjectMetadataTest");
        String objectKey = "downloadObjectMetadataTest.data";
        byte[] bytes = "x我y".getBytes(StandardCharsets.UTF_8);
        try {
            candyS3.createBucket(new CreateBucketOptions.CreateBucketOptionsBuilder(bucket).build());

            Date expiresDate = new Date(System.currentTimeMillis() + 1000 * 60 * 2);
            candyS3.putObject(bucket, objectKey, new PutObjectOptions.PutObjectOptionsBuilder()
                    .configureUploadData().withData(bytes).endConfigureDataContent()
                    .configurePutObjectHeaderOptions()
                    .cacheControl("public, max-age=3600")
                    .contentDisposition("attachment; filename=\"thisisaattachment.html\"")
                    .contentEncoding("gzip, deflate")
                    .contentLanguage("en, zh-CN")
                    .contentType("text/html")
                    .expires(expiresDate)
                    .endConfigurePutObjectHeaderOptions()
                    .build());

            // downloadObject
            {
                S3Object s3Object = candyS3.downloadObject(bucket, objectKey, new DownloadObjectOptions.DownloadObjectOptionsBuilder().build());
                S3Object.S3ObjectMetadata objectMetadata = s3Object.getObjectMetadata();

                Assertions.assertNotNull(s3Object.getObjectMetadata());

                Assertions.assertEquals(s3Object.getSize(), "x我y".getBytes(StandardCharsets.UTF_8).length);

                Assertions.assertEquals(objectMetadata.getCacheControl(), "public, max-age=3600");
                Assertions.assertEquals(objectMetadata.getContentDisposition(), "attachment; filename=\"thisisaattachment.html\"");
                Assertions.assertEquals(objectMetadata.getContentEncoding(), "gzip, deflate");
                Assertions.assertEquals(objectMetadata.getContentLanguage(), "en, zh-CN");
                Assertions.assertNull(objectMetadata.getContentRange());
                Assertions.assertEquals(objectMetadata.getContentType(), "text/html");
                Assertions.assertTrue(objectMetadata.getExpires().after(new Date(expiresDate.getTime() - 1000)));
                Assertions.assertTrue(objectMetadata.getExpires().before(new Date(expiresDate.getTime() + 1000)));
            }

            // downloadObject with range
            {
                S3Object s3Object = candyS3.downloadObject(bucket, objectKey, new DownloadObjectOptions.DownloadObjectOptionsBuilder()
                        .range(1, bytes.length - 2).build());
                S3Object.S3ObjectMetadata objectMetadata = s3Object.getObjectMetadata();

                Assertions.assertNotNull(s3Object.getObjectMetadata());

                Assertions.assertEquals(s3Object.getSize(), (bytes.length - 2) - 1 + 1);

                Assertions.assertEquals(objectMetadata.getCacheControl(), "public, max-age=3600");
                Assertions.assertEquals(objectMetadata.getContentDisposition(), "attachment; filename=\"thisisaattachment.html\"");
                Assertions.assertEquals(objectMetadata.getContentEncoding(), "gzip, deflate");
                Assertions.assertEquals(objectMetadata.getContentLanguage(), "en, zh-CN");
                Assertions.assertEquals(objectMetadata.getContentRange(), "bytes 1-" + (bytes.length - 2) + "/" + (bytes.length));
                Assertions.assertEquals(objectMetadata.getContentType(), "text/html");
                Assertions.assertTrue(objectMetadata.getExpires().after(new Date(expiresDate.getTime() - 1000)));
                Assertions.assertTrue(objectMetadata.getExpires().before(new Date(expiresDate.getTime() + 1000)));
            }

            Thread.sleep(60 * 1000);
        } finally {
            deleteAllObjectVersions(provider(), bucket);
            candyS3.deleteBucket(bucket);
        }
    }

    @Test
    @Tag(TestTag.objectLock)
    void downloadObjectObjectLockPropertiesTest() throws IOException, NoSuchAlgorithmException, InterruptedException {
        CandyS3 candyS3 = init(provider());
        String bucket = genTestBucketName("dOLPTest");
        String objectKey = "downloadObjectObjectLockPropertiesTest.data";
        byte[] bytes = "x我y".getBytes(StandardCharsets.UTF_8);
        try {
            candyS3.createBucket(new CreateBucketOptions.CreateBucketOptionsBuilder(bucket).enableObjectLock().build());
            // Enable object-lock when create bucket is not supported by Tencent cloud COS
            if (S3Provider.TENCENTCLOUD_COS.equals(provider())) {
                candyS3.enableBucketObjectLock(bucket, new UpdateBucketObjectLockOptions.UpdateBucketObjectLockOptionsBuilder().buildWithoutRetention());
            }

            Date retainDate = new Date(System.currentTimeMillis() + 1000 * 60);
            candyS3.putObject(bucket, objectKey, new PutObjectOptions.PutObjectOptionsBuilder()
                    .configureUploadData().withData(bytes).endConfigureDataContent()
                    .configureObjectLockOptions()
                    .lockMode(ObjectRetentionMode.COMPLIANCE)
                    .retainUntilDate(retainDate)
                    .legalHold(true)
                    .endConfigureObjectLockOptions()
                    .build());

            // downloadObject
            {
                S3Object s3Object = candyS3.downloadObject(bucket, objectKey, new DownloadObjectOptions.DownloadObjectOptionsBuilder().build());
                Assertions.assertNotNull(s3Object.getObjectMetadata());

                Assertions.assertEquals(s3Object.getSize(), "x我y".getBytes(StandardCharsets.UTF_8).length);

                Assertions.assertEquals(s3Object.getObjectLockConfiguration().getObjectLockMode(), ObjectRetentionMode.COMPLIANCE);
                Assertions.assertTrue(s3Object.getObjectLockConfiguration().getObjectLockRetainUntilDate().after(new Date(retainDate.getTime() - 1000)));
                Assertions.assertTrue(s3Object.getObjectLockConfiguration().getObjectLockRetainUntilDate().before(new Date(retainDate.getTime() + 1000)));
                Assertions.assertTrue(s3Object.getObjectLockConfiguration().isObjectLockLegalHold());

            }

            Thread.sleep(60 * 1000);
        } finally {
            deleteAllObjectVersions(provider(), bucket);
            candyS3.deleteBucket(bucket);
        }
    }

    @Test
    @Tag(TestTag.basic)
    void downloadObjectStorageClassTest() throws IOException, NoSuchAlgorithmException, InterruptedException {
        CandyS3 candyS3 = init(provider());
        String bucket = genTestBucketName("downloadObjectStorageClassTest");
        String objectKey = "dOSCTest.data";
        byte[] bytes = "x我y".getBytes(StandardCharsets.UTF_8);
        try {
            candyS3.createBucket(new CreateBucketOptions.CreateBucketOptionsBuilder(bucket).build());

            candyS3.putObject(bucket, objectKey, new PutObjectOptions.PutObjectOptionsBuilder()
                    .configureUploadData().withData(bytes).endConfigureDataContent()
                    .storageClass(StorageClass.STANDARD_IA)
                    .build());

            // downloadObject
            {
                S3Object s3Object = candyS3.downloadObject(bucket, objectKey, new DownloadObjectOptions.DownloadObjectOptionsBuilder().build());
                Assertions.assertNotNull(s3Object.getObjectMetadata());

                Assertions.assertEquals(s3Object.getSize(), "x我y".getBytes(StandardCharsets.UTF_8).length);

                Assertions.assertEquals(StorageClass.STANDARD_IA.name(), s3Object.getStorageClass());
            }

            Thread.sleep(60 * 1000);
        } finally {
            deleteAllObjectVersions(provider(), bucket);
            candyS3.deleteBucket(bucket);
        }
    }

    @Test
    @Tag(TestTag.tagging)
    void downloadObjectTagTest() throws IOException, NoSuchAlgorithmException, InterruptedException {
        CandyS3 candyS3 = init(provider());
        String bucket = genTestBucketName("downloadObjectMetadataTest");
        String objectKey = "downloadObjectMetadataTest.data";
        byte[] bytes = "x我y".getBytes(StandardCharsets.UTF_8);
        try {
            candyS3.createBucket(new CreateBucketOptions.CreateBucketOptionsBuilder(bucket).build());

            Map<String, String> tags = new HashMap<>();
            tags.put("test2", "yy");
            tags.put("test3", "zz");
            candyS3.putObject(bucket, objectKey, new PutObjectOptions.PutObjectOptionsBuilder()
                    .configureUploadData().withData(bytes).endConfigureDataContent()
                    .addTag("test1", "xx")
                    .addTags(tags)
                    .build());

            // downloadObject
            {
                S3Object s3Object = candyS3.downloadObject(bucket, objectKey, new DownloadObjectOptions.DownloadObjectOptionsBuilder().build());

                Assertions.assertNotNull(s3Object.getObjectMetadata());

                Assertions.assertEquals(s3Object.getSize(), "x我y".getBytes(StandardCharsets.UTF_8).length);

                Assertions.assertEquals(s3Object.getTagCount(), (Integer) 3);
            }

            Thread.sleep(60 * 1000);
        } finally {
            deleteAllObjectVersions(provider(), bucket);
            candyS3.deleteBucket(bucket);
        }
    }

    @Test
    @Tag(TestTag.basic)
    void getWithPresignUrlTest() throws IOException, InterruptedException, NoSuchAlgorithmException {
        CandyS3 candyS3 = init(provider());
        String bucket = genTestBucketName("getPresignUrlTest");
        String objectKey = "target.data";
        byte[] bytes = "1".getBytes(StandardCharsets.UTF_8);
        try {
            long expireSecs = 10;

            OkHttpClient okHttpClient = new OkHttpClient().newBuilder().build();

            candyS3.createBucket(new CreateBucketOptions.CreateBucketOptionsBuilder(bucket).build());

            candyS3.putObject(bucket, objectKey, new PutObjectOptions.PutObjectOptionsBuilder()
                    .configureUploadData().withData(bytes).endConfigureDataContent()
                    .build());

            // generate presigned url and access object with it
            String presignUrl = candyS3.calculatePresignedUrl(bucket, objectKey, new PresignUrlOptions("GET", expireSecs));
            Assertions.assertNotNull(presignUrl);

            URL url = new URL(presignUrl);
            Request.Builder requestBuilder = new Request.Builder()
                    .url(url)
                    .get();

            Call call1 = okHttpClient.newCall(requestBuilder.build());
            call1.timeout().timeout(5, TimeUnit.SECONDS);
            try (Response response = call1.execute()) {
                Assertions.assertEquals(response.code(), 200);
                Assertions.assertEquals(response.body().string(), "1");
            }

            // wait the presigned url expires
            Thread.sleep((expireSecs + 1) * 1000);

            // access object with an expired presigned url will fail.
            Call call2 = okHttpClient.newCall(requestBuilder.build());
            call2.timeout().timeout(5, TimeUnit.SECONDS);
            try (Response response = call2.execute()) {
                Assertions.assertEquals(403, response.code());
                Assertions.assertTrue(response.body().string().contains("Request has expired"));
            }

        } finally {
            deleteAllObjectVersions(provider(), bucket);
            candyS3.deleteBucket(bucket);
        }
    }

    @Test
    @Tag(TestTag.basic)
    void putWithPresignUrlTest() throws IOException, InterruptedException, NoSuchAlgorithmException {
        CandyS3 candyS3 = init(provider());
        String bucket = genTestBucketName("putWithPresignUrlTest");
        String objectKey = "target.data";
        byte[] bytes = "1".getBytes(StandardCharsets.UTF_8);
        try {
            long expireSecs = 10;

            OkHttpClient okHttpClient = new OkHttpClient().newBuilder().build();

            candyS3.createBucket(new CreateBucketOptions.CreateBucketOptionsBuilder(bucket).build());

            // generate presigned url and upload object with it
            String presignUrl = candyS3.calculatePresignedUrl(bucket, objectKey, new PresignUrlOptions("PUT", expireSecs));
            Assertions.assertNotNull(presignUrl);

            URL url = new URL(presignUrl);
            Request.Builder requestBuilder = new Request.Builder()
                    .url(url)
                    .put(RequestBody.create(bytes));

            Call call1 = okHttpClient.newCall(requestBuilder.build());
            call1.timeout().timeout(5, TimeUnit.SECONDS);
            try (Response response = call1.execute()) {
                Assertions.assertEquals(response.code(), 200);
            }

            S3Object s3Object = candyS3.downloadObject(bucket, objectKey, new DownloadObjectOptions.DownloadObjectOptionsBuilder().build());
            Assertions.assertArrayEquals(bytes, s3Object.getContentBytes());

            // wait the presigned url expires
            Thread.sleep((expireSecs + 1) * 1000);

            // upload object with an expired presigned url will fail.
            Call call2 = okHttpClient.newCall(requestBuilder.build());
            call2.timeout().timeout(5, TimeUnit.SECONDS);
            try (Response response = call2.execute()) {
                Assertions.assertEquals(response.code(), 403);
                Assertions.assertTrue(response.body().string().contains("Request has expired"));
            }

        } finally {
            deleteAllObjectVersions(provider(), bucket);
            candyS3.deleteBucket(bucket);
        }
    }

    @Test
    @Tag(TestTag.objectRetention)
    void objectRetentionTest() throws IOException, NoSuchAlgorithmException {
        CandyS3 candyS3 = init(provider());
        String bucketWithoutRetention = genTestBucketName("oRTest1-bWithoutR"); // bucketwithoutretention
        String bucketWithRetention = genTestBucketName("oRTest1-bWR"); // bucketWithRetention
        try {

            // S3 bucket enable object lock and set retention period on an S3 object
            {
                candyS3.createBucket(new CreateBucketOptions.CreateBucketOptionsBuilder(bucketWithoutRetention).enableObjectLock().build());

                String objectKey1 = "objectKey1";
                Date dateTwoDaysLater = new Date(System.currentTimeMillis() + 1000 * 60 * 60 * 24 * 2);
                candyS3.putObject(bucketWithoutRetention, objectKey1, new PutObjectOptions.PutObjectOptionsBuilder()
                        .configureObjectLockOptions().lockMode(ObjectRetentionMode.GOVERNANCE).retainUntilDate(dateTwoDaysLater).endConfigureObjectLockOptions()
                        .build());
                S3Object s3Object = candyS3.downloadObject(bucketWithoutRetention, objectKey1, new DownloadObjectOptions.DownloadObjectOptionsBuilder()
                        .configureDataOutput().toBytes().endConfigureDataOutput()
                        .build());
                Assertions.assertEquals(s3Object.getObjectLockConfiguration().getObjectLockMode(), ObjectRetentionMode.GOVERNANCE);

                Assertions.assertTrue(s3Object.getObjectLockConfiguration().getObjectLockRetainUntilDate()
                        .after(new Date(dateTwoDaysLater.getTime() - 1000)));
                Assertions.assertTrue(s3Object.getObjectLockConfiguration().getObjectLockRetainUntilDate()
                        .before(new Date(dateTwoDaysLater.getTime() + 1000)));

                SimpleDateFormat simpleDateFormat = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss'Z'");
                simpleDateFormat.setTimeZone(new SimpleTimeZone(0, "UTC"));
                String twoDaysLaterStr = simpleDateFormat.format(dateTwoDaysLater);
                String retainUntilDateStr = simpleDateFormat.format(s3Object.getObjectLockConfiguration().getObjectLockRetainUntilDate());
                Assertions.assertEquals(twoDaysLaterStr, retainUntilDateStr);


                deleteAllObjectVersions(provider(), bucketWithoutRetention);
            }

            // S3 bucket has default retention period, and object can use default retention period or custom retention period
            {
                candyS3.createBucket(new CreateBucketOptions.CreateBucketOptionsBuilder(bucketWithRetention).enableObjectLock().build());
                candyS3.enableBucketObjectLock(bucketWithRetention, new UpdateBucketObjectLockOptions.UpdateBucketObjectLockOptionsBuilder()
                        .retentionDays(ObjectRetentionMode.GOVERNANCE, 1).build());

                {
                    String objectKeyUseBucketDefaultRetentionPeriod = "objectKeyUseBucketDefaultRetentionPeriod";
                    candyS3.putObject(bucketWithRetention, objectKeyUseBucketDefaultRetentionPeriod, new PutObjectOptions.PutObjectOptionsBuilder().build());
                    S3Object s3Object = candyS3.downloadObject(bucketWithRetention, objectKeyUseBucketDefaultRetentionPeriod, new DownloadObjectOptions.DownloadObjectOptionsBuilder()
                            .configureDataOutput().toBytes().endConfigureDataOutput()
                            .build());
                    Assertions.assertEquals(s3Object.getObjectLockConfiguration().getObjectLockMode(), ObjectRetentionMode.GOVERNANCE);
                    Assertions.assertTrue(s3Object.getObjectLockConfiguration().getObjectLockRetainUntilDate()
                            .after(new Date(System.currentTimeMillis() + 1000 * 60 * 60 * 24 - 1000 * 5)));
                    Assertions.assertTrue(s3Object.getObjectLockConfiguration().getObjectLockRetainUntilDate()
                            .before(new Date(System.currentTimeMillis() + 1000 * 60 * 60 * 24 + 1000 * 5)));

                }

                {
                    String objectKeyUseCustomRetentionPeriod = "objectKeyUseCustomRetentionPeriod";
                    Date dateTwoDaysLater = new Date(System.currentTimeMillis() + 1000 * 60 * 60 * 24 * 2);
                    candyS3.putObject(bucketWithRetention, objectKeyUseCustomRetentionPeriod, new PutObjectOptions.PutObjectOptionsBuilder()
                            .configureObjectLockOptions().lockMode(ObjectRetentionMode.GOVERNANCE).retainUntilDate(dateTwoDaysLater).endConfigureObjectLockOptions()
                            .build());
                    S3Object s3Object = candyS3.downloadObject(bucketWithRetention, objectKeyUseCustomRetentionPeriod, new DownloadObjectOptions.DownloadObjectOptionsBuilder()
                            .configureDataOutput().toBytes().endConfigureDataOutput()
                            .build());
                    Assertions.assertEquals(s3Object.getObjectLockConfiguration().getObjectLockMode(), ObjectRetentionMode.GOVERNANCE);
                    Assertions.assertTrue(s3Object.getObjectLockConfiguration().getObjectLockRetainUntilDate()
                            .after(new Date(dateTwoDaysLater.getTime() - 1000)));
                    Assertions.assertTrue(s3Object.getObjectLockConfiguration().getObjectLockRetainUntilDate()
                            .before(new Date(dateTwoDaysLater.getTime() + 1000)));

                    SimpleDateFormat simpleDateFormat = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss'Z'");
                    simpleDateFormat.setTimeZone(new SimpleTimeZone(0, "UTC"));
                    String twoDaysLaterStr = simpleDateFormat.format(dateTwoDaysLater);
                    String retainUntilDateStr = simpleDateFormat.format(s3Object.getObjectLockConfiguration().getObjectLockRetainUntilDate());
                    Assertions.assertEquals(twoDaysLaterStr, retainUntilDateStr);
                }

                deleteAllObjectVersions(provider(), bucketWithRetention);
            }
        } finally {
            candyS3.deleteBucket(bucketWithoutRetention);
            candyS3.deleteBucket(bucketWithRetention);
        }
    }

    @Test
    @Tag(TestTag.objectRetention)
    void objectVersionRetentionTest() throws IOException, NoSuchAlgorithmException {
        CandyS3 candyS3 = init(provider());
        String bucket = genTestBucketName("objectVersionRetentionTest");
        try {

            candyS3.createBucket(new CreateBucketOptions.CreateBucketOptionsBuilder(bucket).enableObjectLock().build());

            String objectKey1 = "objectKey1";
            candyS3.putObject(bucket, objectKey1, new PutObjectOptions.PutObjectOptionsBuilder()
                    .configureUploadData().withData(new byte[]{1}).endConfigureDataContent()
                    .build());
            candyS3.putObject(bucket, objectKey1, new PutObjectOptions.PutObjectOptionsBuilder()
                    .configureUploadData().withData(new byte[]{2}).endConfigureDataContent()
                    .build());

            ListPaginationResult<S3ObjectVersion> objectVersions = candyS3.listObjectVersions(bucket, new ListObjectVersionsOptions());

            Date retainDate = new Date(System.currentTimeMillis() + 1000 * 10);
            candyS3.updateObjectRetention(bucket, objectKey1, new UpdateObjectRetentionOptions().versionId(objectVersions.getResults().get(1).getVersionId())
                    .retentionMode(ObjectRetentionMode.GOVERNANCE)
                    .retainUntilDate(retainDate));

            try {
                candyS3.getObjectRetention(bucket, objectKey1, new GetObjectRetentionOptions());
                Assertions.fail("Should not be here. Exception should be thrown when get retention on an object without retention");
            } catch (Exception ex) {
                Assertions.assertTrue(ex instanceof CandyS3Exception);
                Assertions.assertEquals(((CandyS3Exception) ex).getCode(), CommonErrorCode.NO_SUCH_OBJECT_LOCK_CONFIGURATION.getCode());
                Assertions.assertEquals(((CandyS3Exception) ex).getParsedError().getCode(), "NoSuchObjectLockConfiguration");
            }


            ObjectLockProperties objectV1LockProps = candyS3.getObjectRetention(bucket, objectKey1, new GetObjectRetentionOptions()
                    .versionId(objectVersions.getResults().get(1).getVersionId()));
            Assertions.assertEquals(objectV1LockProps.getObjectLockMode(), ObjectRetentionMode.GOVERNANCE);
            Assertions.assertTrue(objectV1LockProps.getObjectLockRetainUntilDate().after(new Date(retainDate.getTime() - 1000)));
            Assertions.assertTrue(objectV1LockProps.getObjectLockRetainUntilDate().before(new Date(retainDate.getTime() + 1000)));

            try {
                candyS3.getObjectRetention(bucket, objectKey1, new GetObjectRetentionOptions()
                        .versionId(objectVersions.getResults().get(0).getVersionId()));
                Assertions.fail("Should not be here. Exception should be thrown when get retention on an object version without retention");
            } catch (Exception ex) {
                Assertions.assertTrue(ex instanceof CandyS3Exception);
                Assertions.assertEquals(((CandyS3Exception) ex).getCode(), CommonErrorCode.NO_SUCH_OBJECT_LOCK_CONFIGURATION.getCode());
                Assertions.assertEquals(((CandyS3Exception) ex).getParsedError().getCode(), "NoSuchObjectLockConfiguration");
            }

        } finally {
            deleteAllObjectVersions(provider(), bucket);
            candyS3.deleteBucket(bucket);
        }
    }


    @Test
    @Tag(TestTag.objectRetention)
    void updateObjectGovernancePeriodTest() throws IOException, NoSuchAlgorithmException {
        CandyS3 candyS3 = init(provider());
        String bucket = genTestBucketName("uOGPTest");
        try {

            candyS3.createBucket(new CreateBucketOptions.CreateBucketOptionsBuilder(bucket).enableObjectLock().build());

            String objectKey1 = "objectKey1";
            Date retainDate = new Date(System.currentTimeMillis() + 1000 * 20);
            Date closerDate = new Date(System.currentTimeMillis() + 1000 * 10);
            Date laterDate = new Date(System.currentTimeMillis() + 1000 * 30);

            candyS3.putObject(bucket, objectKey1, new PutObjectOptions.PutObjectOptionsBuilder()
                    .configureObjectLockOptions().lockMode(ObjectRetentionMode.GOVERNANCE).retainUntilDate(retainDate).endConfigureObjectLockOptions()
                    .build());

            {
                ObjectLockProperties objectLockProperties = candyS3.getObjectRetention(bucket, objectKey1, new GetObjectRetentionOptions());
                Assertions.assertEquals(objectLockProperties.getObjectLockMode(), ObjectRetentionMode.GOVERNANCE);

                Assertions.assertTrue(objectLockProperties.getObjectLockRetainUntilDate()
                        .after(new Date(retainDate.getTime() - 1000)));
                Assertions.assertTrue(objectLockProperties.getObjectLockRetainUntilDate()
                        .before(new Date(retainDate.getTime() + 1000)));

                SimpleDateFormat simpleDateFormat = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss'Z'");
                simpleDateFormat.setTimeZone(new SimpleTimeZone(0, "UTC"));
                String retainDateStr = simpleDateFormat.format(retainDate);
                String retainUntilDateStr = simpleDateFormat.format(objectLockProperties.getObjectLockRetainUntilDate());
                Assertions.assertEquals(retainDateStr, retainUntilDateStr);
            }

            // Update retention in GOVERNANCE mode to a later date is permitted
            {
                candyS3.updateObjectRetention(bucket, objectKey1, new UpdateObjectRetentionOptions()
                        .retentionMode(ObjectRetentionMode.GOVERNANCE)
                        .retainUntilDate(laterDate));

                ObjectLockProperties objectLockProperties = candyS3.getObjectRetention(bucket, objectKey1, new GetObjectRetentionOptions());
                Assertions.assertEquals(objectLockProperties.getObjectLockMode(), ObjectRetentionMode.GOVERNANCE);

                Assertions.assertTrue(objectLockProperties.getObjectLockRetainUntilDate()
                        .after(new Date(laterDate.getTime() - 1000)));
                Assertions.assertTrue(objectLockProperties.getObjectLockRetainUntilDate()
                        .before(new Date(laterDate.getTime() + 1000)));

                SimpleDateFormat simpleDateFormat = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss'Z'");
                simpleDateFormat.setTimeZone(new SimpleTimeZone(0, "UTC"));
                String laterDateStr = simpleDateFormat.format(laterDate);
                String retainUntilDateStr = simpleDateFormat.format(objectLockProperties.getObjectLockRetainUntilDate());
                Assertions.assertEquals(laterDateStr, retainUntilDateStr);
            }

            // Update retention in GOVERNANCE mode to a closer date is not permitted unless use bypassGovernanceRetention
            {
                try {
                    candyS3.updateObjectRetention(bucket, objectKey1, new UpdateObjectRetentionOptions()
                            .retentionMode(ObjectRetentionMode.GOVERNANCE)
                            .retainUntilDate(closerDate));
                    Assertions.fail("Should not be here. Exception should be thrown when update retention in GOVERNANCE mode to a closer date without bypassGovernanceRetention");
                } catch (Exception ex) {
                    Assertions.assertTrue(ex instanceof CandyS3Exception);
                    Assertions.assertEquals("AccessDenied", ((CandyS3Exception) ex).getParsedError().getCode());
                }

                candyS3.updateObjectRetention(bucket, objectKey1, new UpdateObjectRetentionOptions()
                        .retentionMode(ObjectRetentionMode.GOVERNANCE)
                        .retainUntilDate(closerDate)
                        .bypassGovernanceRetention());

                ObjectLockProperties objectLockProperties = candyS3.getObjectRetention(bucket, objectKey1, new GetObjectRetentionOptions());
                Assertions.assertEquals(objectLockProperties.getObjectLockMode(), ObjectRetentionMode.GOVERNANCE);

                Assertions.assertTrue(objectLockProperties.getObjectLockRetainUntilDate()
                        .after(new Date(closerDate.getTime() - 1000)));
                Assertions.assertTrue(objectLockProperties.getObjectLockRetainUntilDate()
                        .before(new Date(closerDate.getTime() + 1000)));

                SimpleDateFormat simpleDateFormat = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss'Z'");
                simpleDateFormat.setTimeZone(new SimpleTimeZone(0, "UTC"));
                String closerDateStr = simpleDateFormat.format(closerDate);
                String retainUntilDateStr = simpleDateFormat.format(objectLockProperties.getObjectLockRetainUntilDate());
                Assertions.assertEquals(closerDateStr, retainUntilDateStr);
            }

            // Remove retention in GOVERNANCE mode is not permitted unless use bypassGovernanceRetention
            {
                try {
                    candyS3.updateObjectRetention(bucket, objectKey1, new UpdateObjectRetentionOptions());
                    Assertions.fail("Should not be here. Exception should be thrown when remove retention in GOVERNANCE mode without bypassGovernanceRetention");
                } catch (Exception ex) {
                    Assertions.assertTrue(ex instanceof CandyS3Exception);
                    Assertions.assertEquals("AccessDenied", ((CandyS3Exception) ex).getParsedError().getCode());
                }

                candyS3.updateObjectRetention(bucket, objectKey1, new UpdateObjectRetentionOptions().bypassGovernanceRetention());

                try {
                    candyS3.getObjectRetention(bucket, objectKey1, new GetObjectRetentionOptions());
                    Assertions.fail("Should not be here. Exception should be thrown when get retention on an object without retention");
                } catch (Exception ex) {
                    Assertions.assertTrue(ex instanceof CandyS3Exception);
                    Assertions.assertEquals(((CandyS3Exception) ex).getCode(), CommonErrorCode.NO_SUCH_OBJECT_LOCK_CONFIGURATION.getCode());
                    Assertions.assertEquals(((CandyS3Exception) ex).getParsedError().getCode(), "NoSuchObjectLockConfiguration");
                }
            }

        } finally {
            deleteAllObjectVersions(provider(), bucket);
            candyS3.deleteBucket(bucket);
        }
    }

    @Test
    @Tag(TestTag.objectRetention)
    void updateObjectCompliancePeriodTest() throws IOException, NoSuchAlgorithmException, InterruptedException {
        CandyS3 candyS3 = init(provider());
        String bucket = genTestBucketName("uOCPTest");
        try {

            candyS3.createBucket(new CreateBucketOptions.CreateBucketOptionsBuilder(bucket).enableObjectLock().build());

            // Enable object-lock when create bucket is not supported by Tencent cloud COS
            if (S3Provider.TENCENTCLOUD_COS.equals(provider())) {
                candyS3.enableBucketObjectLock(bucket, new UpdateBucketObjectLockOptions.UpdateBucketObjectLockOptionsBuilder().buildWithoutRetention());
            }

            String objectKey1 = "objectKey1";
            Date retainDate = new Date(System.currentTimeMillis() + 1000 * 20);
            Date closerDate = new Date(System.currentTimeMillis() + 1000 * 10);
            Date laterDate = new Date(System.currentTimeMillis() + 1000 * 30);

            candyS3.putObject(bucket, objectKey1, new PutObjectOptions.PutObjectOptionsBuilder()
                    .configureObjectLockOptions().lockMode(ObjectRetentionMode.COMPLIANCE).retainUntilDate(retainDate).endConfigureObjectLockOptions()
                    .build());

            {
                ObjectLockProperties objectLockProperties = candyS3.getObjectRetention(bucket, objectKey1, new GetObjectRetentionOptions());
                Assertions.assertEquals(objectLockProperties.getObjectLockMode(), ObjectRetentionMode.COMPLIANCE);

                Assertions.assertTrue(objectLockProperties.getObjectLockRetainUntilDate()
                        .after(new Date(retainDate.getTime() - 1000)));
                Assertions.assertTrue(objectLockProperties.getObjectLockRetainUntilDate()
                        .before(new Date(retainDate.getTime() + 1000)));

                SimpleDateFormat simpleDateFormat = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss'Z'");
                simpleDateFormat.setTimeZone(new SimpleTimeZone(0, "UTC"));
                String retainDateStr = simpleDateFormat.format(retainDate);
                String retainUntilDateStr = simpleDateFormat.format(objectLockProperties.getObjectLockRetainUntilDate());
                Assertions.assertEquals(retainDateStr, retainUntilDateStr);
            }

            // Update retention in COMPLIANCE mode to a later date is permitted
            {
                candyS3.updateObjectRetention(bucket, objectKey1, new UpdateObjectRetentionOptions()
                        .retentionMode(ObjectRetentionMode.COMPLIANCE)
                        .retainUntilDate(laterDate));

                ObjectLockProperties objectLockProperties = candyS3.getObjectRetention(bucket, objectKey1, new GetObjectRetentionOptions());
                Assertions.assertEquals(objectLockProperties.getObjectLockMode(), ObjectRetentionMode.COMPLIANCE);

                Assertions.assertTrue(objectLockProperties.getObjectLockRetainUntilDate()
                        .after(new Date(laterDate.getTime() - 1000)));
                Assertions.assertTrue(objectLockProperties.getObjectLockRetainUntilDate()
                        .before(new Date(laterDate.getTime() + 1000)));

                SimpleDateFormat simpleDateFormat = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss'Z'");
                simpleDateFormat.setTimeZone(new SimpleTimeZone(0, "UTC"));
                String laterDateStr = simpleDateFormat.format(laterDate);
                String retainUntilDateStr = simpleDateFormat.format(objectLockProperties.getObjectLockRetainUntilDate());
                Assertions.assertEquals(laterDateStr, retainUntilDateStr);
            }

            // Update retention in COMPLIANCE mode to a closer date is not permitted
            {
                try {
                    candyS3.updateObjectRetention(bucket, objectKey1, new UpdateObjectRetentionOptions()
                            .retentionMode(ObjectRetentionMode.COMPLIANCE)
                            .retainUntilDate(closerDate));
                    Assertions.fail("Should not be here. Exception should be thrown when update retention in COMPLIANCE mode to a closer date");
                } catch (Exception ex) {
                    Assertions.assertTrue(ex instanceof CandyS3Exception);
                    if (S3Provider.TENCENTCLOUD_COS.equals(provider())) {
                        Assertions.assertEquals("InvalidObjectLock", ((CandyS3Exception) ex).getParsedError().getCode());
                    } else {
                        Assertions.assertEquals("AccessDenied", ((CandyS3Exception) ex).getParsedError().getCode());
                    }
                }
            }

            // Remove retention in COMPLIANCE mode is not permitted and get an AccessDenied error
            {
                try {
                    candyS3.updateObjectRetention(bucket, objectKey1, new UpdateObjectRetentionOptions());
                    Assertions.fail("Should not be here. Exception should be thrown when remove retention in COMPLIANCE mode");
                } catch (Exception ex) {
                    Assertions.assertTrue(ex instanceof CandyS3Exception);
                    Assertions.assertEquals("AccessDenied", ((CandyS3Exception) ex).getParsedError().getCode());
                }
            }

            Thread.sleep(35 * 1000);
        } finally {
            deleteAllObjectVersions(provider(), bucket);
            candyS3.deleteBucket(bucket);
        }
    }

    @Test
    @Tag(TestTag.objectRetention)
    void updateObjectRetentionModeTest() throws IOException, NoSuchAlgorithmException, InterruptedException {
        CandyS3 candyS3 = init(provider());
        String bucket = genTestBucketName("updateObjectRetentionModeTest");
        try {

            candyS3.createBucket(new CreateBucketOptions.CreateBucketOptionsBuilder(bucket).enableObjectLock().build());

            Date retainDate = new Date(System.currentTimeMillis() + 1000 * 20);
            Date closerDate = new Date(System.currentTimeMillis() + 1000 * 10);
            Date laterDate = new Date(System.currentTimeMillis() + 1000 * 30);

            // Update retention mode from GOVERNANCE to COMPLIANCE mode with a later date is permitted
            {
                String objectKey1 = "objectKey1";
                candyS3.putObject(bucket, objectKey1, new PutObjectOptions.PutObjectOptionsBuilder()
                        .configureObjectLockOptions().lockMode(ObjectRetentionMode.GOVERNANCE).retainUntilDate(retainDate).endConfigureObjectLockOptions()
                        .build());

                candyS3.updateObjectRetention(bucket, objectKey1, new UpdateObjectRetentionOptions()
                        .retentionMode(ObjectRetentionMode.COMPLIANCE)
                        .retainUntilDate(laterDate)
                        .bypassGovernanceRetention());

                ObjectLockProperties objectLockProperties = candyS3.getObjectRetention(bucket, objectKey1, new GetObjectRetentionOptions());
                Assertions.assertEquals(objectLockProperties.getObjectLockMode(), ObjectRetentionMode.COMPLIANCE);

                Assertions.assertTrue(objectLockProperties.getObjectLockRetainUntilDate()
                        .after(new Date(laterDate.getTime() - 1000)));
                Assertions.assertTrue(objectLockProperties.getObjectLockRetainUntilDate()
                        .before(new Date(laterDate.getTime() + 1000)));

                SimpleDateFormat simpleDateFormat = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss'Z'");
                simpleDateFormat.setTimeZone(new SimpleTimeZone(0, "UTC"));
                String laterDateStr = simpleDateFormat.format(laterDate);
                String retainUntilDateStr = simpleDateFormat.format(objectLockProperties.getObjectLockRetainUntilDate());
                Assertions.assertEquals(laterDateStr, retainUntilDateStr);
            }


            // Update retention mode from GOVERNANCE to COMPLIANCE mode with a closer date is not permitted unless use bypassGovernanceRetention
            {
                String objectKey2 = "objectKey2";
                candyS3.putObject(bucket, objectKey2, new PutObjectOptions.PutObjectOptionsBuilder()
                        .configureObjectLockOptions().lockMode(ObjectRetentionMode.GOVERNANCE).retainUntilDate(retainDate).endConfigureObjectLockOptions()
                        .build());

                try {
                    candyS3.updateObjectRetention(bucket, objectKey2, new UpdateObjectRetentionOptions()
                            .retentionMode(ObjectRetentionMode.COMPLIANCE)
                            .retainUntilDate(closerDate));
                    Assertions.fail("Should not be here. Exception should be thrown when update retention mode from GOVERNANCE to COMPLIANCE with a closer date unless use bypassGovernanceRetention");
                } catch (Exception ex) {
                    Assertions.assertTrue(ex instanceof CandyS3Exception);
                    Assertions.assertEquals("AccessDenied", ((CandyS3Exception) ex).getParsedError().getCode());
                }

                candyS3.updateObjectRetention(bucket, objectKey2, new UpdateObjectRetentionOptions()
                        .retentionMode(ObjectRetentionMode.COMPLIANCE)
                        .retainUntilDate(closerDate)
                        .bypassGovernanceRetention());

                ObjectLockProperties objectLockProperties = candyS3.getObjectRetention(bucket, objectKey2, new GetObjectRetentionOptions());
                Assertions.assertEquals(objectLockProperties.getObjectLockMode(), ObjectRetentionMode.COMPLIANCE);

                Assertions.assertTrue(objectLockProperties.getObjectLockRetainUntilDate()
                        .after(new Date(closerDate.getTime() - 1000)));
                Assertions.assertTrue(objectLockProperties.getObjectLockRetainUntilDate()
                        .before(new Date(closerDate.getTime() + 1000)));

                SimpleDateFormat simpleDateFormat = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss'Z'");
                simpleDateFormat.setTimeZone(new SimpleTimeZone(0, "UTC"));
                String closerDateStr = simpleDateFormat.format(closerDate);
                String retainUntilDateStr = simpleDateFormat.format(objectLockProperties.getObjectLockRetainUntilDate());
                Assertions.assertEquals(closerDateStr, retainUntilDateStr);
            }

            // Update retention mode from COMPLIANCE to GOVERNANCE is not permitted
            {
                String objectKey3 = "objectKey3";
                candyS3.putObject(bucket, objectKey3, new PutObjectOptions.PutObjectOptionsBuilder()
                        .configureObjectLockOptions().lockMode(ObjectRetentionMode.COMPLIANCE).retainUntilDate(retainDate).endConfigureObjectLockOptions()
                        .build());
                try {
                    candyS3.updateObjectRetention(bucket, objectKey3, new UpdateObjectRetentionOptions()
                            .retentionMode(ObjectRetentionMode.GOVERNANCE)
                            .retainUntilDate(laterDate));
                    Assertions.fail("Should not be here. Exception should be thrown when update retention mode from COMPLIANCE to GOVERNANCE");
                } catch (Exception ex) {
                    Assertions.assertTrue(ex instanceof CandyS3Exception);
                    Assertions.assertEquals("AccessDenied", ((CandyS3Exception) ex).getParsedError().getCode());
                }
            }

            Thread.sleep(30 * 1000);
        } finally {
            deleteAllObjectVersions(provider(), bucket);
            candyS3.deleteBucket(bucket);
        }
    }

    @Test
    @Tag(TestTag.objectRetention)
    void multipartUploadObjectRetentionTest() throws IOException, NoSuchAlgorithmException, InterruptedException {
        CandyS3 candyS3 = init(provider());
        String bucket = genTestBucketName("mpartRTest");
        try {

            candyS3.createBucket(new CreateBucketOptions.CreateBucketOptionsBuilder(bucket).enableObjectLock().build());

            // Enable object-lock when create bucket is not supported by Tencent cloud COS
            if (S3Provider.TENCENTCLOUD_COS.equals(provider())) {
                candyS3.enableBucketObjectLock(bucket, new UpdateBucketObjectLockOptions.UpdateBucketObjectLockOptionsBuilder().buildWithoutRetention());
            }

            String objectKey1 = "objectKey1";
            Date retainDate = new Date(System.currentTimeMillis() + 1000 * 10);
            String uploadId = candyS3.createMultipartUpload(bucket, objectKey1, new CreateMultipartUploadOptions.CreateMultipartUploadOptionsBuilder()
                    .configureObjectLockOptions().lockMode(ObjectRetentionMode.COMPLIANCE).retainUntilDate(retainDate).endConfigureObjectLockOptions()
                    .build());
            S3Part part = candyS3.uploadPart(bucket, objectKey1, uploadId, 1, new UploadPartOptions.UploadPartOptionsBuilder()
                    .configureUploadData().withData(StringUtils.repeat("x", 6 * 1024 * 1024).getBytes()).endConfigureDataContent()
                    .build());
            candyS3.completeMultipartUpload(bucket, objectKey1, uploadId, Arrays.asList(part),
                    new CompleteMultipartUploadOptions.CompleteMultipartUploadOptionsBuilder().build());

            ObjectLockProperties objectLockProperties = candyS3.getObjectRetention(bucket, objectKey1, new GetObjectRetentionOptions());
            Assertions.assertEquals(objectLockProperties.getObjectLockMode(), ObjectRetentionMode.COMPLIANCE);
            Assertions.assertTrue(objectLockProperties.getObjectLockRetainUntilDate().after(new Date(retainDate.getTime() - 1000)));
            Assertions.assertTrue(objectLockProperties.getObjectLockRetainUntilDate().before(new Date(retainDate.getTime() + 1000)));

            Thread.sleep(10 * 1000);
        } finally {
            deleteAllObjectVersions(provider(), bucket);
            candyS3.deleteBucket(bucket);
        }
    }

    @Test
    @Tag(TestTag.objectRetention)
    void copyObjectRetentionTest() throws IOException, NoSuchAlgorithmException, InterruptedException {
        CandyS3 candyS3 = init(provider());
        String bucket = genTestBucketName("copyObjectRetentionTest");
        try {

            candyS3.createBucket(new CreateBucketOptions.CreateBucketOptionsBuilder(bucket).enableObjectLock().build());

            // Enable object-lock when create bucket is not supported by Tencent cloud COS
            if (S3Provider.TENCENTCLOUD_COS.equals(provider())) {
                candyS3.enableBucketObjectLock(bucket, new UpdateBucketObjectLockOptions.UpdateBucketObjectLockOptionsBuilder().buildWithoutRetention());
            }

            String objectKey1 = "objectKey1";
            candyS3.putObject(bucket, objectKey1, new PutObjectOptions.PutObjectOptionsBuilder().build());

            String objectKey2 = "objectKey2";
            Date retainDate = new Date(System.currentTimeMillis() + 1000 * 10);
            candyS3.copyObject(bucket, objectKey2, new CopyObjectOptions.CopyObjectOptionsBuilder()
                    .copySource(bucket, objectKey1)
                    .configureObjectLockOptions().lockMode(ObjectRetentionMode.COMPLIANCE).retainUntilDate(retainDate).endConfigureObjectLockOptions()
                    .build());


            ObjectLockProperties objectLockProperties = candyS3.getObjectRetention(bucket, objectKey2, new GetObjectRetentionOptions());
            Assertions.assertEquals(objectLockProperties.getObjectLockMode(), ObjectRetentionMode.COMPLIANCE);
            Assertions.assertTrue(objectLockProperties.getObjectLockRetainUntilDate().after(new Date(retainDate.getTime() - 1000)));
            Assertions.assertTrue(objectLockProperties.getObjectLockRetainUntilDate().before(new Date(retainDate.getTime() + 1000)));

            Thread.sleep(10 * 1000);
        } finally {
            deleteAllObjectVersions(provider(), bucket);
            candyS3.deleteBucket(bucket);
        }
    }

    @Test
    @Tag(TestTag.objectRetention)
    void objectRetentionTimezoneTest() throws IOException, NoSuchAlgorithmException, InterruptedException {
        CandyS3 candyS3 = init(provider());
        String bucket = genTestBucketName("objectRetentionTimezoneTest");
        try {

            // S3 bucket enable object lock and set retention period on an S3 object
            candyS3.createBucket(new CreateBucketOptions.CreateBucketOptionsBuilder(bucket).enableObjectLock().build());

            // Enable object-lock when create bucket is not supported by Tencent cloud COS
            if (S3Provider.TENCENTCLOUD_COS.equals(provider())) {
                candyS3.enableBucketObjectLock(bucket, new UpdateBucketObjectLockOptions.UpdateBucketObjectLockOptionsBuilder().buildWithoutRetention());
            }

            String objectKey1 = "objectKey1";

            TimeZone.setDefault(TimeZone.getTimeZone("America/New_York"));
            Assertions.assertEquals("America/New_York", TimeZone.getDefault().getID());

            Date retainUntilDate = new Date(System.currentTimeMillis() + 1000 * 60);
            candyS3.putObject(bucket, objectKey1, new PutObjectOptions.PutObjectOptionsBuilder()
                    .configureObjectLockOptions().lockMode(ObjectRetentionMode.COMPLIANCE).retainUntilDate(retainUntilDate).endConfigureObjectLockOptions()
                    .build());

            TimeZone.setDefault(TimeZone.getTimeZone("Asia/Shanghai"));
            Assertions.assertEquals("Asia/Shanghai", TimeZone.getDefault().getID());

            Date retainUntilDate2 = new Date(System.currentTimeMillis() + 1000 * 60);

            S3Object s3Object = candyS3.getObjectMetadata(bucket, objectKey1, new DownloadObjectOptions.DownloadObjectOptionsBuilder()
                    .configureDataOutput().toBytes().endConfigureDataOutput()
                    .build());
            Assertions.assertEquals(s3Object.getObjectLockConfiguration().getObjectLockMode(), ObjectRetentionMode.COMPLIANCE);

            Assertions.assertTrue(s3Object.getObjectLockConfiguration().getObjectLockRetainUntilDate()
                    .after(new Date(retainUntilDate.getTime() - 2000)));
            Assertions.assertTrue(s3Object.getObjectLockConfiguration().getObjectLockRetainUntilDate()
                    .before(new Date(retainUntilDate.getTime() + 2000)));

            Assertions.assertTrue(s3Object.getObjectLockConfiguration().getObjectLockRetainUntilDate()
                    .after(new Date(retainUntilDate2.getTime() - 2000)));
            Assertions.assertTrue(s3Object.getObjectLockConfiguration().getObjectLockRetainUntilDate()
                    .before(new Date(retainUntilDate2.getTime() + 2000)));

            SimpleDateFormat simpleDateFormat = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss'Z'");
            simpleDateFormat.setTimeZone(new SimpleTimeZone(0, "UTC"));
            String twoDaysLaterStr1 = simpleDateFormat.format(retainUntilDate);
            String twoDaysLaterStr2 = simpleDateFormat.format(retainUntilDate);
            String retainUntilDateStr = simpleDateFormat.format(s3Object.getObjectLockConfiguration().getObjectLockRetainUntilDate());
            Assertions.assertEquals(twoDaysLaterStr1, retainUntilDateStr);
            Assertions.assertEquals(twoDaysLaterStr2, retainUntilDateStr);

            Thread.sleep(1000 * 60);
        } finally {
            deleteAllObjectVersions(provider(), bucket);
            candyS3.deleteBucket(bucket);
        }

    }


    @Test
    @Tag(TestTag.objectRetention)
    void deleteRetainObjectTest() throws IOException, NoSuchAlgorithmException, InterruptedException {
        CandyS3 candyS3 = init(provider());
        String bucket = genTestBucketName("deleteRetainObjectTest");
        try {

            candyS3.createBucket(new CreateBucketOptions.CreateBucketOptionsBuilder(bucket).enableObjectLock().build());

            {
                String objectKey1 = "objectKey1";
                Date retainDate = new Date(System.currentTimeMillis() + 1000 * 10);
                candyS3.putObject(bucket, objectKey1, new PutObjectOptions.PutObjectOptionsBuilder()
                        .configureObjectLockOptions().lockMode(ObjectRetentionMode.GOVERNANCE).retainUntilDate(retainDate).endConfigureObjectLockOptions()
                        .build());

                // Delete object without versionId is permitted when the object is in governance mode and the retention period has not expired.
                candyS3.deleteObject(bucket, new DeleteObjectOptions(objectKey1));

                ListPaginationResult<S3Object> objects = candyS3.listObjects(bucket, new ListObjectOptions().prefix(objectKey1));
                Assertions.assertEquals(objects.getResults().size(), 0);

                ListPaginationResult<S3ObjectVersion> objectVersions = candyS3.listObjectVersions(bucket, new ListObjectVersionsOptions());
                Assertions.assertEquals(objectVersions.getResults().size(), 1);
                Assertions.assertEquals(objectVersions.getDeleteMarkers().size(), 1);

                // Delete deleteMarker to retain the object version for subsequent tests
                candyS3.deleteObject(bucket, new DeleteObjectOptions(objectKey1).versionId(objectVersions.getDeleteMarkers().get(0).getVersionId()));

                objectVersions = candyS3.listObjectVersions(bucket, new ListObjectVersionsOptions());
                // Delete object with versionId is not permitted when the object is in governance mode and the retention period has not expired.
                try {
                    candyS3.deleteObject(bucket, new DeleteObjectOptions(objectKey1).versionId(objectVersions.getResults().get(0).getVersionId()));
                    Assertions.fail("Should not be here. Exception should be thrown when delete object version that is in governance mode and the retention period has not expired.");
                } catch (Exception ex) {
                    Assertions.assertTrue(ex instanceof CandyS3Exception);
                    Assertions.assertEquals("AccessDenied", ((CandyS3Exception) ex).getParsedError().getCode());
                }

                objectVersions = candyS3.listObjectVersions(bucket, new ListObjectVersionsOptions());
                Assertions.assertEquals(objectVersions.getResults().size(), 1);
                Assertions.assertEquals(objectVersions.getDeleteMarkers().size(), 0);

                // Delete object with versionId is permitted when the object is in governance mode and the retention period has expired.
                Thread.sleep(10 * 1000);
                candyS3.deleteObject(bucket, new DeleteObjectOptions(objectKey1).versionId(objectVersions.getResults().get(0).getVersionId()));

                objectVersions = candyS3.listObjectVersions(bucket, new ListObjectVersionsOptions());
                Assertions.assertEquals(objectVersions.getResults().size(), 0);
                Assertions.assertEquals(objectVersions.getDeleteMarkers().size(), 0);

                deleteAllObjectVersions(provider(), bucket);
            }

            // Delete object with versionId is not permitted when the object is in governance mode and the retention period has not expired,
            // unless the request is made with bypass governance retention.
            {
                String objectKey2 = "objectKey2";
                Date retainDate = new Date(System.currentTimeMillis() + 1000 * 10);
                candyS3.putObject(bucket, objectKey2, new PutObjectOptions.PutObjectOptionsBuilder()
                        .configureObjectLockOptions().lockMode(ObjectRetentionMode.GOVERNANCE).retainUntilDate(retainDate).endConfigureObjectLockOptions()
                        .build());

                ListPaginationResult<S3ObjectVersion> objectVersions = candyS3.listObjectVersions(bucket, new ListObjectVersionsOptions());

                candyS3.deleteObject(bucket, new DeleteObjectOptions(objectKey2)
                        .versionId(objectVersions.getResults().get(0).getVersionId())
                        .bypassGovernanceRetention());

                objectVersions = candyS3.listObjectVersions(bucket, new ListObjectVersionsOptions());
                Assertions.assertEquals(objectVersions.getResults().size(), 0);
                Assertions.assertEquals(objectVersions.getDeleteMarkers().size(), 0);

                deleteAllObjectVersions(provider(), bucket);
            }

            // In compliance mode, a protected object version can't be overwritten or deleted by any user
            {
                String objectKey3 = "objectKey3";
                Date retainDate = new Date(System.currentTimeMillis() + 1000 * 10);
                candyS3.putObject(bucket, objectKey3, new PutObjectOptions.PutObjectOptionsBuilder()
                        .configureObjectLockOptions().lockMode(ObjectRetentionMode.COMPLIANCE).retainUntilDate(retainDate).endConfigureObjectLockOptions()
                        .build());

                // Delete object without versionId is permitted when the object is in compliance mode and the retention period has not expired.
                candyS3.deleteObject(bucket, new DeleteObjectOptions(objectKey3));

                ListPaginationResult<S3Object> objects = candyS3.listObjects(bucket, new ListObjectOptions().prefix(objectKey3));
                Assertions.assertEquals(objects.getResults().size(), 0);

                ListPaginationResult<S3ObjectVersion> objectVersions = candyS3.listObjectVersions(bucket, new ListObjectVersionsOptions());
                Assertions.assertEquals(objectVersions.getResults().size(), 1);
                Assertions.assertEquals(objectVersions.getDeleteMarkers().size(), 1);

                // Delete deleteMarker to retain the object version for subsequent tests
                candyS3.deleteObject(bucket, new DeleteObjectOptions(objectKey3).versionId(objectVersions.getDeleteMarkers().get(0).getVersionId()));

                objectVersions = candyS3.listObjectVersions(bucket, new ListObjectVersionsOptions());

                // Delete object with versionId is not permitted when the object is in compliance mode and the retention period has not expired.
                try {
                    candyS3.deleteObject(bucket, new DeleteObjectOptions(objectKey3).versionId(objectVersions.getResults().get(0).getVersionId()));
                    Assertions.fail("Should not be here. Exception should be thrown when delete object version that is in compliance mode and the retention period has not expired.");
                } catch (Exception ex) {
                    Assertions.assertTrue(ex instanceof CandyS3Exception);
                    Assertions.assertEquals("AccessDenied", ((CandyS3Exception) ex).getParsedError().getCode());
                }

                objectVersions = candyS3.listObjectVersions(bucket, new ListObjectVersionsOptions());
                Assertions.assertEquals(objectVersions.getResults().size(), 1);
                Assertions.assertEquals(objectVersions.getDeleteMarkers().size(), 0);

                // Delete object with versionId is permitted when the object is in compliance mode and the retention period has expired.
                Thread.sleep(10 * 1000);
                candyS3.deleteObject(bucket, new DeleteObjectOptions(objectKey3).versionId(objectVersions.getResults().get(0).getVersionId()));

                objectVersions = candyS3.listObjectVersions(bucket, new ListObjectVersionsOptions());
                Assertions.assertEquals(objectVersions.getResults().size(), 0);
                Assertions.assertEquals(objectVersions.getDeleteMarkers().size(), 0);

                deleteAllObjectVersions(provider(), bucket);
            }

        } finally {
            candyS3.deleteBucket(bucket);
        }

    }

    @Test
    @Tag(TestTag.objectRetention)
    void deleteLegalHoldObjectTest() throws IOException, NoSuchAlgorithmException, InterruptedException {
        CandyS3 candyS3 = init(provider());
        String bucket = genTestBucketName("deleteLegalHoldObjectTest");
        try {

            candyS3.createBucket(new CreateBucketOptions.CreateBucketOptionsBuilder(bucket).enableObjectLock().build());

            String objectKey1 = "objectKey1";
            candyS3.putObject(bucket, objectKey1, new PutObjectOptions.PutObjectOptionsBuilder()
                    .configureObjectLockOptions().legalHold(true).endConfigureObjectLockOptions()
                    .build());

            // Delete object without versionId is permitted when the object is in legal hold
            candyS3.deleteObject(bucket, new DeleteObjectOptions(objectKey1));

            ListPaginationResult<S3Object> objects = candyS3.listObjects(bucket, new ListObjectOptions().prefix(objectKey1));
            Assertions.assertEquals(objects.getResults().size(), 0);

            ListPaginationResult<S3ObjectVersion> objectVersions = candyS3.listObjectVersions(bucket, new ListObjectVersionsOptions());
            Assertions.assertEquals(objectVersions.getResults().size(), 1);
            Assertions.assertEquals(objectVersions.getDeleteMarkers().size(), 1);

            // Delete deleteMarker to retain the object version for subsequent tests
            candyS3.deleteObject(bucket, new DeleteObjectOptions(objectKey1).versionId(objectVersions.getDeleteMarkers().get(0).getVersionId()));

            objectVersions = candyS3.listObjectVersions(bucket, new ListObjectVersionsOptions());
            // Delete object with versionId is not permitted when the object is in legal hold
            try {
                candyS3.deleteObject(bucket, new DeleteObjectOptions(objectKey1).versionId(objectVersions.getResults().get(0).getVersionId()));
                Assertions.fail("Should not be here. Exception should be thrown when delete object version that is in legal hold.");
            } catch (Exception ex) {
                Assertions.assertTrue(ex instanceof CandyS3Exception);
                Assertions.assertEquals("AccessDenied", ((CandyS3Exception) ex).getParsedError().getCode());
            }

            objectVersions = candyS3.listObjectVersions(bucket, new ListObjectVersionsOptions());
            Assertions.assertEquals(objectVersions.getResults().size(), 1);
            Assertions.assertEquals(objectVersions.getDeleteMarkers().size(), 0);

            for (S3ObjectVersion item : objectVersions.getResults()) {
                candyS3.updateObjectLegalHold(bucket, item.getKey(), new ObjectLegalHoldOptions().versionId(item.getVersionId()).legalHold(false));
            }

        } finally {
            deleteAllObjectVersions(provider(), bucket);
            candyS3.deleteBucket(bucket);
        }
    }

    @Test
    @Tag(TestTag.objectRetention)
    void updateObjectLegalHoldTest() throws IOException, NoSuchAlgorithmException, InterruptedException {
        CandyS3 candyS3 = init(provider());
        String bucket = genTestBucketName("updateObjectLegalHoldTest");
        try {
            candyS3.createBucket(new CreateBucketOptions.CreateBucketOptionsBuilder(bucket).enableObjectLock().build());

            String objectKey1 = "objectKey1";
            candyS3.putObject(bucket, objectKey1, new PutObjectOptions.PutObjectOptionsBuilder()
                    .configureObjectLockOptions().legalHold(true).endConfigureObjectLockOptions()
                    .build());
            Assertions.assertTrue(candyS3.isObjectLegalHold(bucket, objectKey1, new ObjectLegalHoldOptions()));

            candyS3.updateObjectLegalHold(bucket, objectKey1, new ObjectLegalHoldOptions().legalHold(false));
            Assertions.assertFalse(candyS3.isObjectLegalHold(bucket, objectKey1, new ObjectLegalHoldOptions()));

            candyS3.putObject(bucket, objectKey1, new PutObjectOptions.PutObjectOptionsBuilder()
                    .configureUploadData().withData(new byte[]{1}).endConfigureDataContent()
                    .build());

            try {
                candyS3.isObjectLegalHold(bucket, objectKey1, new ObjectLegalHoldOptions());
                Assertions.fail("Should not be here. Exception should be thrown when check legal hold if object-lock is disabled");
            } catch (Exception ex) {
                Assertions.assertTrue(ex instanceof CandyS3Exception);
                Assertions.assertEquals(((CandyS3Exception) ex).getCode(), CommonErrorCode.NO_SUCH_OBJECT_LOCK_CONFIGURATION.getCode());
                Assertions.assertEquals(((CandyS3Exception) ex).getParsedError().getCode(), "NoSuchObjectLockConfiguration");
            }

            candyS3.putObject(bucket, objectKey1, new PutObjectOptions.PutObjectOptionsBuilder()
                    .configureUploadData().withData(new byte[]{2}).endConfigureDataContent()
                    .configureObjectLockOptions().lockMode(ObjectRetentionMode.COMPLIANCE).retainUntilDate(new Date(System.currentTimeMillis() + 10 * 1000)).endConfigureObjectLockOptions()
                    .build());
            Assertions.assertFalse(candyS3.isObjectLegalHold(bucket, objectKey1, new ObjectLegalHoldOptions()));

            ListPaginationResult<S3ObjectVersion> objectVersions = candyS3.listObjectVersions(bucket, new ListObjectVersionsOptions());

            candyS3.updateObjectLegalHold(bucket, objectKey1, new ObjectLegalHoldOptions().legalHold(false).versionId(objectVersions.getResults().get(1).getVersionId()));

            Assertions.assertFalse(candyS3.isObjectLegalHold(bucket, objectKey1, new ObjectLegalHoldOptions()
                    .versionId(objectVersions.getResults().get(0).getVersionId())));
            Assertions.assertFalse(candyS3.isObjectLegalHold(bucket, objectKey1, new ObjectLegalHoldOptions()
                    .versionId(objectVersions.getResults().get(1).getVersionId())));
            Assertions.assertFalse(candyS3.isObjectLegalHold(bucket, objectKey1, new ObjectLegalHoldOptions()
                    .versionId(objectVersions.getResults().get(2).getVersionId())));

            candyS3.updateObjectLegalHold(bucket, objectKey1, new ObjectLegalHoldOptions().legalHold(true));
            Assertions.assertTrue(candyS3.isObjectLegalHold(bucket, objectKey1, new ObjectLegalHoldOptions()
                    .versionId(objectVersions.getResults().get(0).getVersionId())));
            Assertions.assertFalse(candyS3.isObjectLegalHold(bucket, objectKey1, new ObjectLegalHoldOptions()
                    .versionId(objectVersions.getResults().get(1).getVersionId())));
            Assertions.assertFalse(candyS3.isObjectLegalHold(bucket, objectKey1, new ObjectLegalHoldOptions()
                    .versionId(objectVersions.getResults().get(2).getVersionId())));

            candyS3.updateObjectLegalHold(bucket, objectKey1, new ObjectLegalHoldOptions().legalHold(true).versionId(objectVersions.getResults().get(2).getVersionId()));
            Assertions.assertTrue(candyS3.isObjectLegalHold(bucket, objectKey1, new ObjectLegalHoldOptions()
                    .versionId(objectVersions.getResults().get(0).getVersionId())));
            Assertions.assertFalse(candyS3.isObjectLegalHold(bucket, objectKey1, new ObjectLegalHoldOptions()
                    .versionId(objectVersions.getResults().get(1).getVersionId())));
            Assertions.assertTrue(candyS3.isObjectLegalHold(bucket, objectKey1, new ObjectLegalHoldOptions()
                    .versionId(objectVersions.getResults().get(2).getVersionId())));

            candyS3.updateObjectLegalHold(bucket, objectKey1, new ObjectLegalHoldOptions().legalHold(false)
                    .versionId(objectVersions.getResults().get(0).getVersionId()));
            candyS3.updateObjectLegalHold(bucket, objectKey1, new ObjectLegalHoldOptions().legalHold(false)
                    .versionId(objectVersions.getResults().get(1).getVersionId()));
            candyS3.updateObjectLegalHold(bucket, objectKey1, new ObjectLegalHoldOptions().legalHold(false)
                    .versionId(objectVersions.getResults().get(2).getVersionId()));

            Thread.sleep(1000 * 10);

        } finally {
            deleteAllObjectVersions(provider(), bucket);
            candyS3.deleteBucket(bucket);
        }
    }

    @Test
    @Tag(TestTag.basic)
    void deleteObjectsBatchTest() throws IOException, NoSuchAlgorithmException {
        CandyS3 candyS3 = init(provider());
        String bucket = genTestBucketName("deleteObjectsBatchTest");
        try {
            candyS3.createBucket(new CreateBucketOptions.CreateBucketOptionsBuilder(bucket).build());
            candyS3.putObject(bucket, "key1", new PutObjectOptions.PutObjectOptionsBuilder().build());
            candyS3.putObject(bucket, "key2", new PutObjectOptions.PutObjectOptionsBuilder().build());
            candyS3.putObject(bucket, "key3/1", new PutObjectOptions.PutObjectOptionsBuilder().build());
            candyS3.putObject(bucket, "key4", new PutObjectOptions.PutObjectOptionsBuilder().build());
            candyS3.putObject(bucket, "key5", new PutObjectOptions.PutObjectOptionsBuilder().build());
            candyS3.putObject(bucket, "key6/2", new PutObjectOptions.PutObjectOptionsBuilder().build());

            ListPaginationResult<S3Object> s3Objects = candyS3.listObjects(bucket, new ListObjectOptions());
            Assertions.assertEquals(s3Objects.getResults().size(), 6);

            DeleteObjectsBatchResult deleteObjectsBatchResult = candyS3.deleteObjectsBatch(bucket, new DeleteObjectsBatchOptions.DeleteObjectsBatchOptionsBuilder()
                    .addDeleteObject("key1")
                    .addDeleteObject("key2")
                    .addDeleteObject("key3/1")
                    .addDeleteObject("key4")
                    .build());

            Set<String> deletedKeys = deleteObjectsBatchResult.getDeleted().stream()
                    .map(DeleteObjectsBatchResult.DeletedObject::getKey)
                    .collect(Collectors.toSet());
            Assertions.assertEquals(deletedKeys.size(), 4);
            Assertions.assertTrue(deletedKeys.contains("key1"));
            Assertions.assertTrue(deletedKeys.contains("key2"));
            Assertions.assertTrue(deletedKeys.contains("key3/1"));
            Assertions.assertTrue(deletedKeys.contains("key4"));

            ListPaginationResult<S3Object> s3Objects2 = candyS3.listObjects(bucket, new ListObjectOptions());
            Assertions.assertEquals(s3Objects2.getResults().size(), 2);

            deleteObjectsBatchResult = candyS3.deleteObjectsBatch(bucket, new DeleteObjectsBatchOptions.DeleteObjectsBatchOptionsBuilder()
                    .addDeleteObjects(Arrays.asList("key5", "key6/2"))
                    .build());
            ListPaginationResult<S3Object> s3Objects3 = candyS3.listObjects(bucket, new ListObjectOptions());
            Assertions.assertEquals(s3Objects3.getResults().size(), 0);

            deletedKeys = deleteObjectsBatchResult.getDeleted().stream()
                    .map(DeleteObjectsBatchResult.DeletedObject::getKey)
                    .collect(Collectors.toSet());
            Assertions.assertEquals(deletedKeys.size(), 2);
            Assertions.assertTrue(deletedKeys.contains("key5"));
            Assertions.assertTrue(deletedKeys.contains("key6/2"));

        } finally {
            candyS3.deleteBucket(bucket);
        }
    }

    @Test
    @Tag(TestTag.versioning)
    void deleteVersioningObjectsBatchTest() throws IOException, NoSuchAlgorithmException {
        CandyS3 candyS3 = init(provider());
        String bucket = genTestBucketName("dVOBatchTest");
        try {
            candyS3.createBucket(new CreateBucketOptions.CreateBucketOptionsBuilder(bucket).enableObjectLock().build());

            // Tencent cloud COS and Aliyun OSS: enable object-lock when create bucket is not supported, and default versioning is false
            if (S3Provider.TENCENTCLOUD_COS.equals(provider()) || S3Provider.ALIYUN_OSS.equals(provider())) {
                candyS3.setBucketVersioning(bucket, true);
            }

            String objectKey1 = "objectKey1";
            candyS3.putObject(bucket, objectKey1, new PutObjectOptions.PutObjectOptionsBuilder()
                    .configureUploadData().withData(new byte[]{1}).endConfigureDataContent().build());
            candyS3.putObject(bucket, objectKey1, new PutObjectOptions.PutObjectOptionsBuilder()
                    .configureUploadData().withData(new byte[]{2}).endConfigureDataContent().build());

            ListPaginationResult<S3ObjectVersion> objectVersions = candyS3.listObjectVersions(bucket, new ListObjectVersionsOptions());

            {
                String deleteVersionId = objectVersions.getResults().get(1).getVersionId();
                DeleteObjectsBatchResult deleteObjectsBatchResult = candyS3.deleteObjectsBatch(bucket, new DeleteObjectsBatchOptions.DeleteObjectsBatchOptionsBuilder()
                        .addDeleteObject(objectKey1, deleteVersionId)
                        .build());

                Assertions.assertEquals(candyS3.listObjectVersions(bucket, new ListObjectVersionsOptions()).getResults().size(), 1);
                Assertions.assertEquals(deleteObjectsBatchResult.getDeleted().size(), 1);
                Assertions.assertEquals(deleteObjectsBatchResult.getDeleted().get(0).getKey(), objectKey1);
                Assertions.assertEquals(deleteObjectsBatchResult.getDeleted().get(0).getVersionId(), deleteVersionId);
            }


            {
                DeleteObjectsBatchResult deleteObjectsBatchResult = candyS3.deleteObjectsBatch(bucket, new DeleteObjectsBatchOptions.DeleteObjectsBatchOptionsBuilder()
                        .addDeleteObject(objectKey1)
                        .build());

                Assertions.assertEquals(candyS3.listObjectVersions(bucket, new ListObjectVersionsOptions()).getDeleteMarkers().size(), 1);
                Assertions.assertEquals(deleteObjectsBatchResult.getDeleted().size(), 1);
                Assertions.assertEquals(deleteObjectsBatchResult.getDeleted().get(0).getDeleteMarker(), true);
            }


        } finally {
            deleteAllObjectVersions(provider(), bucket);
            candyS3.deleteBucket(bucket);
        }
    }

    @Test
    @Tag(TestTag.conditional)
    void deleteObjectConditionalTest() throws IOException, NoSuchAlgorithmException {
        CandyS3 candyS3 = init(provider());
        String bucket = genTestBucketName("dOCTest");
        try {
            candyS3.createBucket(new CreateBucketOptions.CreateBucketOptionsBuilder(bucket).build());

            // delete object with if-match matches etag
            {
                String objectKey1 = "objectKey1";
                candyS3.putObject(bucket, objectKey1, new PutObjectOptions.PutObjectOptionsBuilder()
                        .configureUploadData().withData(new byte[]{1}).endConfigureDataContent().build());
                Assertions.assertEquals(candyS3.listObjects(bucket, new ListObjectOptions()).getResults().size(), 1);

                String etag = candyS3.getObjectMetadata(bucket, objectKey1, new DownloadObjectOptions.DownloadObjectOptionsBuilder().build()).geteTag();
                candyS3.deleteObject(bucket, new DeleteObjectOptions(objectKey1)
                        .configureConditionalOptions().ifMatch(etag).endConfigure());
                Assertions.assertEquals(candyS3.listObjects(bucket, new ListObjectOptions()).getResults().size(), 0);
            }

            // delete object with if-match use * to match any etag
            {
                String objectKey2 = "objectKey2";
                candyS3.putObject(bucket, objectKey2, new PutObjectOptions.PutObjectOptionsBuilder()
                        .configureUploadData().withData(new byte[]{1}).endConfigureDataContent().build());
                Assertions.assertEquals(candyS3.listObjects(bucket, new ListObjectOptions()).getResults().size(), 1);

                candyS3.deleteObject(bucket, new DeleteObjectOptions(objectKey2)
                        .configureConditionalOptions().matchAny().endConfigure());
                Assertions.assertEquals(candyS3.listObjects(bucket, new ListObjectOptions()).getResults().size(), 0);
            }

            // delete object with if-match not match etag, should throw PreconditionFailed
            {
                String objectKey3 = "objectKey3";
                candyS3.putObject(bucket, objectKey3, new PutObjectOptions.PutObjectOptionsBuilder()
                        .configureUploadData().withData(new byte[]{1}).endConfigureDataContent().build());
                Assertions.assertEquals(candyS3.listObjects(bucket, new ListObjectOptions()).getResults().size(), 1);

                try {
                    candyS3.deleteObject(bucket, new DeleteObjectOptions(objectKey3)
                            .configureConditionalOptions().ifMatch("x").endConfigure());
                    Assertions.fail("Should not be here. PreconditionFailed should be thrown.");
                } catch (Exception ex) {
                    Assertions.assertTrue(ex instanceof CandyS3Exception);
                    Assertions.assertEquals(CommonErrorCode.OBJECT_PRECONDITION_FAILED.getCode(), ((CandyS3Exception) ex).getCode());
                    Assertions.assertEquals("PreconditionFailed", ((CandyS3Exception) ex).getParsedError().getCode());
                }
                Assertions.assertEquals(candyS3.listObjects(bucket, new ListObjectOptions()).getResults().size(), 1);
            }

        } finally {
            deleteAllObject(provider(), bucket);
            candyS3.deleteBucket(bucket);
        }
    }

    @Test
    @Tag(TestTag.conditional)
    void deleteObjectsBatchConditionalTest() throws IOException, NoSuchAlgorithmException {
        CandyS3 candyS3 = init(provider());
        String bucket = genTestBucketName("dOBCTest");
        try {
            candyS3.createBucket(new CreateBucketOptions.CreateBucketOptionsBuilder(bucket).build());

            // delete objects with all if-match matches etag
            {
                String objectKey1 = "objectKey1";
                String objectKey2 = "objectKey2";
                candyS3.putObject(bucket, objectKey1, new PutObjectOptions.PutObjectOptionsBuilder()
                        .configureUploadData().withData(new byte[]{1}).endConfigureDataContent().build());
                candyS3.putObject(bucket, objectKey2, new PutObjectOptions.PutObjectOptionsBuilder()
                        .configureUploadData().withData(new byte[]{1}).endConfigureDataContent().build());

                Assertions.assertEquals(2, candyS3.listObjects(bucket, new ListObjectOptions()).getResults().size());

                String object1Etag = candyS3.getObjectMetadata(bucket, objectKey1, new DownloadObjectOptions.DownloadObjectOptionsBuilder().build()).geteTag();
                String object2Etag = candyS3.getObjectMetadata(bucket, objectKey2, new DownloadObjectOptions.DownloadObjectOptionsBuilder().build()).geteTag();
                candyS3.deleteObjectsBatch(bucket, new DeleteObjectsBatchOptions.DeleteObjectsBatchOptionsBuilder()
                        .addConditionalDeleteObject(objectKey1, object1Etag)
                        .addConditionalDeleteObject(objectKey2, object2Etag)
                        .build());
                Assertions.assertEquals(0, candyS3.listObjects(bucket, new ListObjectOptions()).getResults().size());
            }

            // delete object with all if-match use * to match any etag
            {
                String objectKey3 = "objectKey3";
                String objectKey4 = "objectKey4";
                candyS3.putObject(bucket, objectKey3, new PutObjectOptions.PutObjectOptionsBuilder()
                        .configureUploadData().withData(new byte[]{1}).endConfigureDataContent().build());
                candyS3.putObject(bucket, objectKey4, new PutObjectOptions.PutObjectOptionsBuilder()
                        .configureUploadData().withData(new byte[]{1}).endConfigureDataContent().build());
                Assertions.assertEquals(2, candyS3.listObjects(bucket, new ListObjectOptions()).getResults().size());

                candyS3.deleteObjectsBatch(bucket, new DeleteObjectsBatchOptions.DeleteObjectsBatchOptionsBuilder()
                        .addConditionalDeleteObject(objectKey3, "*")
                        .addConditionalDeleteObject(objectKey4, "*")
                        .build());
                Assertions.assertEquals(0, candyS3.listObjects(bucket, new ListObjectOptions()).getResults().size());
            }

            // delete object with some if-match matches etag, will only delete these objects
            {
                String objectKey5 = "objectKey5";
                String objectKey6 = "objectKey6";
                candyS3.putObject(bucket, objectKey5, new PutObjectOptions.PutObjectOptionsBuilder()
                        .configureUploadData().withData(new byte[]{1}).endConfigureDataContent().build());
                candyS3.putObject(bucket, objectKey6, new PutObjectOptions.PutObjectOptionsBuilder()
                        .configureUploadData().withData(new byte[]{1}).endConfigureDataContent().build());
                Assertions.assertEquals(2, candyS3.listObjects(bucket, new ListObjectOptions()).getResults().size());

                String object5Etag = candyS3.getObjectMetadata(bucket, objectKey5, new DownloadObjectOptions.DownloadObjectOptionsBuilder().build()).geteTag();
                DeleteObjectsBatchResult result = candyS3.deleteObjectsBatch(bucket, new DeleteObjectsBatchOptions.DeleteObjectsBatchOptionsBuilder()
                        .addConditionalDeleteObject(objectKey5, object5Etag)
                        .addConditionalDeleteObject(objectKey6, "x")
                        .build());
                Assertions.assertFalse(result.isSuccessful());
                Assertions.assertEquals(1, result.getDeleted().size());
                Assertions.assertEquals(objectKey5, result.getDeleted().get(0).getKey());
                Assertions.assertEquals(1, result.getErrors().size());
                Assertions.assertEquals(objectKey6, result.getErrors().get(0).getKey());

                List<S3Object> objects = candyS3.listObjects(bucket, new ListObjectOptions()).getResults();
                Assertions.assertEquals(1, objects.size());
                Assertions.assertEquals(objectKey6, objects.get(0).getKey());

                deleteAllObject(provider(), bucket);
            }

            // delete object with some if-match use * to match any etag and other if-match not matches etag, will only delete these objects
            {
                String objectKey7 = "objectKey7";
                String objectKey8 = "objectKey8";
                candyS3.putObject(bucket, objectKey7, new PutObjectOptions.PutObjectOptionsBuilder()
                        .configureUploadData().withData(new byte[]{1}).endConfigureDataContent().build());
                candyS3.putObject(bucket, objectKey8, new PutObjectOptions.PutObjectOptionsBuilder()
                        .configureUploadData().withData(new byte[]{1}).endConfigureDataContent().build());
                Assertions.assertEquals(2, candyS3.listObjects(bucket, new ListObjectOptions()).getResults().size());

                DeleteObjectsBatchResult result = candyS3.deleteObjectsBatch(bucket, new DeleteObjectsBatchOptions.DeleteObjectsBatchOptionsBuilder()
                        .addConditionalDeleteObject(objectKey7, "*")
                        .addConditionalDeleteObject(objectKey8, "x")
                        .build());
                Assertions.assertFalse(result.isSuccessful());
                Assertions.assertEquals(1, result.getDeleted().size());
                Assertions.assertEquals(objectKey7, result.getDeleted().get(0).getKey());
                Assertions.assertEquals(1, result.getErrors().size());
                Assertions.assertEquals(objectKey8, result.getErrors().get(0).getKey());

                List<S3Object> objects = candyS3.listObjects(bucket, new ListObjectOptions()).getResults();
                Assertions.assertEquals(1, objects.size());
                Assertions.assertEquals(objectKey8, objects.get(0).getKey());
            }

        } finally {
            deleteAllObject(provider(), bucket);
            candyS3.deleteBucket(bucket);
        }
    }

    @Test
    @Tag(TestTag.objectRetention)
    void deleteRetainObjectsBatchTest() throws IOException, NoSuchAlgorithmException {
        CandyS3 candyS3 = init(provider());
        String bucket = genTestBucketName("deleteRetainObjectsBatchTest");
        try {

            candyS3.createBucket(new CreateBucketOptions.CreateBucketOptionsBuilder(bucket).enableObjectLock().build());

            String objectKey1 = "objectKey1";
            String objectKey2 = "objectKey2";

            {
                Date retainDate = new Date(System.currentTimeMillis() + 1000 * 10);
                candyS3.putObject(bucket, objectKey1, new PutObjectOptions.PutObjectOptionsBuilder()
                        .configureObjectLockOptions().lockMode(ObjectRetentionMode.GOVERNANCE).retainUntilDate(retainDate).endConfigureObjectLockOptions()
                        .build());
                candyS3.putObject(bucket, objectKey2, new PutObjectOptions.PutObjectOptionsBuilder()
                        .build());

                ListPaginationResult<S3ObjectVersion> objectVersions = candyS3.listObjectVersions(bucket, new ListObjectVersionsOptions());

                DeleteObjectsBatchResult deleteObjectsBatchResult = candyS3.deleteObjectsBatch(bucket, new DeleteObjectsBatchOptions.DeleteObjectsBatchOptionsBuilder()
                        .addDeleteObject(objectVersions.getResults().get(0).getKey(), objectVersions.getResults().get(0).getVersionId())
                        .addDeleteObject(objectVersions.getResults().get(1).getKey(), objectVersions.getResults().get(1).getVersionId())
                        .build());

                Assertions.assertFalse(deleteObjectsBatchResult.isSuccessful());

                Assertions.assertEquals(deleteObjectsBatchResult.getDeleted().size(), 1);
                Assertions.assertEquals(deleteObjectsBatchResult.getDeleted().get(0).getKey(), objectKey2);

                Assertions.assertEquals(deleteObjectsBatchResult.getErrors().get(0).getKey(), objectKey1);
                Assertions.assertEquals(deleteObjectsBatchResult.getErrors().get(0).getCode(), "AccessDenied");

                deleteAllObjectVersions(provider(), bucket);
            }

            {
                Date retainDate = new Date(System.currentTimeMillis() + 1000 * 10);
                candyS3.putObject(bucket, objectKey1, new PutObjectOptions.PutObjectOptionsBuilder()
                        .configureObjectLockOptions().lockMode(ObjectRetentionMode.GOVERNANCE).retainUntilDate(retainDate).endConfigureObjectLockOptions()
                        .build());
                candyS3.putObject(bucket, objectKey2, new PutObjectOptions.PutObjectOptionsBuilder()
                        .build());

                ListPaginationResult<S3ObjectVersion> objectVersions = candyS3.listObjectVersions(bucket, new ListObjectVersionsOptions());

                DeleteObjectsBatchResult deleteObjectsBatchResult = candyS3.deleteObjectsBatch(bucket, new DeleteObjectsBatchOptions.DeleteObjectsBatchOptionsBuilder()
                        .addDeleteObject(objectVersions.getResults().get(0).getKey(), objectVersions.getResults().get(0).getVersionId())
                        .addDeleteObject(objectVersions.getResults().get(1).getKey(), objectVersions.getResults().get(1).getVersionId())
                        .bypassGovernanceRetention()
                        .build());

                Assertions.assertTrue(deleteObjectsBatchResult.isSuccessful());
                Assertions.assertEquals(deleteObjectsBatchResult.getDeleted().size(), 2);
                Assertions.assertNull(deleteObjectsBatchResult.getErrors());

                deleteAllObjectVersions(provider(), bucket);
            }

        } finally {
            candyS3.deleteBucket(bucket);
        }
    }

    @Test
    @Tag(TestTag.objectRetention)
    void deleteLegalHoldObjectsBatchTest() throws IOException, NoSuchAlgorithmException {
        CandyS3 candyS3 = init(provider());
        String bucket = genTestBucketName("dLHOBatchTest");
        try {

            candyS3.createBucket(new CreateBucketOptions.CreateBucketOptionsBuilder(bucket).enableObjectLock().build());

            // Enable object-lock when create bucket is not supported by Tencent cloud COS
            if (S3Provider.TENCENTCLOUD_COS.equals(provider())) {
                candyS3.enableBucketObjectLock(bucket, new UpdateBucketObjectLockOptions.UpdateBucketObjectLockOptionsBuilder().buildWithoutRetention());
            }

            String objectKey1 = "objectKey1";
            String objectKey2 = "objectKey2";

            candyS3.putObject(bucket, objectKey1, new PutObjectOptions.PutObjectOptionsBuilder()
                    .configureObjectLockOptions().legalHold(true).endConfigureObjectLockOptions()
                    .build());
            candyS3.putObject(bucket, objectKey2, new PutObjectOptions.PutObjectOptionsBuilder()
                    .build());

            ListPaginationResult<S3ObjectVersion> objectVersions = candyS3.listObjectVersions(bucket, new ListObjectVersionsOptions());

            DeleteObjectsBatchResult deleteObjectsBatchResult = candyS3.deleteObjectsBatch(bucket, new DeleteObjectsBatchOptions.DeleteObjectsBatchOptionsBuilder()
                    .addDeleteObject(objectVersions.getResults().get(0).getKey(), objectVersions.getResults().get(0).getVersionId())
                    .addDeleteObject(objectVersions.getResults().get(1).getKey(), objectVersions.getResults().get(1).getVersionId())
                    .build());

            Assertions.assertFalse(deleteObjectsBatchResult.isSuccessful());

            Assertions.assertEquals(deleteObjectsBatchResult.getDeleted().size(), 1);
            Assertions.assertEquals(deleteObjectsBatchResult.getDeleted().get(0).getKey(), objectKey2);

            Assertions.assertEquals(deleteObjectsBatchResult.getErrors().get(0).getKey(), objectKey1);
            if (S3Provider.TENCENTCLOUD_COS.equals(provider())) {
                Assertions.assertEquals(deleteObjectsBatchResult.getErrors().get(0).getCode(), "ObjectLocked");
            } else {
                Assertions.assertEquals(deleteObjectsBatchResult.getErrors().get(0).getCode(), "AccessDenied");
            }

            candyS3.updateObjectLegalHold(bucket, objectKey1, new ObjectLegalHoldOptions().legalHold(false));


        } finally {
            deleteAllObjectVersions(provider(), bucket);
            candyS3.deleteBucket(bucket);
        }
    }

    void deleteAllObjectVersions(S3Provider provider, String bucket) throws IOException {
        if (S3Provider.CLOUDFLARE_R2.equals(provider)) {
            deleteAllObject(provider, bucket);
            return;
        }

        CandyS3 candyS3 = init(provider());

        BucketObjectLockConfiguration bucketObjectLockConfiguration = null;
        try {
            bucketObjectLockConfiguration = candyS3.getBucketObjectLockConfiguration(bucket);
        } catch (Exception ex) {
        }

        outer:
        while (true) {
            ListPaginationResult<S3ObjectVersion> result = candyS3.listObjectVersions(bucket, new ListObjectVersionsOptions());
            if (result.getResults().isEmpty() && result.getDeleteMarkers().isEmpty()) {
                return;
            }

            for (S3ObjectVersion objectVersion : result.getResults()) {
                try {
                    // TODO Aliyun OSS will retain the object after upload legal hold?
                    if (!S3Provider.ALIYUN_OSS.equals(provider())) {
                        candyS3.updateObjectLegalHold(bucket, objectVersion.getKey(), new ObjectLegalHoldOptions().versionId(objectVersion.getVersionId()).legalHold(false));
                    }
                } catch (Exception ex) {
//                System.out.println("disable object legal hold:[" + objectVersion.getKey() + "]-[" + objectVersion.getVersionId() + "] error:");
//                ex.printStackTrace();
                }

                try {
                    DeleteObjectOptions deleteObjectOptions = new DeleteObjectOptions(objectVersion.getKey()).versionId(objectVersion.getVersionId());
                    if (bucketObjectLockConfiguration != null && bucketObjectLockConfiguration.isObjectLockEnabled()) {
                        deleteObjectOptions.bypassGovernanceRetention();
                    }
                    candyS3.deleteObject(bucket, deleteObjectOptions);
                } catch (Exception ex) {
                    System.out.println("delete object version:[" + objectVersion.getKey() + "]-[" + objectVersion.getVersionId() + "] error:");
                    ex.printStackTrace();
                    break outer;
                }
            }
            for (S3ObjectVersion deleteMarker : result.getDeleteMarkers()) {
                try {
                    candyS3.deleteObject(bucket, new DeleteObjectOptions(deleteMarker.getKey()).versionId(deleteMarker.getVersionId()));
                } catch (Exception ex) {
                    System.out.println("delete object deleteMarker:[" + deleteMarker.getKey() + "]-[" + deleteMarker.getVersionId() + "] error:");
                    ex.printStackTrace();
                    break outer;
                }
            }
        }

    }

    void deleteAllObject(S3Provider provider, String bucket) throws IOException {
        CandyS3 candyS3 = init(provider);

        BucketObjectLockConfiguration bucketObjectLockConfiguration = null;
        try {
            bucketObjectLockConfiguration = candyS3.getBucketObjectLockConfiguration(bucket);
        } catch (Exception ex) {
        }

        while (true) {
            ListPaginationResult<S3Object> result = candyS3.listObjects(bucket, new ListObjectOptions());
            if (result.getResults().isEmpty()) {
                return;
            }

            for (S3Object s3Object : result.getResults()) {
                try {
                    // TODO Aliyun OSS will retain the object after upload legal hold?
                    if (!S3Provider.ALIYUN_OSS.equals(provider())) {
                        candyS3.updateObjectLegalHold(bucket, s3Object.getKey(), new ObjectLegalHoldOptions().legalHold(false));
                    }
                } catch (Exception ex) {
//                System.out.println("disable object legal hold:[" + objectVersion.getKey() + "]-[" + objectVersion.getVersionId() + "] error:");
//                ex.printStackTrace();
                }

                try {
                    DeleteObjectOptions deleteObjectOptions = new DeleteObjectOptions(s3Object.getKey());
                    if (bucketObjectLockConfiguration != null && bucketObjectLockConfiguration.isObjectLockEnabled()) {
                        deleteObjectOptions.bypassGovernanceRetention();
                    }
                    candyS3.deleteObject(bucket, deleteObjectOptions);
                } catch (Exception ex) {
                    System.out.println("delete object:[" + s3Object.getKey() + "] error:");
                    ex.printStackTrace();
                }
            }
        }
    }

    void abortAllMultipartUploads(S3Provider provider, String bucket) throws IOException {
        CandyS3 candyS3 = init(provider);

        while (true) {
            ListPaginationResult<S3MultipartUpload> result = candyS3.listMultipartUploads(bucket, new ListMultipartUploadOptions());
            if (result.getResults().isEmpty() && result.getDeleteMarkers().isEmpty()) {
                return;
            }

            for (S3MultipartUpload multipartUpload : result.getResults()) {
                try {
                    candyS3.abortMultipartUpload(bucket, multipartUpload.getKey(), multipartUpload.getUploadId(), new AbortMultipartUploadOptions());
                } catch (Exception ex) {
                    System.out.println("abort multipart upload:[" + multipartUpload.getKey() + "]-[" + multipartUpload.getUploadId() + "] error:");
                    ex.printStackTrace();
                }
            }
        }
    }

    void removeTestBuckets(S3Provider provider) throws IOException {
        CandyS3 candyS3 = init(provider);

        while (true) {
            List<Bucket> buckets = candyS3.listBucket(new ListBucketOptions()).getResults();
            if (buckets.isEmpty() || buckets.stream().noneMatch(b -> b.getName().startsWith("test-"))) {
                return;
            }
            for (Bucket b : buckets) {
                if (!b.getName().startsWith("test-")) {
                    continue;
                }
                try {
                    if (StringUtils.isNotBlank(b.getBucketRegion())) {
                        candyS3.setRegion(b.getBucketRegion());
                    }

                    abortAllMultipartUploads(provider(), b.getName());
                    if (!S3Provider.CLOUDFLARE_R2.equals(provider())) {
                        deleteAllObjectVersions(provider(), b.getName());
                    } else {
                        deleteAllObject(provider(), b.getName());
                    }

                    candyS3.deleteBucket(b.getName());
                } catch (Exception ex) {
                    System.out.println("delete bucket:[" + b.getName() + "] error:");
                    ex.printStackTrace();
                }
            }
        }
    }


    public static void mkTempFiles() {
        Logger.getLogger(OkHttpClient.class.getName()).setLevel(Level.FINE);

        File tempDir = new File("./temp/");
        if (!tempDir.exists()) {
            tempDir.mkdirs();
        }
    }

    public static void cleanUpTempFiles() throws IOException {
        File tempDir = new File("./temp/");
        if (tempDir.exists() && tempDir.isDirectory()) {
            for (File f : tempDir.listFiles()) {
                f.delete();
            }
        }
    }

}
