package io.github.matian2014.candys3;

import io.github.matian2014.candys3.exceptions.CandyS3Exception;
import io.github.matian2014.candys3.exceptions.CommonErrorCode;
import io.github.matian2014.candys3.exceptions.S3ServerError;
import io.github.matian2014.candys3.options.*;
import io.github.matian2014.candys3.signer.*;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.dataformat.xml.XmlMapper;
import com.fasterxml.jackson.dataformat.xml.annotation.JacksonXmlElementWrapper;
import com.fasterxml.jackson.dataformat.xml.annotation.JacksonXmlProperty;
import io.github.matian2014.candys3.options.*;
import io.github.matian2014.candys3.signer.*;
import okhttp3.*;
import org.apache.commons.lang3.ArrayUtils;
import org.apache.commons.lang3.StringUtils;

import java.io.*;
import java.net.MalformedURLException;
import java.net.URL;
import java.security.NoSuchAlgorithmException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.*;
import java.util.concurrent.TimeUnit;

public class CandyS3 {

    /**
     * The S3 provider to use.
     */
    protected final S3Provider provider;


    /**
     * The region of the S3 service.
     */
    protected String region;

    /**
     * The access key of the S3 service.
     */
    protected String accessKey;
    /**
     * The secret key of the S3 service.
     */
    protected String secretKey;

    /**
     * Whether to use SSL for requests.
     */
    protected boolean useSSL = false;

    /**
     * Whether to use the S3 Accelerate endpoint.
     */
    protected boolean useAccelerate = false;

    /**
     * The timeout for requests, in seconds. 0 for indefinite timeout.
     */
    protected long timeout = 60;

    /**
     * The domain of the custom S3 service.
     */
    protected String customProviderDomain;
    /**
     * Whether to use path-style addressing for requests to the custom S3 service.
     */
    protected boolean customProviderUsePathStyle = false;

    /**
     * The account ID of the Cloudflare R2 service.
     */
    protected String cloudflareR2AccountId;

    protected final XmlMapper xmlMapper;
    protected final OkHttpClient okHttpClient;

    /**
     * The default chunk size for multipart uploads, in bytes.
     */
    private static final Integer SIZE_5M = 1024 * 1024 * 5;

    public CandyS3(S3Provider provider) {
        this.provider = provider;
        this.xmlMapper = XmlMapper.xmlBuilder().build();
        this.okHttpClient = new OkHttpClient().newBuilder().build();
    }

    public S3Provider getProvider() {
        return provider;
    }

    public String getCustomProviderDomain() {
        return customProviderDomain;
    }

    public boolean isCustomProviderUsePathStyle() {
        return customProviderUsePathStyle;
    }

    public void setCustomProviderDomain(String customProviderDomain) {
        this.customProviderDomain = customProviderDomain;
    }

    public void setCustomProviderUsePathStyle(boolean customProviderUsePathStyle) {
        this.customProviderUsePathStyle = customProviderUsePathStyle;
    }

    public long getTimeout() {
        return timeout;
    }

    public void setTimeout(long timeout) {
        this.timeout = timeout;
    }

    public String getRegion() {
        return region;
    }

    public void setRegion(String region) {
        this.region = region;
    }

    public boolean isUseSSL() {
        return useSSL;
    }

    public void setUseSSL(boolean useSSL) {
        this.useSSL = useSSL;
    }

    public boolean isUseAccelerate() {
        return useAccelerate;
    }

    public void setUseAccelerate(boolean useAccelerate) {
        this.useAccelerate = useAccelerate;
    }

    public String getAccessKey() {
        return accessKey;
    }

    public void setAccessKey(String accessKey) {
        this.accessKey = accessKey;
    }

    public String getSecretKey() {
        return secretKey;
    }

    public void setSecretKey(String secretKey) {
        this.secretKey = secretKey;
    }

    public String getCloudflareR2AccountId() {
        return cloudflareR2AccountId;
    }

    public void setCloudflareR2AccountId(String cloudflareR2AccountId) {
        this.cloudflareR2AccountId = cloudflareR2AccountId;
    }

    protected String buildUrl(String bucket) {
        switch (provider) {
            case AWS:
                // http[s]://[bucket.]s3[.region].domain
                // http[s]://[bucket.]s3-accelerate[.region].domain
                return String.format("http%s://%s%s%s%s", useSSL ? "s" : "", StringUtils.isEmpty(bucket) ? "" : bucket + ".",
                        useAccelerate ? "s3-accelerate" : "s3",
                        StringUtils.isEmpty(region) ? "" : ("." + region), provider.getDomain());
            case CLOUDFLARE_R2:
                return String.format("http%s://%s%s%s", useSSL ? "s" : "",
                        cloudflareR2AccountId, provider.getDomain(), StringUtils.isEmpty(bucket) ? "" : "/" + bucket);
            case ALIYUN_OSS:
                return String.format("http%s://%s%s%s", useSSL ? "s" : "", StringUtils.isEmpty(bucket) ? "" : bucket + ".",
                        useAccelerate ? "oss-accelerate" : "oss-" + region, provider.getDomain());
            case TENCENTCLOUD_COS:
                return String.format("http%s://%s%s%s", useSSL ? "s" : "", StringUtils.isEmpty(bucket) ? "" : bucket + ".",
                        useAccelerate ? "cos.accelerate" : "cos." + region, provider.getDomain());
            case CUSTOM:
                return String.format("http%s://%s/%s", useSSL ? "s" : "",
                        customProviderDomain, StringUtils.isEmpty(bucket) ? "" : bucket);
            default:
                throw new IllegalArgumentException("provider not supported.");
        }
    }

    /**
     * Returns a list of all buckets owned by the authenticated sender of the request.
     * @param options the options to list buckets.
     * @return the list of buckets.
     * @throws IOException if an error occurs when send request.
     * @see <a href="https://docs.aws.amazon.com/AmazonS3/latest/API/API_ListBuckets.html">ListBuckets</a>
     */
    public ListPaginationResult<Bucket> listBucket(ListBucketOptions options) throws IOException {
        if (options.getMaxBuckets() != null && (options.getMaxBuckets() > 10000 || options.getMaxBuckets() < 1)) {
            throw new IllegalArgumentException("maxBuckets must between 1 to 10000.");
        }
        Map<String, String> params = new HashMap<>();
        if (options.isFilterBucketRegion()) {
            params.put("bucket-region", region);
        }
        if (!StringUtils.isEmpty(options.getContinuationToken())) {
            params.put("continuation-token", options.getContinuationToken());
        }
        if (options.getMaxBuckets() != null) {
            params.put("max-buckets", options.getMaxBuckets() + "");
        }
        if (!StringUtils.isEmpty(options.getPrefix())) {
            params.put("prefix", options.getPrefix());
        }

        Map<String, String> headers = new HashMap<String, String>();
        headers.put(HttpConstants.HEADER_AMZ_CONTENT_HASH, AWS4SignerBase.EMPTY_BODY_SHA256);

        URL url = new URL(buildUrl("") + "/" + buildParams(params));

        AWS4SignerForAuthorizationHeader signer = new AWS4SignerForAuthorizationHeader(
                url, HttpConstants.HTTP_METHOD_GET, HttpConstants.CONSTANT_AWS_SERVICENAME_S3, region);
        String authorization = signer.computeSignature(headers,
                params,
                AWS4SignerBase.EMPTY_BODY_SHA256,
                accessKey,
                secretKey);

        headers.put(HttpConstants.HEADER_AUTHORIZATION, authorization);

        Request.Builder requestBuilder = new Request.Builder()
                .url(url)
                .get();
        for (Map.Entry<String, String> header : headers.entrySet()) {
            requestBuilder.addHeader(header.getKey(), header.getValue());
        }
        Call call = okHttpClient.newCall(requestBuilder.build());
        call.timeout().timeout(timeout, TimeUnit.SECONDS);
        try (Response response = call.execute()) {
            if (!response.isSuccessful()) {
                throw new CandyS3Exception(CommonErrorCode.SERVER_ERROR.getCode(), parseErrorMessage(response));
            }
            JsonNode root = xmlMapper.readTree(response.body().string());

            JsonNode bucketNode = root.path("Buckets").path("Bucket");
            List<Bucket> buckets = new ArrayList<>(0);
            if (!bucketNode.isMissingNode()) {
                // jackson cant recognize node as an array when it has only one child.
                if (bucketNode.isArray()) {
                    buckets = xmlMapper.treeToValue(bucketNode,
                            xmlMapper.getTypeFactory().constructCollectionType(List.class, Bucket.class));
                } else {
                    buckets = new ArrayList<>(1);
                    buckets.add(xmlMapper.treeToValue(bucketNode, Bucket.class));
                }
            }

            JsonNode ownerNode = root.path("Owner");
            Owner owner = null;
            if (!ownerNode.isMissingNode()) {
                owner = xmlMapper.treeToValue(ownerNode, Owner.class);
            }
            for (Bucket bucket : buckets) {
                bucket.setOwner(owner);
            }

            return new ListPaginationResult<Bucket>(buckets, root.path("ContinuationToken").asText());
        }
    }

    /**
     * Use this operation to determine if a bucket exists and if you have permission to access it.
     * @param bucket the bucket name.
     * @return true if the bucket exists, false otherwise.
     * @throws IOException if an error occurs when send request.
     * @see <a href="https://docs.aws.amazon.com/AmazonS3/latest/API/API_HeadBucket.html">HeadBucket</a>
     */
    public boolean bucketExists(String bucket) throws IOException {
        if (StringUtils.isEmpty(bucket)) {
            throw new IllegalArgumentException("bucket is required.");
        }
        Map<String, String> headers = new HashMap<String, String>();
        headers.put(HttpConstants.HEADER_AMZ_CONTENT_HASH, AWS4SignerBase.EMPTY_BODY_SHA256);

        URL url = new URL(buildUrl(bucket));

        AWS4SignerForAuthorizationHeader signer = new AWS4SignerForAuthorizationHeader(
                url, HttpConstants.HTTP_METHOD_HEAD, HttpConstants.CONSTANT_AWS_SERVICENAME_S3, region);
        String authorization = signer.computeSignature(headers,
                null, // no query parameters
                AWS4SignerBase.EMPTY_BODY_SHA256,
                accessKey,
                secretKey);

        headers.put(HttpConstants.HEADER_AUTHORIZATION, authorization);

        Request.Builder requestBuilder = new Request.Builder()
                .url(url)
                .head();
        for (Map.Entry<String, String> header : headers.entrySet()) {
            requestBuilder.addHeader(header.getKey(), header.getValue());
        }
        Call call = okHttpClient.newCall(requestBuilder.build());
        call.timeout().timeout(timeout, TimeUnit.SECONDS);
        try (Response response = call.execute()) {
            if (response.code() == 200) {
                return true;
            } else if (response.code() == 404) {
                return false;
            } else {
                throw new CandyS3Exception(CommonErrorCode.SERVER_ERROR.getCode(), parseErrorMessage(response));
            }
        }
    }

    /**
     * Returns the Region the bucket resides in.
     * @param bucket the bucket name.
     * @return the Region the bucket resides in.
     * @throws IOException if an error occurs when send request.
     * @see <a href="https://docs.aws.amazon.com/AmazonS3/latest/API/API_GetBucketLocation.html">GetBucketLocation</a>
     */
    public String getBucketLocation(String bucket) throws IOException {
        if (StringUtils.isEmpty(bucket)) {
            throw new IllegalArgumentException("bucket is required.");
        }

        Map<String, String> params = new HashMap<>();
        params.put("location", "");

        Map<String, String> headers = new HashMap<String, String>();
        headers.put(HttpConstants.HEADER_AMZ_CONTENT_HASH, AWS4SignerBase.EMPTY_BODY_SHA256);

        URL url = new URL(buildUrl(bucket) + "/" + buildParams(params));

        AWS4SignerForAuthorizationHeader signer = new AWS4SignerForAuthorizationHeader(
                url, HttpConstants.HTTP_METHOD_GET, HttpConstants.CONSTANT_AWS_SERVICENAME_S3, region);
        String authorization = signer.computeSignature(headers,
                params,
                AWS4SignerBase.EMPTY_BODY_SHA256,
                accessKey,
                secretKey);

        headers.put(HttpConstants.HEADER_AUTHORIZATION, authorization);

        Request.Builder requestBuilder = new Request.Builder()
                .url(url)
                .get();
        for (Map.Entry<String, String> header : headers.entrySet()) {
            requestBuilder.addHeader(header.getKey(), header.getValue());
        }
        Call call = okHttpClient.newCall(requestBuilder.build());
        call.timeout().timeout(timeout, TimeUnit.SECONDS);
        try (Response response = call.execute()) {
            if (!response.isSuccessful()) {
                throw new CandyS3Exception(CommonErrorCode.SERVER_ERROR.getCode(), parseErrorMessage(response));
            }
            JsonNode root = xmlMapper.readTree(response.body().string());
            return root.asText("");
        }
    }

    /**
     * Creates a new S3 bucket.
     * @param options the options to create bucket.
     * @throws IOException if an error occurs when send request.
     * @see <a href="https://docs.aws.amazon.com/AmazonS3/latest/API/API_CreateBucket.html">CreateBucket</a>
     */
    public void createBucket(CreateBucketOptions options) throws IOException {
        if (StringUtils.isEmpty(options.getBucket())) {
            throw new IllegalArgumentException("bucket is required.");
        }

        Map<String, String> headers = new HashMap<String, String>();
        if (options.isEnableObjectLock()) {
            headers.put(HttpConstants.HEADER_AMZ_BUCKET_OBJECT_LOCK_ENABLED, options.isEnableObjectLock() + "");
        }

        CreateBucketConfiguration createBucketConfiguration = new CreateBucketConfiguration();
        createBucketConfiguration.locationConstraint = options.getLocationConstraint();

        String body = "";
        if (StringUtils.isNotEmpty(createBucketConfiguration.locationConstraint)) {
            body = xmlMapper.writeValueAsString(createBucketConfiguration);
        }

        String bodyHash = StringUtils.isEmpty(body) ? AWS4SignerBase.EMPTY_BODY_SHA256 : BinaryUtils.toHex(AWS4SignerBase.hash(body));
        headers.put(HttpConstants.HEADER_AMZ_CONTENT_HASH, bodyHash);

        URL url = new URL(buildUrl(options.getBucket()));

        AWS4SignerForAuthorizationHeader signer = new AWS4SignerForAuthorizationHeader(
                url, HttpConstants.HTTP_METHOD_PUT, HttpConstants.CONSTANT_AWS_SERVICENAME_S3, region);
        String authorization = signer.computeSignature(headers,
                null, // no query parameters
                bodyHash,
                accessKey,
                secretKey);

        headers.put(HttpConstants.HEADER_AUTHORIZATION, authorization);

        Request.Builder requestBuilder = new Request.Builder()
                .url(url)
                .put(RequestBody.create(body, MediaType.get("text/plain")));
        for (Map.Entry<String, String> header : headers.entrySet()) {
            requestBuilder.addHeader(header.getKey(), header.getValue());
        }
        Call call = okHttpClient.newCall(requestBuilder.build());
        call.timeout().timeout(timeout, TimeUnit.SECONDS);
        try (Response response = call.execute()) {
            if (!response.isSuccessful()) {
                S3ServerError serverError = parseErrorMessage(response);
                if (response.code() == 409 && ("BucketAlreadyExists".equals(serverError.getCode()) || "BucketAlreadyOwnedByYou".equals(serverError.getCode()))) {
                    throw new CandyS3Exception(CommonErrorCode.BUCKET_ALREADY_EXISTS.getCode(), serverError);
                } else {
                    throw new CandyS3Exception(CommonErrorCode.SERVER_ERROR.getCode(), serverError);
                }
            }
        }
    }

    /**
     * Deletes the S3 bucket. All objects (including all object versions and delete markers) in the bucket must be deleted before the bucket itself can be deleted.
     * @param bucket the bucket name.
     * @throws IOException if an error occurs when send request.
     * @see <a href="https://docs.aws.amazon.com/AmazonS3/latest/API/API_DeleteBucket.html">DeleteBucket</a>
     */
    public void deleteBucket(String bucket) throws IOException {
        if (StringUtils.isEmpty(bucket)) {
            throw new IllegalArgumentException("bucket is required.");
        }

        Map<String, String> headers = new HashMap<String, String>();
        headers.put(HttpConstants.HEADER_AMZ_CONTENT_HASH, AWS4SignerBase.EMPTY_BODY_SHA256);

        URL url = new URL(buildUrl(bucket));

        AWS4SignerForAuthorizationHeader signer = new AWS4SignerForAuthorizationHeader(
                url, HttpConstants.HTTP_METHOD_DELETE, HttpConstants.CONSTANT_AWS_SERVICENAME_S3, region);
        String authorization = signer.computeSignature(headers,
                null, // no query parameters
                AWS4SignerBase.EMPTY_BODY_SHA256,
                accessKey,
                secretKey);

        headers.put(HttpConstants.HEADER_AUTHORIZATION, authorization);

        Request.Builder requestBuilder = new Request.Builder()
                .url(url)
                .delete();
        for (Map.Entry<String, String> header : headers.entrySet()) {
            requestBuilder.addHeader(header.getKey(), header.getValue());
        }
        Call call = okHttpClient.newCall(requestBuilder.build());
        call.timeout().timeout(timeout, TimeUnit.SECONDS);
        try (Response response = call.execute()) {
            if (!response.isSuccessful()) {
                throw new CandyS3Exception(CommonErrorCode.SERVER_ERROR.getCode(), parseErrorMessage(response));
            }
        }
    }

    /**
     * Sets the versioning state of an existing bucket.
     * @param bucket the bucket name.
     * @param versioning the versioning state.
     * @throws IOException if an error occurs when send request.
     * @see <a href="https://docs.aws.amazon.com/AmazonS3/latest/API/API_PutBucketVersioning.html">PutBucketVersioning</a>
     */
    public void setBucketVersioning(String bucket, boolean versioning) throws IOException {
        if (StringUtils.isEmpty(bucket)) {
            throw new IllegalArgumentException("bucket is required.");
        }

        Map<String, String> params = new HashMap<>();
        params.put("versioning", "");

        Map<String, String> headers = new HashMap<String, String>();

        VersioningConfiguration versioningConfiguration = new VersioningConfiguration();
        versioningConfiguration.status = versioning ? "Enabled" : "Suspended";

        String body = xmlMapper.writeValueAsString(versioningConfiguration);

        String bodyHash = BinaryUtils.toHex(AWS4SignerBase.hash(body));
        headers.put(HttpConstants.HEADER_AMZ_CONTENT_HASH, bodyHash);

        URL url = new URL(buildUrl(bucket) + "/" + buildParams(params));

        AWS4SignerForAuthorizationHeader signer = new AWS4SignerForAuthorizationHeader(
                url, HttpConstants.HTTP_METHOD_PUT, HttpConstants.CONSTANT_AWS_SERVICENAME_S3, region);
        String authorization = signer.computeSignature(headers,
                params,
                bodyHash,
                accessKey,
                secretKey);

        headers.put(HttpConstants.HEADER_AUTHORIZATION, authorization);

        Request.Builder requestBuilder = new Request.Builder()
                .url(url)
                .put(RequestBody.create(body, MediaType.get("text/plain")));
        for (Map.Entry<String, String> header : headers.entrySet()) {
            requestBuilder.addHeader(header.getKey(), header.getValue());
        }
        Call call = okHttpClient.newCall(requestBuilder.build());
        call.timeout().timeout(timeout, TimeUnit.SECONDS);
        try (Response response = call.execute()) {
            if (!response.isSuccessful()) {
                throw new CandyS3Exception(CommonErrorCode.SERVER_ERROR.getCode(), parseErrorMessage(response));
            }
        }
    }

    /**
     * Returns the versioning state of a bucket.
     * @param bucket the bucket name.
     * @return true if the bucket is versioning enabled, false otherwise.
     * @throws IOException if an error occurs when send request.
     * @see <a href="https://docs.aws.amazon.com/AmazonS3/latest/API/API_GetBucketVersioning.html">GetBucketVersioning</a>
     */
    public boolean isBucketVersioning(String bucket) throws IOException {
        if (StringUtils.isEmpty(bucket)) {
            throw new IllegalArgumentException("bucket is required.");
        }

        Map<String, String> params = new HashMap<>();
        params.put("versioning", "");

        Map<String, String> headers = new HashMap<String, String>();
        headers.put(HttpConstants.HEADER_AMZ_CONTENT_HASH, AWS4SignerBase.EMPTY_BODY_SHA256);

        URL url = new URL(buildUrl(bucket) + "/" + buildParams(params));

        AWS4SignerForAuthorizationHeader signer = new AWS4SignerForAuthorizationHeader(
                url, HttpConstants.HTTP_METHOD_GET, HttpConstants.CONSTANT_AWS_SERVICENAME_S3, region);
        String authorization = signer.computeSignature(headers,
                params,
                AWS4SignerBase.EMPTY_BODY_SHA256,
                accessKey,
                secretKey);

        headers.put(HttpConstants.HEADER_AUTHORIZATION, authorization);

        Request.Builder requestBuilder = new Request.Builder()
                .url(url)
                .get();
        for (Map.Entry<String, String> header : headers.entrySet()) {
            requestBuilder.addHeader(header.getKey(), header.getValue());
        }
        Call call = okHttpClient.newCall(requestBuilder.build());
        call.timeout().timeout(timeout, TimeUnit.SECONDS);
        try (Response response = call.execute()) {
            if (!response.isSuccessful()) {
                throw new CandyS3Exception(CommonErrorCode.SERVER_ERROR.getCode(), parseErrorMessage(response));
            }
            JsonNode root = xmlMapper.readTree(response.body().string());
            return "Enabled".equals(root.path("Status").asText(""));
        }
    }

    /**
     * Gets the Object Lock configuration for a bucket, or throw exception if object-lock is not enabled.
     * @param bucket the bucket name.
     * @return the Object Lock configuration for the bucket.
     * @throws IOException if an error occurs when send request.
     * @see <a href="https://docs.aws.amazon.com/AmazonS3/latest/API/API_GetObjectLockConfiguration.html">GetObjectLockConfiguration</a>
     */
    public BucketObjectLockConfiguration getBucketObjectLockConfiguration(String bucket) throws IOException {
        if (StringUtils.isEmpty(bucket)) {
            throw new IllegalArgumentException("bucket is required.");
        }

        Map<String, String> params = new HashMap<>();
        params.put("object-lock", "");

        Map<String, String> headers = new HashMap<String, String>();
        headers.put(HttpConstants.HEADER_AMZ_CONTENT_HASH, AWS4SignerBase.EMPTY_BODY_SHA256);

        URL url = new URL(buildUrl(bucket) + "/" + buildParams(params));

        AWS4SignerForAuthorizationHeader signer = new AWS4SignerForAuthorizationHeader(
                url, HttpConstants.HTTP_METHOD_GET, HttpConstants.CONSTANT_AWS_SERVICENAME_S3, region);
        String authorization = signer.computeSignature(headers,
                params,
                AWS4SignerBase.EMPTY_BODY_SHA256,
                accessKey,
                secretKey);

        headers.put(HttpConstants.HEADER_AUTHORIZATION, authorization);

        Request.Builder requestBuilder = new Request.Builder()
                .url(url)
                .get();
        for (Map.Entry<String, String> header : headers.entrySet()) {
            requestBuilder.addHeader(header.getKey(), header.getValue());
        }
        Call call = okHttpClient.newCall(requestBuilder.build());
        call.timeout().timeout(timeout, TimeUnit.SECONDS);
        try (Response response = call.execute()) {
            if (!response.isSuccessful()) {
                S3ServerError serverError = parseErrorMessage(response);
                if ("ObjectLockConfigurationNotFoundError".equals(serverError.getCode())) {
                    throw new CandyS3Exception(CommonErrorCode.BUCKET_OBJECT_LOCK_NOT_ENABLED.getCode(), serverError);
                } else {
                    throw new CandyS3Exception(CommonErrorCode.SERVER_ERROR.getCode(), serverError);
                }
            }
            JsonNode root = xmlMapper.readTree(response.body().string());

            BucketObjectLockConfiguration configuration = new BucketObjectLockConfiguration();
            JsonNode lockEnabledNode = root.path("ObjectLockEnabled");
            if (!lockEnabledNode.isMissingNode()) {
                configuration.setObjectLockEnabled("Enabled".equals(lockEnabledNode.asText("")));
            } else {
                configuration.setObjectLockEnabled(false);
            }

            JsonNode retentionNode = root.path("Rule").path("DefaultRetention");
            if (!retentionNode.isMissingNode()) {
                configuration.setMode(ObjectRetentionMode.valueOf(retentionNode.path("Mode").asText()));
                JsonNode yearsNode = retentionNode.path("Years");
                if (!yearsNode.isMissingNode()) {
                    configuration.setYears(yearsNode.asInt());
                }
                JsonNode daysNode = retentionNode.path("Days");
                if (!daysNode.isMissingNode()) {
                    configuration.setDays(daysNode.asInt());
                }
            }

            return configuration;
        }
    }

    /**
     * Places an Object Lock configuration on the specified bucket. This needs enable versioning first in AWS S3.
     * @param bucket the bucket name.
     * @param options the options to enable object lock.
     * @throws IOException if an error occurs when send request.
     * @throws NoSuchAlgorithmException if error occurs when compute signature.
     * @see <a href="https://docs.aws.amazon.com/AmazonS3/latest/API/API_PutObjectLockConfiguration.html">PutObjectLockConfiguration</a>
     */
    public void enableBucketObjectLock(String bucket, UpdateBucketObjectLockOptions options) throws IOException, NoSuchAlgorithmException {
        if (StringUtils.isEmpty(bucket)) {
            throw new IllegalArgumentException("bucket is required.");
        }
        Map<String, String> params = new HashMap<>();
        params.put("object-lock", "");

        Map<String, String> headers = new HashMap<String, String>();

        ObjectLockConfiguration lockConfiguration = new ObjectLockConfiguration();
        lockConfiguration.objectLockEnabled = "Enabled";
        if (options.getRetentionMode() != null) {
            ObjectLockConfigurationLockRule rule = new ObjectLockConfigurationLockRule();
            ObjectLockConfigurationObjectRetention retention = new ObjectLockConfigurationObjectRetention();
            retention.mode = options.getRetentionMode().name();
            retention.years = options.getYears();
            retention.days = options.getDays();
            rule.retention = retention;
            lockConfiguration.lockRule = rule;
        }


        String body = xmlMapper.writeValueAsString(lockConfiguration);
        String bodyHash = StringUtils.isEmpty(body) ? AWS4SignerBase.EMPTY_BODY_SHA256 : BinaryUtils.toHex(AWS4SignerBase.hash(body));
        headers.put(HttpConstants.HEADER_AMZ_CONTENT_HASH, bodyHash);
        headers.put(HttpConstants.HEADER_CONTENT_MD5, BinaryUtils.md5(body));

        URL url = new URL(buildUrl(bucket) + "/" + buildParams(params));

        AWS4SignerForAuthorizationHeader signer = new AWS4SignerForAuthorizationHeader(
                url, HttpConstants.HTTP_METHOD_PUT, HttpConstants.CONSTANT_AWS_SERVICENAME_S3, region);
        String authorization = signer.computeSignature(headers,
                params,
                bodyHash,
                accessKey,
                secretKey);

        headers.put(HttpConstants.HEADER_AUTHORIZATION, authorization);

        Request.Builder requestBuilder = new Request.Builder()
                .url(url)
                .put(RequestBody.create(body, MediaType.get("text/plain")));
        for (Map.Entry<String, String> header : headers.entrySet()) {
            requestBuilder.addHeader(header.getKey(), header.getValue());
        }
        Call call = okHttpClient.newCall(requestBuilder.build());
        call.timeout().timeout(timeout, TimeUnit.SECONDS);
        try (Response response = call.execute()) {
            if (!response.isSuccessful()) {
                throw new CandyS3Exception(CommonErrorCode.SERVER_ERROR.getCode(), parseErrorMessage(response));
            }
        }
    }

    /**
     * Retrieves the PublicAccessBlock configuration for an Amazon S3 bucket.
     * @param bucket the bucket name.
     * @return the PublicAccessBlock configuration for the bucket.
     * @throws IOException if an error occurs when send request.
     * @see <a href="https://docs.aws.amazon.com/AmazonS3/latest/API/API_GetPublicAccessBlock.html">GetPublicAccessBlock</a>
     */
    public BucketPublicAccessBlock getBucketPublicAccessBlock(String bucket) throws IOException {
        if (StringUtils.isEmpty(bucket)) {
            throw new IllegalArgumentException("bucket is required.");
        }

        Map<String, String> params = new HashMap<>();
        params.put("publicAccessBlock", "");

        Map<String, String> headers = new HashMap<String, String>();
        headers.put(HttpConstants.HEADER_AMZ_CONTENT_HASH, AWS4SignerBase.EMPTY_BODY_SHA256);

        URL url = new URL(buildUrl(bucket) + "/" + buildParams(params));

        AWS4SignerForAuthorizationHeader signer = new AWS4SignerForAuthorizationHeader(
                url, HttpConstants.HTTP_METHOD_GET, HttpConstants.CONSTANT_AWS_SERVICENAME_S3, region);
        String authorization = signer.computeSignature(headers,
                params,
                AWS4SignerBase.EMPTY_BODY_SHA256,
                accessKey,
                secretKey);

        headers.put(HttpConstants.HEADER_AUTHORIZATION, authorization);

        Request.Builder requestBuilder = new Request.Builder()
                .url(url)
                .get();
        for (Map.Entry<String, String> header : headers.entrySet()) {
            requestBuilder.addHeader(header.getKey(), header.getValue());
        }
        Call call = okHttpClient.newCall(requestBuilder.build());
        call.timeout().timeout(timeout, TimeUnit.SECONDS);
        try (Response response = call.execute()) {
            if (!response.isSuccessful()) {
                S3ServerError serverError = parseErrorMessage(response);
                if ("NoSuchPublicAccessBlockConfiguration".equals(serverError.getCode())) {
                    throw new CandyS3Exception(CommonErrorCode.BUCKET_NO_PUBLIC_ACCESS_BLOCK.getCode(), serverError);
                } else {
                    throw new CandyS3Exception(CommonErrorCode.SERVER_ERROR.getCode(), serverError);
                }
            }

            JsonNode root = xmlMapper.readTree(response.body().string());
            return xmlMapper.treeToValue(root, BucketPublicAccessBlock.class);
        }
    }

    /**
     * Creates, modifies or removes the PublicAccessBlock configuration for an Amazon S3 bucket.
     * @param bucket the bucket name.
     * @param options the options to update public access block.
     * @throws IOException if an error occurs when send request.
     * @see <a href="https://docs.aws.amazon.com/AmazonS3/latest/API/API_PutPublicAccessBlock.html">PutPublicAccessBlock</a>
     * @see <a href="https://docs.aws.amazon.com/AmazonS3/latest/API/API_DeletePublicAccessBlock.html">DeletePublicAccessBlock</a>
     */
    public void updateBucketPublicAccessBlock(String bucket, UpdateBucketPublicAccessBlockOptions options) throws IOException {
        if (StringUtils.isEmpty(bucket)) {
            throw new IllegalArgumentException("bucket is required.");
        }

        if (options.isRemoveBlocks()) {
            this.deleteBucketPublicAccessBlock(bucket);
        } else {
            this.putBucketPublicAccessBlock(bucket,
                    options.isBlockPublicAcls(), options.isBlockPublicPolicy(), options.isIgnorePublicAcls(), options.isRestrictPublicBuckets());
        }

    }

    private void putBucketPublicAccessBlock(String bucket,
                                            boolean blockPublicAcls, boolean blockPublicPolicy, boolean ignorePublicAcls, boolean restrictPublicBuckets) throws IOException {


        Map<String, String> params = new HashMap<>();
        params.put("publicAccessBlock", "");

        Map<String, String> headers = new HashMap<String, String>();

        PublicAccessBlockConfiguration blockConfiguration = new PublicAccessBlockConfiguration();
        blockConfiguration.blockPublicAcls = blockPublicAcls;
        blockConfiguration.blockPublicPolicy = blockPublicPolicy;
        blockConfiguration.ignorePublicAcls = ignorePublicAcls;
        blockConfiguration.restrictPublicBuckets = restrictPublicBuckets;

        String body = xmlMapper.writeValueAsString(blockConfiguration);
        String bodyHash = BinaryUtils.toHex(AWS4SignerBase.hash(body));
        headers.put(HttpConstants.HEADER_AMZ_CONTENT_HASH, bodyHash);

        URL url = new URL(buildUrl(bucket) + "/" + buildParams(params));

        AWS4SignerForAuthorizationHeader signer = new AWS4SignerForAuthorizationHeader(
                url, HttpConstants.HTTP_METHOD_PUT, HttpConstants.CONSTANT_AWS_SERVICENAME_S3, region);
        String authorization = signer.computeSignature(headers,
                params,
                bodyHash,
                accessKey,
                secretKey);

        headers.put(HttpConstants.HEADER_AUTHORIZATION, authorization);

        Request.Builder requestBuilder = new Request.Builder()
                .url(url)
                .put(RequestBody.create(body, MediaType.get("text/plain")));
        for (Map.Entry<String, String> header : headers.entrySet()) {
            requestBuilder.addHeader(header.getKey(), header.getValue());
        }
        Call call = okHttpClient.newCall(requestBuilder.build());
        call.timeout().timeout(timeout, TimeUnit.SECONDS);
        try (Response response = call.execute()) {
            if (!response.isSuccessful()) {
                throw new CandyS3Exception(CommonErrorCode.SERVER_ERROR.getCode(), parseErrorMessage(response));
            }
        }
    }

    private void deleteBucketPublicAccessBlock(String bucket) throws IOException {
        Map<String, String> params = new HashMap<>();
        params.put("publicAccessBlock", "");

        Map<String, String> headers = new HashMap<String, String>();
        headers.put(HttpConstants.HEADER_AMZ_CONTENT_HASH, AWS4SignerBase.EMPTY_BODY_SHA256);

        URL url = new URL(buildUrl(bucket) + "/" + buildParams(params));

        AWS4SignerForAuthorizationHeader signer = new AWS4SignerForAuthorizationHeader(
                url, HttpConstants.HTTP_METHOD_DELETE, HttpConstants.CONSTANT_AWS_SERVICENAME_S3, region);
        String authorization = signer.computeSignature(headers,
                params,
                AWS4SignerBase.EMPTY_BODY_SHA256,
                accessKey,
                secretKey);

        headers.put(HttpConstants.HEADER_AUTHORIZATION, authorization);

        Request.Builder requestBuilder = new Request.Builder()
                .url(url)
                .delete();
        for (Map.Entry<String, String> header : headers.entrySet()) {
            requestBuilder.addHeader(header.getKey(), header.getValue());
        }
        Call call = okHttpClient.newCall(requestBuilder.build());
        call.timeout().timeout(timeout, TimeUnit.SECONDS);
        try (Response response = call.execute()) {
            if (!response.isSuccessful()) {
                throw new CandyS3Exception(CommonErrorCode.SERVER_ERROR.getCode(), parseErrorMessage(response));
            }
        }
    }


    /**
     * Returns the policy of a specified bucket, or throw exception if no policy exists.
     * @param bucket the bucket name.
     * @return the policy of the bucket.
     * @throws IOException if an error occurs when send request.
     * @see <a href="https://docs.aws.amazon.com/AmazonS3/latest/API/API_GetBucketPolicy.html">GetBucketPolicy</a>
     */
    public String getBucketPolicy(String bucket) throws IOException {
        if (StringUtils.isEmpty(bucket)) {
            throw new IllegalArgumentException("bucket is required.");
        }

        Map<String, String> params = new HashMap<>();
        params.put("policy", "");

        Map<String, String> headers = new HashMap<String, String>();
        headers.put(HttpConstants.HEADER_AMZ_CONTENT_HASH, AWS4SignerBase.EMPTY_BODY_SHA256);

        URL url = new URL(buildUrl(bucket) + "/" + buildParams(params));

        AWS4SignerForAuthorizationHeader signer = new AWS4SignerForAuthorizationHeader(
                url, HttpConstants.HTTP_METHOD_GET, HttpConstants.CONSTANT_AWS_SERVICENAME_S3, region);
        String authorization = signer.computeSignature(headers,
                params,
                AWS4SignerBase.EMPTY_BODY_SHA256,
                accessKey,
                secretKey);

        headers.put(HttpConstants.HEADER_AUTHORIZATION, authorization);

        Request.Builder requestBuilder = new Request.Builder()
                .url(url)
                .get();
        for (Map.Entry<String, String> header : headers.entrySet()) {
            requestBuilder.addHeader(header.getKey(), header.getValue());
        }
        Call call = okHttpClient.newCall(requestBuilder.build());
        call.timeout().timeout(timeout, TimeUnit.SECONDS);
        try (Response response = call.execute()) {
            if (!response.isSuccessful()) {
                S3ServerError serverError = parseErrorMessage(response);
                if (response.code() == 404) {
                    throw new CandyS3Exception(CommonErrorCode.BUCKET_NO_POLICY.getCode(), serverError);
                } else {
                    throw new CandyS3Exception(CommonErrorCode.SERVER_ERROR.getCode(), serverError);
                }
            }

            return response.body().string();
        }
    }

    /**
     * Applies an Amazon S3 bucket policy to an Amazon S3 bucket, or deletes the policy of the specified bucket.
     * @param bucket the bucket name.
     * @param options the options to update bucket policy.
     * @throws IOException if an error occurs when send request.
     * @throws NoSuchAlgorithmException if error occurs when compute signature.
     * @see <a href="https://docs.aws.amazon.com/AmazonS3/latest/API/API_PutBucketPolicy.html">PutBucketPolicy</a>
     * @see <a href="https://docs.aws.amazon.com/AmazonS3/latest/API/API_DeleteBucketPolicy.html">DeleteBucketPolicy</a>
     */
    public void updateBucketPolicy(String bucket, UpdateBucketPolicyOptions options) throws IOException, NoSuchAlgorithmException {
        if (StringUtils.isEmpty(bucket)) {
            throw new IllegalArgumentException("bucket is required.");
        }

        if (StringUtils.isNotEmpty(options.getPolicyJson())) {
            this.putBucketPolicy(bucket, options.getPolicyJson());
        } else {
            this.deleteBucketPolicy(bucket);
        }
    }

    private void putBucketPolicy(String bucket, String policy) throws NoSuchAlgorithmException, IOException {
        Map<String, String> params = new HashMap<>();
        params.put("policy", "");

        Map<String, String> headers = new HashMap<String, String>();

        String body = policy;
        String bodyHash = BinaryUtils.toHex(AWS4SignerBase.hash(body));
        headers.put(HttpConstants.HEADER_AMZ_CONTENT_HASH, bodyHash);
        headers.put(HttpConstants.HEADER_CONTENT_MD5, BinaryUtils.md5(body));

        URL url = new URL(buildUrl(bucket) + "/" + buildParams(params));

        AWS4SignerForAuthorizationHeader signer = new AWS4SignerForAuthorizationHeader(
                url, HttpConstants.HTTP_METHOD_PUT, HttpConstants.CONSTANT_AWS_SERVICENAME_S3, region);
        String authorization = signer.computeSignature(headers,
                params,
                bodyHash,
                accessKey,
                secretKey);

        headers.put(HttpConstants.HEADER_AUTHORIZATION, authorization);

        Request.Builder requestBuilder = new Request.Builder()
                .url(url)
                .put(RequestBody.create(body, MediaType.get("text/plain")));
        for (Map.Entry<String, String> header : headers.entrySet()) {
            requestBuilder.addHeader(header.getKey(), header.getValue());
        }
        Call call = okHttpClient.newCall(requestBuilder.build());
        call.timeout().timeout(timeout, TimeUnit.SECONDS);
        try (Response response = call.execute()) {
            if (!response.isSuccessful()) {
                throw new CandyS3Exception(CommonErrorCode.SERVER_ERROR.getCode(), parseErrorMessage(response));
            }
        }
    }

    private void deleteBucketPolicy(String bucket) throws IOException {
        Map<String, String> params = new HashMap<>();
        params.put("policy", "");

        Map<String, String> headers = new HashMap<String, String>();
        headers.put(HttpConstants.HEADER_AMZ_CONTENT_HASH, AWS4SignerBase.EMPTY_BODY_SHA256);

        URL url = new URL(buildUrl(bucket) + "/" + buildParams(params));

        AWS4SignerForAuthorizationHeader signer = new AWS4SignerForAuthorizationHeader(
                url, HttpConstants.HTTP_METHOD_DELETE, HttpConstants.CONSTANT_AWS_SERVICENAME_S3, region);
        String authorization = signer.computeSignature(headers,
                params,
                AWS4SignerBase.EMPTY_BODY_SHA256,
                accessKey,
                secretKey);

        headers.put(HttpConstants.HEADER_AUTHORIZATION, authorization);

        Request.Builder requestBuilder = new Request.Builder()
                .url(url)
                .delete();
        for (Map.Entry<String, String> header : headers.entrySet()) {
            requestBuilder.addHeader(header.getKey(), header.getValue());
        }
        Call call = okHttpClient.newCall(requestBuilder.build());
        call.timeout().timeout(timeout, TimeUnit.SECONDS);
        try (Response response = call.execute()) {
            if (!response.isSuccessful()) {
                throw new CandyS3Exception(CommonErrorCode.SERVER_ERROR.getCode(), parseErrorMessage(response));
            }
        }
    }

    /**
     * Retrieves the policy status for an Amazon S3 bucket, indicating whether the bucket is public. Or throw exception if no policy exists.
     * @param bucket the bucket name.
     * @return the policy status of the bucket.
     * @throws IOException if an error occurs when send request.
     * @see <a href="https://docs.aws.amazon.com/AmazonS3/latest/API/API_GetBucketPolicyStatus.html">GetBucketPolicyStatus</a>
     */
    public BucketPolicyStatus getBucketPolicyStatus(String bucket) throws IOException {
        if (StringUtils.isEmpty(bucket)) {
            throw new IllegalArgumentException("bucket is required.");
        }

        Map<String, String> params = new HashMap<>();
        params.put("policyStatus", "");

        Map<String, String> headers = new HashMap<String, String>();
        headers.put(HttpConstants.HEADER_AMZ_CONTENT_HASH, AWS4SignerBase.EMPTY_BODY_SHA256);

        URL url = new URL(buildUrl(bucket) + "/" + buildParams(params));

        AWS4SignerForAuthorizationHeader signer = new AWS4SignerForAuthorizationHeader(
                url, HttpConstants.HTTP_METHOD_GET, HttpConstants.CONSTANT_AWS_SERVICENAME_S3, region);
        String authorization = signer.computeSignature(headers,
                params,
                AWS4SignerBase.EMPTY_BODY_SHA256,
                accessKey,
                secretKey);

        headers.put(HttpConstants.HEADER_AUTHORIZATION, authorization);

        Request.Builder requestBuilder = new Request.Builder()
                .url(url)
                .get();
        for (Map.Entry<String, String> header : headers.entrySet()) {
            requestBuilder.addHeader(header.getKey(), header.getValue());
        }
        Call call = okHttpClient.newCall(requestBuilder.build());
        call.timeout().timeout(timeout, TimeUnit.SECONDS);
        try (Response response = call.execute()) {
            if (!response.isSuccessful()) {
                S3ServerError serverError = parseErrorMessage(response);
                if ("NoSuchBucketPolicy".equals(serverError.getCode())) {
                    throw new CandyS3Exception(CommonErrorCode.BUCKET_NO_POLICY.getCode(), serverError);
                } else {
                    throw new CandyS3Exception(CommonErrorCode.SERVER_ERROR.getCode(), serverError);
                }
            }

            JsonNode root = xmlMapper.readTree(response.body().string());
            BucketPolicyStatus policyStatus = new BucketPolicyStatus();
            policyStatus.setPublic(root.path("IsPublic").asBoolean());
            return policyStatus;
        }
    }

    /**
     * Returns the tag set associated with the general purpose bucket, or throw exception if no tag set associated with the bucket.
     * @param bucket the bucket name.
     * @return the tag set associated with the bucket.
     * @throws IOException if an error occurs when send request.
     * @see <a href="https://docs.aws.amazon.com/AmazonS3/latest/API/API_GetBucketTagging.html">GetBucketTagging</a>
     */
    public Map<String, String> getBucketTag(String bucket) throws IOException {
        if (StringUtils.isEmpty(bucket)) {
            throw new IllegalArgumentException("bucket is required.");
        }

        Map<String, String> params = new HashMap<>();
        params.put("tagging", "");

        Map<String, String> headers = new HashMap<String, String>();
        headers.put(HttpConstants.HEADER_AMZ_CONTENT_HASH, AWS4SignerBase.EMPTY_BODY_SHA256);

        URL url = new URL(buildUrl(bucket) + "/" + buildParams(params));

        AWS4SignerForAuthorizationHeader signer = new AWS4SignerForAuthorizationHeader(
                url, HttpConstants.HTTP_METHOD_GET, HttpConstants.CONSTANT_AWS_SERVICENAME_S3, region);
        String authorization = signer.computeSignature(headers,
                params,
                AWS4SignerBase.EMPTY_BODY_SHA256,
                accessKey,
                secretKey);

        headers.put(HttpConstants.HEADER_AUTHORIZATION, authorization);

        Request.Builder requestBuilder = new Request.Builder()
                .url(url)
                .get();
        for (Map.Entry<String, String> header : headers.entrySet()) {
            requestBuilder.addHeader(header.getKey(), header.getValue());
        }
        Call call = okHttpClient.newCall(requestBuilder.build());
        call.timeout().timeout(timeout, TimeUnit.SECONDS);
        try (Response response = call.execute()) {
            if (!response.isSuccessful()) {
                S3ServerError serverError = parseErrorMessage(response);
                if (response.code() == 404 && "NoSuchTagSet".equals(serverError.getCode())) {
                    throw new CandyS3Exception(CommonErrorCode.BUCKET_NO_ASSOCIATED_TAG.getCode(), serverError);
                } else {
                    throw new CandyS3Exception(CommonErrorCode.SERVER_ERROR.getCode(), serverError);
                }
            }
            Map<String, String> tags = new HashMap<>();

            JsonNode root = xmlMapper.readTree(response.body().string());
            JsonNode tagNode = root.path("TagSet").path("Tag");
            if (!tagNode.isMissingNode()) {
                if (tagNode.isArray()) {
                    for (JsonNode kvNode : tagNode) {
                        tags.put(kvNode.path("Key").asText(), kvNode.path("Value").asText());
                    }
                } else {
                    tags.put(tagNode.path("Key").asText(), tagNode.path("Value").asText());
                }
            }

            return tags;
        }
    }

    /**
     * Sets or deletes the tags for a general purpose bucket.
     * @param bucket the bucket name.
     * @param options the options to update bucket tag.
     * @throws IOException if an error occurs when send request.
     * @throws NoSuchAlgorithmException if error occurs when compute signature.
     * @see <a href="https://docs.aws.amazon.com/AmazonS3/latest/API/API_PutBucketTagging.html">PutBucketTagging</a>
     * @see <a href="https://docs.aws.amazon.com/AmazonS3/latest/API/API_DeleteBucketTagging.html">DeleteBucketTagging</a>
     */
    public void updateBucketTag(String bucket, UpdateBucketTagOptions options) throws NoSuchAlgorithmException, IOException {
        if (StringUtils.isEmpty(bucket)) {
            throw new IllegalArgumentException("bucket is required.");
        }

        if (!options.isRemoveTags()) {
            this.putBucketTag(bucket, options.getTags());
        } else {
            this.deleteBucketTag(bucket);
        }
    }

    private void putBucketTag(String bucket, Map<String, String> tags) throws NoSuchAlgorithmException, IOException {
        Map<String, String> params = new HashMap<>();
        params.put("tagging", "");

        Map<String, String> headers = new HashMap<String, String>();

        Tagging tagging = new Tagging();
        tagging.tagSet = new TagSet();
        tagging.tagSet.tag = new ArrayList<>(tags.size());
        for (Map.Entry<String, String> kv : tags.entrySet()) {
            tagging.tagSet.tag.add(new Tag(kv.getKey(), kv.getValue()));
        }

        String body = xmlMapper.writeValueAsString(tagging);
        String bodyHash = BinaryUtils.toHex(AWS4SignerBase.hash(body));
        headers.put(HttpConstants.HEADER_AMZ_CONTENT_HASH, bodyHash);
        headers.put(HttpConstants.HEADER_CONTENT_MD5, BinaryUtils.md5(body));

        URL url = new URL(buildUrl(bucket) + "/" + buildParams(params));

        AWS4SignerForAuthorizationHeader signer = new AWS4SignerForAuthorizationHeader(
                url, HttpConstants.HTTP_METHOD_PUT, HttpConstants.CONSTANT_AWS_SERVICENAME_S3, region);
        String authorization = signer.computeSignature(headers,
                params,
                bodyHash,
                accessKey,
                secretKey);

        headers.put(HttpConstants.HEADER_AUTHORIZATION, authorization);

        Request.Builder requestBuilder = new Request.Builder()
                .url(url)
                .put(RequestBody.create(body, MediaType.get("text/plain")));
        for (Map.Entry<String, String> header : headers.entrySet()) {
            requestBuilder.addHeader(header.getKey(), header.getValue());
        }
        Call call = okHttpClient.newCall(requestBuilder.build());
        call.timeout().timeout(timeout, TimeUnit.SECONDS);
        try (Response response = call.execute()) {
            if (!response.isSuccessful()) {
                throw new CandyS3Exception(CommonErrorCode.SERVER_ERROR.getCode(), parseErrorMessage(response));
            }
        }
    }

    private void deleteBucketTag(String bucket) throws IOException {
        Map<String, String> params = new HashMap<>();
        params.put("tagging", "");

        Map<String, String> headers = new HashMap<String, String>();
        headers.put(HttpConstants.HEADER_AMZ_CONTENT_HASH, AWS4SignerBase.EMPTY_BODY_SHA256);

        URL url = new URL(buildUrl(bucket) + "/" + buildParams(params));

        AWS4SignerForAuthorizationHeader signer = new AWS4SignerForAuthorizationHeader(
                url, HttpConstants.HTTP_METHOD_DELETE, HttpConstants.CONSTANT_AWS_SERVICENAME_S3, region);
        String authorization = signer.computeSignature(headers,
                params,
                AWS4SignerBase.EMPTY_BODY_SHA256,
                accessKey,
                secretKey);

        headers.put(HttpConstants.HEADER_AUTHORIZATION, authorization);

        Request.Builder requestBuilder = new Request.Builder()
                .url(url)
                .delete();
        for (Map.Entry<String, String> header : headers.entrySet()) {
            requestBuilder.addHeader(header.getKey(), header.getValue());
        }
        Call call = okHttpClient.newCall(requestBuilder.build());
        call.timeout().timeout(timeout, TimeUnit.SECONDS);
        try (Response response = call.execute()) {
            if (!response.isSuccessful()) {
                throw new CandyS3Exception(CommonErrorCode.SERVER_ERROR.getCode(), parseErrorMessage(response));
            }
        }
    }

    /**
     * Returns the default encryption configuration for an Amazon S3 bucket.
     * @param bucket the bucket name.
     * @return the default encryption configuration for the bucket.
     * @throws IOException if an error occurs when send request.
     * @see <a href="https://docs.aws.amazon.com/AmazonS3/latest/API/API_GetBucketEncryption.html">GetBucketEncryption</a>
     */
    public List<ServerSideEncryptionProperties> getBucketServerSideEncryption(String bucket) throws IOException {
        if (StringUtils.isEmpty(bucket)) {
            throw new IllegalArgumentException("bucket is required.");
        }

        Map<String, String> params = new HashMap<>();
        params.put("encryption", "");

        Map<String, String> headers = new HashMap<String, String>();
        headers.put(HttpConstants.HEADER_AMZ_CONTENT_HASH, AWS4SignerBase.EMPTY_BODY_SHA256);

        URL url = new URL(buildUrl(bucket) + "/" + buildParams(params));

        AWS4SignerForAuthorizationHeader signer = new AWS4SignerForAuthorizationHeader(
                url, HttpConstants.HTTP_METHOD_GET, HttpConstants.CONSTANT_AWS_SERVICENAME_S3, region);
        String authorization = signer.computeSignature(headers,
                params,
                AWS4SignerBase.EMPTY_BODY_SHA256,
                accessKey,
                secretKey);

        headers.put(HttpConstants.HEADER_AUTHORIZATION, authorization);

        Request.Builder requestBuilder = new Request.Builder()
                .url(url)
                .get();
        for (Map.Entry<String, String> header : headers.entrySet()) {
            requestBuilder.addHeader(header.getKey(), header.getValue());
        }
        Call call = okHttpClient.newCall(requestBuilder.build());
        call.timeout().timeout(timeout, TimeUnit.SECONDS);
        try (Response response = call.execute()) {
            if (!response.isSuccessful()) {
                S3ServerError serverError = parseErrorMessage(response);
                throw new CandyS3Exception(CommonErrorCode.SERVER_ERROR.getCode(), serverError);
            }

            List<ServerSideEncryptionProperties> configurations = new ArrayList<>();
            JsonNode root = xmlMapper.readTree(response.body().string());
            JsonNode ruleNode = root.path("Rule");
            if (!ruleNode.isMissingNode()) {
                if (ruleNode.isArray()) {
                    for (JsonNode rule : ruleNode) {
                        ServerSideEncryptionProperties conf = new ServerSideEncryptionProperties();
                        JsonNode node = rule.path("ApplyServerSideEncryptionByDefault");
                        conf.setSseAlgorithm(node.path("ApplyServerSideEncryptionByDefault").asText());
                        configurations.add(conf);
                    }
                } else {
                    ServerSideEncryptionProperties conf = new ServerSideEncryptionProperties();
                    JsonNode node = ruleNode.path("ApplyServerSideEncryptionByDefault");
                    conf.setSseAlgorithm(node.path("SSEAlgorithm").asText());
                    configurations.add(conf);
                }
            }

            return configurations;
        }
    }

    /**
     * Configure or reset the default encryption and Amazon S3 Bucket Keys for an existing bucket.
     * @param bucket the bucket name.
     * @param options the options to update bucket server side encryption.
     * @throws IOException if an error occurs when send request.
     * @throws NoSuchAlgorithmException if error occurs when compute signature.
     * @see <a href="https://docs.aws.amazon.com/AmazonS3/latest/API/API_PutBucketEncryption.html">PutBucketEncryption</a>
     * @see <a href="https://docs.aws.amazon.com/AmazonS3/latest/API/API_DeleteBucketEncryption.html">DeleteBucketEncryption</a>
     */
    public void updateBucketServerSideEncryption(String bucket, UpdateServerSideEncryptionOptions options) throws NoSuchAlgorithmException, IOException {
        if (StringUtils.isEmpty(bucket)) {
            throw new IllegalArgumentException("bucket is required.");
        }

        Map<String, String> params = new HashMap<>();
        params.put("encryption", "");

        Map<String, String> headers = new HashMap<String, String>();

        ServerSideEncryptionConfiguration conf = new ServerSideEncryptionConfiguration();
        conf.rule = new ServerSideEncryptionRule();
        conf.rule.applyConfig = new ServerSideEncryptionRuleApplyConfig();
        conf.rule.applyConfig.sseAlgorithm = options.getSseAlgorithm();

        String body = xmlMapper.writeValueAsString(conf);
        String bodyHash = BinaryUtils.toHex(AWS4SignerBase.hash(body));
        headers.put(HttpConstants.HEADER_AMZ_CONTENT_HASH, bodyHash);
        headers.put(HttpConstants.HEADER_CONTENT_MD5, BinaryUtils.md5(body));

        URL url = new URL(buildUrl(bucket) + "/" + buildParams(params));

        AWS4SignerForAuthorizationHeader signer = new AWS4SignerForAuthorizationHeader(
                url, HttpConstants.HTTP_METHOD_PUT, HttpConstants.CONSTANT_AWS_SERVICENAME_S3, region);
        String authorization = signer.computeSignature(headers,
                params,
                bodyHash,
                accessKey,
                secretKey);

        headers.put(HttpConstants.HEADER_AUTHORIZATION, authorization);

        Request.Builder requestBuilder = new Request.Builder()
                .url(url)
                .put(RequestBody.create(body, MediaType.get("text/plain")));
        for (Map.Entry<String, String> header : headers.entrySet()) {
            requestBuilder.addHeader(header.getKey(), header.getValue());
        }
        Call call = okHttpClient.newCall(requestBuilder.build());
        call.timeout().timeout(timeout, TimeUnit.SECONDS);
        try (Response response = call.execute()) {
            if (!response.isSuccessful()) {
                throw new CandyS3Exception(CommonErrorCode.SERVER_ERROR.getCode(), parseErrorMessage(response));
            }
        }
    }

    /**
     * Configure or reset the default encryption and Amazon S3 Bucket Keys for an existing bucket.
     * @param bucket the bucket name.
     * @param accelerate whether to enable accelerate.
     * @throws IOException if an error occurs when send request.
     * @see <a href="https://docs.aws.amazon.com/AmazonS3/latest/API/API_PutBucketAccelerateConfiguration.html">PutBucketAccelerateConfiguration</a>
     */
    public void setBucketAccelerate(String bucket, boolean accelerate) throws IOException {
        if (StringUtils.isEmpty(bucket)) {
            throw new IllegalArgumentException("bucket is required.");
        }

        Map<String, String> params = new HashMap<>();
        params.put("accelerate", "");

        Map<String, String> headers = new HashMap<String, String>();

        AccelerateConfiguration accelerateConfiguration = new AccelerateConfiguration();
        accelerateConfiguration.status = accelerate ? "Enabled" : "Suspended";

        String body = xmlMapper.writeValueAsString(accelerateConfiguration);

        String bodyHash = BinaryUtils.toHex(AWS4SignerBase.hash(body));
        headers.put(HttpConstants.HEADER_AMZ_CONTENT_HASH, bodyHash);

        URL url = new URL(buildUrl(bucket) + "/" + buildParams(params));

        AWS4SignerForAuthorizationHeader signer = new AWS4SignerForAuthorizationHeader(
                url, HttpConstants.HTTP_METHOD_PUT, HttpConstants.CONSTANT_AWS_SERVICENAME_S3, region);
        String authorization = signer.computeSignature(headers,
                params,
                bodyHash,
                accessKey,
                secretKey);

        headers.put(HttpConstants.HEADER_AUTHORIZATION, authorization);

        Request.Builder requestBuilder = new Request.Builder()
                .url(url)
                .put(RequestBody.create(body, MediaType.get("text/plain")));
        for (Map.Entry<String, String> header : headers.entrySet()) {
            requestBuilder.addHeader(header.getKey(), header.getValue());
        }
        Call call = okHttpClient.newCall(requestBuilder.build());
        call.timeout().timeout(timeout, TimeUnit.SECONDS);
        try (Response response = call.execute()) {
            if (!response.isSuccessful()) {
                throw new CandyS3Exception(CommonErrorCode.SERVER_ERROR.getCode(), parseErrorMessage(response));
            }
        }
    }

    /**
     * Return the Transfer Acceleration state of a bucket, which is either Enabled or Suspended
     * @param bucket the bucket name.
     * @throws IOException if an error occurs when send request.
     * @return true if the Transfer Acceleration is Enabled, otherwise false
     * @see <a href="https://docs.aws.amazon.com/AmazonS3/latest/API/API_GetBucketAccelerateConfiguration.html">GetBucketAccelerateConfiguration</a>
     */
    public boolean isBucketAccelerated(String bucket) throws IOException {
        if (StringUtils.isEmpty(bucket)) {
            throw new IllegalArgumentException("bucket is required.");
        }

        Map<String, String> params = new HashMap<>();
        params.put("accelerate", "");

        Map<String, String> headers = new HashMap<String, String>();
        headers.put(HttpConstants.HEADER_AMZ_CONTENT_HASH, AWS4SignerBase.EMPTY_BODY_SHA256);

        URL url = new URL(buildUrl(bucket) + "/" + buildParams(params));

        AWS4SignerForAuthorizationHeader signer = new AWS4SignerForAuthorizationHeader(
                url, HttpConstants.HTTP_METHOD_GET, HttpConstants.CONSTANT_AWS_SERVICENAME_S3, region);
        String authorization = signer.computeSignature(headers,
                params,
                AWS4SignerBase.EMPTY_BODY_SHA256,
                accessKey,
                secretKey);

        headers.put(HttpConstants.HEADER_AUTHORIZATION, authorization);

        Request.Builder requestBuilder = new Request.Builder()
                .url(url)
                .get();
        for (Map.Entry<String, String> header : headers.entrySet()) {
            requestBuilder.addHeader(header.getKey(), header.getValue());
        }
        Call call = okHttpClient.newCall(requestBuilder.build());
        call.timeout().timeout(timeout, TimeUnit.SECONDS);
        try (Response response = call.execute()) {
            if (!response.isSuccessful()) {
                throw new CandyS3Exception(CommonErrorCode.SERVER_ERROR.getCode(), parseErrorMessage(response));
            }
            JsonNode root = xmlMapper.readTree(response.body().string());
            return "Enabled".equals(root.path("Status").asText(""));
        }
    }

    /**
     * Returns some or all (up to 1,000) of the objects in a bucket with each request. You can use the request parameters as selection criteria to return a subset of the objects in a bucket.
     * @param bucket the bucket name.
     * @param options the options to list objects.
     * @return the list pagination result of objects.
     * @throws IOException if an error occurs when send request.
     * @see <a href="https://docs.aws.amazon.com/AmazonS3/latest/API/API_ListObjectsV2.html">ListObjectsV2</a>
     */
    public ListPaginationResult<S3Object> listObjects(String bucket, ListObjectOptions options) throws IOException {
        Map<String, String> params = new HashMap<>();
        params.put("list-type", "2");
        if (!StringUtils.isEmpty(options.getContinuationToken())) {
            params.put("continuation-token", options.getContinuationToken());
        }
        if (options.getDelimiter() != null && !StringUtils.isEmpty(options.getDelimiter().toString())) {
            params.put("delimiter", options.getDelimiter().toString());
        }
        if (options.isFetchOwner()) {
            params.put("fetch-owner", options.isFetchOwner() + "");
        }
        if (options.getMaxKeys() != null) {
            params.put("max-keys", options.getMaxKeys() + "");
        }
        if (!StringUtils.isEmpty(options.getPrefix())) {
            params.put("prefix", options.getPrefix());
        }
        if (!StringUtils.isEmpty(options.getStartAfter())) {
            params.put("start-after", options.getStartAfter());
        }

        Map<String, String> headers = new HashMap<String, String>();
        headers.put(HttpConstants.HEADER_AMZ_CONTENT_HASH, AWS4SignerBase.EMPTY_BODY_SHA256);

        URL url = new URL(buildUrl(bucket) + "/" + buildParams(params));

        AWS4SignerForAuthorizationHeader signer = new AWS4SignerForAuthorizationHeader(
                url, HttpConstants.HTTP_METHOD_GET, HttpConstants.CONSTANT_AWS_SERVICENAME_S3, region);
        String authorization = signer.computeSignature(headers,
                params,
                AWS4SignerBase.EMPTY_BODY_SHA256,
                accessKey,
                secretKey);

        headers.put(HttpConstants.HEADER_AUTHORIZATION, authorization);

        Request.Builder requestBuilder = new Request.Builder()
                .url(url)
                .get();
        for (Map.Entry<String, String> header : headers.entrySet()) {
            requestBuilder.addHeader(header.getKey(), header.getValue());
        }
        Call call = okHttpClient.newCall(requestBuilder.build());
        call.timeout().timeout(timeout, TimeUnit.SECONDS);
        try (Response response = call.execute()) {
            if (!response.isSuccessful()) {
                S3ServerError serverError = parseErrorMessage(response);
                if (response.code() == 404) {
                    throw new CandyS3Exception(CommonErrorCode.NO_SUCH_BUCKET.getCode(), serverError);
                }
                throw new CandyS3Exception(CommonErrorCode.SERVER_ERROR.getCode(), serverError);
            }

            JsonNode root = xmlMapper.readTree(response.body().string());

            JsonNode contentsNode = root.path("Contents");
            List<S3Object> s3Objects = new ArrayList<>(0);
            if (!contentsNode.isMissingNode()) {
                // jackson cant recognize node as an array when it has only one child.
                if (contentsNode.isArray()) {
                    s3Objects = xmlMapper.treeToValue(contentsNode,
                            xmlMapper.getTypeFactory().constructCollectionType(List.class, S3Object.class));
                } else {
                    s3Objects = new ArrayList<>(1);
                    s3Objects.add(xmlMapper.treeToValue(contentsNode, S3Object.class));
                }
            }

            ListPaginationResult<S3Object> paginationResult = new ListPaginationResult<>(s3Objects, root.path("NextContinuationToken").asText());

            JsonNode commonPrefixesNode = root.path("CommonPrefixes");
            List<ListPaginationResult.CommonPrefix> commonPrefixes = new ArrayList<>();
            if (!commonPrefixesNode.isMissingNode()) {
                if (commonPrefixesNode.isArray()) {
                    commonPrefixes = xmlMapper.treeToValue(commonPrefixesNode,
                            xmlMapper.getTypeFactory().constructCollectionType(List.class, ListPaginationResult.CommonPrefix.class));
                } else {
                    commonPrefixes.add(xmlMapper.treeToValue(commonPrefixesNode, ListPaginationResult.CommonPrefix.class));
                }
            }
            paginationResult.setCommonPrefixes(commonPrefixes);

            return paginationResult;
        }
    }

    /**
     * Returns metadata about all versions of the objects in a bucket. You can also use request parameters as selection criteria to return metadata about a subset of all the object versions.
     * @param bucket the bucket name.
     * @param options the options to list object versions.
     * @return the list pagination result of object versions.
     * @throws IOException if an error occurs when send request.
     * @see <a href="https://docs.aws.amazon.com/AmazonS3/latest/API/API_ListObjectVersions.html">ListObjectVersions</a>
     */
    public ListPaginationResult<S3ObjectVersion> listObjectVersions(String bucket, ListObjectVersionsOptions options) throws IOException {
        Map<String, String> params = new HashMap<>();
        params.put("versions", "");
        if (!StringUtils.isEmpty(options.getKeyMarker())) {
            params.put("key-marker", options.getKeyMarker());
        }
        if (!StringUtils.isEmpty(options.getVersionIdMarker())) {
            params.put("version-id-marker", options.getVersionIdMarker());
        }
        if (options.getDelimiter() != null && !StringUtils.isEmpty(options.getDelimiter().toString())) {
            params.put("delimiter", options.getDelimiter().toString());
        }
        if (options.getMaxKeys() != null) {
            params.put("max-keys", options.getMaxKeys() + "");
        }
        if (!StringUtils.isEmpty(options.getPrefix())) {
            params.put("prefix", options.getPrefix());
        }


        Map<String, String> headers = new HashMap<String, String>();
        headers.put(HttpConstants.HEADER_AMZ_CONTENT_HASH, AWS4SignerBase.EMPTY_BODY_SHA256);

        URL url = new URL(buildUrl(bucket) + "/" + buildParams(params));

        AWS4SignerForAuthorizationHeader signer = new AWS4SignerForAuthorizationHeader(
                url, HttpConstants.HTTP_METHOD_GET, HttpConstants.CONSTANT_AWS_SERVICENAME_S3, region);
        String authorization = signer.computeSignature(headers,
                params,
                AWS4SignerBase.EMPTY_BODY_SHA256,
                accessKey,
                secretKey);

        headers.put(HttpConstants.HEADER_AUTHORIZATION, authorization);

        Request.Builder requestBuilder = new Request.Builder()
                .url(url)
                .get();
        for (Map.Entry<String, String> header : headers.entrySet()) {
            requestBuilder.addHeader(header.getKey(), header.getValue());
        }
        Call call = okHttpClient.newCall(requestBuilder.build());
        call.timeout().timeout(timeout, TimeUnit.SECONDS);
        try (Response response = call.execute()) {
            if (!response.isSuccessful()) {
                S3ServerError serverError = parseErrorMessage(response);
                if (response.code() == 404) {
                    throw new CandyS3Exception(CommonErrorCode.NO_SUCH_BUCKET.getCode(), serverError);
                }
                throw new CandyS3Exception(CommonErrorCode.SERVER_ERROR.getCode(), serverError);
            }

            JsonNode root = xmlMapper.readTree(response.body().string());

            JsonNode versionsNode = root.path("Version");
            List<S3ObjectVersion> s3ObjectVersions = new ArrayList<>(0);
            if (!versionsNode.isMissingNode()) {
                // jackson cant recognize node as an array when it has only one child.
                if (versionsNode.isArray()) {
                    s3ObjectVersions = xmlMapper.treeToValue(versionsNode,
                            xmlMapper.getTypeFactory().constructCollectionType(List.class, S3ObjectVersion.class));
                } else {
                    s3ObjectVersions = new ArrayList<>(1);
                    s3ObjectVersions.add(xmlMapper.treeToValue(versionsNode, S3ObjectVersion.class));
                }
            }

            JsonNode deleteMakersNode = root.path("DeleteMarker");
            List<S3ObjectVersion> deleteMakers = new ArrayList<>(0);
            if (!deleteMakersNode.isMissingNode()) {
                // jackson cant recognize node as an array when it has only one child.
                if (deleteMakersNode.isArray()) {
                    deleteMakers = xmlMapper.treeToValue(deleteMakersNode,
                            xmlMapper.getTypeFactory().constructCollectionType(List.class, S3ObjectVersion.class));
                } else {
                    deleteMakers = new ArrayList<>(1);
                    deleteMakers.add(xmlMapper.treeToValue(deleteMakersNode, S3ObjectVersion.class));
                }
            }

            ListPaginationResult<S3ObjectVersion> paginationResult = new ListPaginationResult<>(s3ObjectVersions, deleteMakers,
                    root.path("NextKeyMarker").asText(),
                    root.path("NextVersionIdMarker").asText());

            JsonNode commonPrefixesNode = root.path("CommonPrefixes");
            List<ListPaginationResult.CommonPrefix> commonPrefixes = new ArrayList<>();
            if (!commonPrefixesNode.isMissingNode()) {
                if (commonPrefixesNode.isArray()) {
                    commonPrefixes = xmlMapper.treeToValue(commonPrefixesNode,
                            xmlMapper.getTypeFactory().constructCollectionType(List.class, ListPaginationResult.CommonPrefix.class));
                } else {
                    commonPrefixes.add(xmlMapper.treeToValue(commonPrefixesNode, ListPaginationResult.CommonPrefix.class));
                }
            }
            paginationResult.setCommonPrefixes(commonPrefixes);

            return paginationResult;
        }
    }


    /**
     * Adds an object to a bucket.
     * This method will automatically determine whether to use multipart upload based on the size of the content, and automatically handle the multipart upload.
     * @param bucket the bucket name.
     * @param objectKey the object key.
     * @param options the options to put object.
     * @return ETag of the object.
     * @throws IOException if an error occurs when send request.
     * @throws NoSuchAlgorithmException if error occurs when compute signature.
     * @see <a href="https://docs.aws.amazon.com/AmazonS3/latest/API/API_PutObject.html">PutObject</a>
     */
    public String putObject(String bucket, String objectKey, PutObjectOptions options) throws IOException, NoSuchAlgorithmException {
        if (StringUtils.isEmpty(bucket)) {
            throw new IllegalArgumentException("bucket is required.");
        }
        if (StringUtils.isEmpty(objectKey)) {
            throw new IllegalArgumentException("objectKey is required.");
        }

        if (options.getObjectDataContentOptions() != null) {
            checkDataInput(options.getObjectDataContentOptions());
        }

        Map<String, String> headers = parseUploadHeaders(options.getHeaderProperties());
        headers.putAll(parseUploadHeaders(options.getObjectLockOptions()));
        headers.putAll(parseUploadHeaders(options.getServerSideEncryptionOptions()));
        if (options.getStorageClass() != null) {
            headers.put(HttpConstants.HEADER_AMZ_STORAGE_CLASS, options.getStorageClass());
        }
        if (options.getTagSet() != null && !options.getTagSet().isEmpty()) {
            headers.put(HttpConstants.HEADER_AMZ_TAGGING, buildUrlKVVariables(options.getTagSet()));
        }

        if (options.getObjectDataContentOptions() == null) {
            headers.putAll(parseUploadHeaders(options.getCondition()));
            // Create empty object
            return doUpload(bucket, objectKey, headers, null, null,
                    new byte[0]);
        } else {
            ObjectDataContentOptions dataContentOptions = options.getObjectDataContentOptions();
            if (ArrayUtils.isNotEmpty(dataContentOptions.getInputBytes())) {
                headers.putAll(parseUploadHeaders(options.getCondition()));
                // When use bytes in memory, upload directly.
                return doUpload(bucket, objectKey, headers, null, null,
                        dataContentOptions.getInputBytes());
            } else {
                BufferedInputStream bin;
                long len;
                if (dataContentOptions.getInputStream() != null) {
                    bin = new BufferedInputStream(dataContentOptions.getInputStream());
                    if (dataContentOptions.getInputStream() instanceof FileInputStream) {
                        len = ((FileInputStream) dataContentOptions.getInputStream()).getChannel().size();
                    } else {
                        // available may be not accurate when used as length.
                        len = dataContentOptions.getInputStream().available();
                    }
                } else {
                    File file = new File(dataContentOptions.getInputFile());
                    bin = new BufferedInputStream(new FileInputStream(file));
                    len = file.length();
                }

                if (len <= SIZE_5M) {
                    headers.putAll(parseUploadHeaders(options.getCondition()));
                    // When size is not greater to 5MB, upload directly.
                    ByteArrayOutputStream bout = new ByteArrayOutputStream();
                    byte[] bytes = new byte[SIZE_5M];
                    int l = -1;
                    while ((l = bin.read(bytes)) != -1) {
                        bout.write(bytes, 0, l);
                    }
                    bin.close();

                    byte[] data = bout.toByteArray();
                    bout.close();
                    return doUpload(bucket, objectKey, headers, null, null, data);

                } else {
                    String uploadId = this.createMultipartUpload(bucket, objectKey, headers);
                    if (StringUtils.isEmpty(uploadId)) {
                        throw new CandyS3Exception(CommonErrorCode.SERVER_ERROR.getCode(), CommonErrorCode.SERVER_ERROR.getMsg());
                    }
                    try {
                        List<S3Part> s3Parts = new ArrayList<>();
                        int partNum = 1;
                        ByteArrayOutputStream bout = null;
                        byte[] buffer = new byte[SIZE_5M];
                        int l = -1;
                        while ((l = bin.read(buffer)) != -1) {
                            bout = new ByteArrayOutputStream();
                            bout.write(buffer, 0, l);
                            // Part number must be an integer between 1 and 10000, inclusive.
                            S3Part part = this.uploadPart(bucket, objectKey, uploadId, partNum,
                                    new UploadPartOptions.UploadPartOptionsBuilder()
                                            // Note: be care of data input order
                                            // inputBytes is advanced to inputStream and then inputFile
                                            .configureUploadData()
                                            .withData(bout.toByteArray())
                                            .endConfigureDataContent()
                                            .build());
                            bout.close();
                            s3Parts.add(part);
                            partNum++;
                        }

                        CompleteMultipartUploadOptions completeMultipartUploadOptions =
                                new CompleteMultipartUploadOptions.CompleteMultipartUploadOptionsBuilder().build();
                        completeMultipartUploadOptions.setCondition(options.getCondition());
                        return this.completeMultipartUpload(bucket, objectKey, uploadId, s3Parts, completeMultipartUploadOptions);
                    } catch (Exception ex) {
                        if (StringUtils.isNotEmpty(uploadId)) {
                            this.abortMultipartUpload(bucket, uploadId, objectKey,
                                    new AbortMultipartUploadOptions());
                        }
                        throw ex;
                    }
                }
            }
        }
    }

    /**
     * upload object or part
     *
     * @param bucket the bucket name.
     * @param objectKey the object key.
     * @param uploadId  for multipart upload only. null when upload directly
     * @param partNum   for multipart upload only. null when upload directly
     * @param data the data to upload.
     * @return object/part etag
     * @throws IOException if an error occurs when send request.
     * @throws NoSuchAlgorithmException if error occurs when compute signature.
     */
    private String doUpload(String bucket, String objectKey,
                            Map<String, String> includedHeaders,
                            String uploadId, Integer partNum,
                            byte[] data) throws IOException, NoSuchAlgorithmException {

        Map<String, String> params = new HashMap<>();
        if (StringUtils.isNotEmpty(uploadId) && partNum != null) {
            params.put("uploadId", uploadId);
            params.put("partNumber", partNum + "");
        }

        if (includedHeaders == null) {
            includedHeaders = new HashMap<>();
        }

        includedHeaders.put("Content-Length", data.length + "");

        String bodyHash = ArrayUtils.isEmpty(data) ? AWS4SignerBase.EMPTY_BODY_SHA256 : BinaryUtils.toHex(AWS4SignerBase.hash(data));
        includedHeaders.put(HttpConstants.HEADER_AMZ_CONTENT_HASH, bodyHash);
        String contentMD5 = null;
        contentMD5 = BinaryUtils.md5(data);
        includedHeaders.put(HttpConstants.HEADER_CONTENT_MD5, contentMD5);

        URL url = new URL(buildUrl(bucket) + "/" + objectKey + buildParams(params));

        AWS4SignerForAuthorizationHeader signer = new AWS4SignerForAuthorizationHeader(
                url, HttpConstants.HTTP_METHOD_PUT, HttpConstants.CONSTANT_AWS_SERVICENAME_S3, region);
        String authorization = signer.computeSignature(includedHeaders,
                params,
                bodyHash,
                accessKey,
                secretKey);

        includedHeaders.put(HttpConstants.HEADER_AUTHORIZATION, authorization);

        Request.Builder requestBuilder = new Request.Builder()
                .url(url)
                .put(RequestBody.create(data));
        for (Map.Entry<String, String> header : includedHeaders.entrySet()) {
            requestBuilder.addHeader(header.getKey(), header.getValue());
        }
        Call call = okHttpClient.newCall(requestBuilder.build());
        call.timeout().timeout(timeout, TimeUnit.SECONDS);
        try (Response response = call.execute()) {
            if (!response.isSuccessful()) {
                S3ServerError serverError = parseErrorMessage(response);
                if (response.code() == 412 && "PreconditionFailed".equals(serverError.getCode())) {
                    throw new CandyS3Exception(CommonErrorCode.OBJECT_PRECONDITION_FAILED.getCode(), serverError);
                } else if (response.code() == 409 && "ConditionalRequestConflict".equals(serverError.getCode())) {
                    throw new CandyS3Exception(CommonErrorCode.OBJECT_CONDITIONAL_REQUEST_CONFLICT.getCode(), serverError);
                } else {
                    throw new CandyS3Exception(CommonErrorCode.SERVER_ERROR.getCode(), serverError);
                }
            }

            return response.header("ETag");
        }
    }

    /**
     * The HEAD operation retrieves metadata from an object without returning the object itself.
     * @param bucket the bucket name.
     * @param objectKey the object key.
     * @param options the options to head object.
     * @return S3Object which contains metadata only.
     * @throws IOException if an error occurs when send request.
     * @see <a href="https://docs.aws.amazon.com/AmazonS3/latest/API/API_HeadObject.html">HeadObject</a>
     */
    public S3Object getObjectMetadata(String bucket, String objectKey, DownloadObjectOptions options) throws IOException {
        if (StringUtils.isEmpty(bucket)) {
            throw new IllegalArgumentException("bucket is required.");
        }
        if (StringUtils.isEmpty(objectKey)) {
            throw new IllegalArgumentException("objectKey is required.");
        }

        Map<String, String> params = new HashMap<>();
        if (options.getPartNumber() != null) {
            params.put("partNumber", options.getPartNumber() + "");
        }
        if (StringUtils.isNotEmpty(options.getVersionId())) {
            params.put("versionId", options.getVersionId());
        }

        if (options.getResponseHeaderOptions() != null) {
            DownloadObjectOptions.DownloadObjectResponseHeaderOptions responseHeaderOptions =
                    options.getResponseHeaderOptions();
            if (StringUtils.isNotEmpty(responseHeaderOptions.getResponseCacheControl())) {
                params.put("response-cache-control", responseHeaderOptions.getResponseCacheControl());
            }
            if (StringUtils.isNotEmpty(responseHeaderOptions.getResponseContentDisposition())) {
                params.put("response-content-disposition", responseHeaderOptions.getResponseContentDisposition());
            }
            if (StringUtils.isNotEmpty(responseHeaderOptions.getResponseContentEncoding())) {
                params.put("response-content-encoding", responseHeaderOptions.getResponseContentEncoding());
            }
            if (StringUtils.isNotEmpty(responseHeaderOptions.getResponseContentLanguage())) {
                params.put("response-content-language", responseHeaderOptions.getResponseContentLanguage());
            }
            if (StringUtils.isNotEmpty(responseHeaderOptions.getResponseContentType())) {
                params.put("response-content-type", responseHeaderOptions.getResponseContentType());
            }
            if (responseHeaderOptions.getResponseExpires() != null) {
                params.put("response-expires", formatHttpDate(responseHeaderOptions.getResponseExpires()));
            }
        }

        Map<String, String> headers = new HashMap<String, String>();
        if (StringUtils.isNotEmpty(options.getRange())) {
            headers.put("Range", "bytes=" + options.getRange());
        }
        headers.putAll(parseDownloadHeaders(options.getCondition()));

        headers.put(HttpConstants.HEADER_AMZ_CONTENT_HASH, AWS4SignerBase.EMPTY_BODY_SHA256);

        URL url = new URL(buildUrl(bucket) + "/" + objectKey + buildParams(params));

        AWS4SignerForAuthorizationHeader signer = new AWS4SignerForAuthorizationHeader(
                url, HttpConstants.HTTP_METHOD_HEAD, HttpConstants.CONSTANT_AWS_SERVICENAME_S3, region);
        String authorization = signer.computeSignature(headers,
                params,
                AWS4SignerBase.EMPTY_BODY_SHA256,
                accessKey,
                secretKey);

        headers.put(HttpConstants.HEADER_AUTHORIZATION, authorization);

        Request.Builder requestBuilder = new Request.Builder()
                .url(url)
                .head();
        for (Map.Entry<String, String> header : headers.entrySet()) {
            requestBuilder.addHeader(header.getKey(), header.getValue());
        }
        Call call = okHttpClient.newCall(requestBuilder.build());
        call.timeout().timeout(timeout, TimeUnit.SECONDS);
        try (Response response = call.execute()) {
            if (!response.isSuccessful()) {
                if (response.code() == 304) {
                    throw new CandyS3Exception(CommonErrorCode.OBJECT_NOT_MODIFIED.getCode(),
                            CommonErrorCode.OBJECT_NOT_MODIFIED.getMsg());
                } else if (response.code() == 404) {
                    if (response.headers().names().contains("x-amz-delete-marker") && Boolean.parseBoolean(response.headers().get("x-amz-delete-marker"))) {
                        throw new CandyS3Exception(CommonErrorCode.OBJECT_VERSION_IS_DELETE_MARKER.getCode(),
                                CommonErrorCode.OBJECT_VERSION_IS_DELETE_MARKER.getMsg());
                    } else {
                        throw new CandyS3Exception(CommonErrorCode.NO_SUCH_OBJECT.getCode(),
                                CommonErrorCode.NO_SUCH_OBJECT.getMsg());
                    }
                } else {
                    S3ServerError serverError = parseErrorMessage(response);
                    if (response.code() == 412) {
                        serverError.setCode("PreconditionFailed");
                        throw new CandyS3Exception(CommonErrorCode.OBJECT_PRECONDITION_FAILED.getCode(), serverError);
                    } else {
                        throw new CandyS3Exception(CommonErrorCode.SERVER_ERROR.getCode(), serverError);
                    }
                }
            }

            Headers responseHeaders = response.headers();

            S3Object s3Object = new S3Object();
            s3Object.setKey(objectKey);
            s3Object.setLastModified(responseHeaders.getDate("Last-Modified"));
            s3Object.seteTag(responseHeaders.get("ETag"));
            s3Object.setSize(Long.parseLong(responseHeaders.get("Content-Length")));
            s3Object.setStorageClass(responseHeaders.get("x-amz-storage-class"));
            s3Object.setVersionId(responseHeaders.get("x-amz-version-id"));
            if (responseHeaders.names().contains("x-amz-mp-parts-count")) {
                s3Object.setPartsCount(Integer.parseInt(responseHeaders.get("x-amz-mp-parts-count")));
            }
            if (responseHeaders.names().contains("x-amz-tagging-count")) {
                s3Object.setTagCount(Integer.parseInt(responseHeaders.get("x-amz-tagging-count")));
            }
            parseObjectLockPropertiesFromResponseHeaders(s3Object, responseHeaders);
            parseObjectSseConfigurationFromResponseHeaders(s3Object, responseHeaders);
            s3Object.setObjectMetadata(parseObjectMetadataFromResponseHeaders(responseHeaders));
            return s3Object;
        }
    }

    /**
     * Retrieves an object's retention settings.
     * @param bucket the bucket name.
     * @param objectKey the object key.
     * @param options the options to get object retention.
     * @return the object retention properties.
     * @throws IOException if an error occurs when send request.
     * @see <a href="https://docs.aws.amazon.com/AmazonS3/latest/API/API_GetObjectRetention.html">GetObjectRetention</a>
     */
    public ObjectLockProperties getObjectRetention(String bucket, String objectKey, GetObjectRetentionOptions options) throws IOException {
        if (StringUtils.isEmpty(bucket)) {
            throw new IllegalArgumentException("bucket is required.");
        }
        if (StringUtils.isEmpty(objectKey)) {
            throw new IllegalArgumentException("objectKey is required.");
        }

        Map<String, String> params = new HashMap<>();
        params.put("retention", "");
        if (StringUtils.isNotEmpty(options.getVersionId())) {
            params.put("versionId", options.getVersionId());
        }

        Map<String, String> headers = new HashMap<String, String>();
        headers.put(HttpConstants.HEADER_AMZ_CONTENT_HASH, AWS4SignerBase.EMPTY_BODY_SHA256);

        URL url = new URL(buildUrl(bucket) + "/" + objectKey + buildParams(params));

        AWS4SignerForAuthorizationHeader signer = new AWS4SignerForAuthorizationHeader(
                url, HttpConstants.HTTP_METHOD_GET, HttpConstants.CONSTANT_AWS_SERVICENAME_S3, region);
        String authorization = signer.computeSignature(headers,
                params,
                AWS4SignerBase.EMPTY_BODY_SHA256,
                accessKey,
                secretKey);

        headers.put(HttpConstants.HEADER_AUTHORIZATION, authorization);

        Request.Builder requestBuilder = new Request.Builder()
                .url(url)
                .get();
        for (Map.Entry<String, String> header : headers.entrySet()) {
            requestBuilder.addHeader(header.getKey(), header.getValue());
        }
        Call call = okHttpClient.newCall(requestBuilder.build());
        call.timeout().timeout(timeout, TimeUnit.SECONDS);
        try (Response response = call.execute()) {
            if (!response.isSuccessful()) {
                S3ServerError serverError = parseErrorMessage(response);
                if (response.code() == 404 && "NoSuchObjectLockConfiguration".equals(serverError.getCode())) {
                    throw new CandyS3Exception(CommonErrorCode.NO_SUCH_OBJECT_LOCK_CONFIGURATION.getCode(), serverError);
                } else {
                    throw new CandyS3Exception(CommonErrorCode.SERVER_ERROR.getCode(), serverError);
                }
            }
            JsonNode root = xmlMapper.readTree(response.body().string());

            ObjectLockProperties objectLockProperties = new ObjectLockProperties();

            objectLockProperties.setObjectLockMode(ObjectRetentionMode.valueOf(root.path("Mode").asText()));

            JsonNode retainUntilDateNode = root.path("RetainUntilDate");
            if (!retainUntilDateNode.isMissingNode()) {
                SimpleDateFormat simpleDateFormatSec = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss'Z'");
                simpleDateFormatSec.setTimeZone(new SimpleTimeZone(0, "UTC"));
                try {
                    objectLockProperties.setObjectLockRetainUntilDate(simpleDateFormatSec.parse(retainUntilDateNode.asText()));
                } catch (ParseException e) {
                    ParseException finalParseException = e;
                    SimpleDateFormat simpleDateFormatMill = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSS'Z'");
                    simpleDateFormatMill.setTimeZone(new SimpleTimeZone(0, "UTC"));
                    try {
                        objectLockProperties.setObjectLockRetainUntilDate(simpleDateFormatMill.parse(retainUntilDateNode.asText()));
                        finalParseException = null;
                    } catch (ParseException ex) {
                        finalParseException = ex;
                    }

                    if (finalParseException != null) {
                        throw new CandyS3Exception(CommonErrorCode.SERVER_ERROR.getCode(), e.getMessage());
                    }
                }
            }
            return objectLockProperties;
        }
    }


    /**
     * Places an Object Retention configuration on an object.
     * @param bucket the bucket name.
     * @param objectKey the object key.
     * @param options the options to put object retention.
     * @throws IOException if an error occurs when send request.
     * @throws NoSuchAlgorithmException if error occurs when compute signature.
     * @see <a href="https://docs.aws.amazon.com/AmazonS3/latest/API/API_PutObjectRetention.html">PutObjectRetention</a>
     */
    public void updateObjectRetention(String bucket, String objectKey, UpdateObjectRetentionOptions options) throws IOException, NoSuchAlgorithmException {
        if (StringUtils.isEmpty(bucket)) {
            throw new IllegalArgumentException("bucket is required.");
        }
        if (StringUtils.isEmpty(objectKey)) {
            throw new IllegalArgumentException("objectKey is required.");
        }
        Map<String, String> params = new HashMap<>();
        params.put("retention", "");
        if (StringUtils.isNotEmpty(options.getVersionId())) {
            params.put("versionId", options.getVersionId());
        }

        Map<String, String> headers = new HashMap<String, String>();
        if (options.isBypassGovernanceRetention()) {
            headers.put(HttpConstants.HEADER_AMZ_BYPASS_GOVERNANCE_RETENTION, true + "");
        }

        Retention retention = new Retention();
        if (options.getRetentionMode() != null) {
            retention.mode = options.getRetentionMode().name();

            SimpleDateFormat simpleDateFormat = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss'Z'");
            simpleDateFormat.setTimeZone(new SimpleTimeZone(0, "UTC"));
            retention.retainUntilDate = simpleDateFormat.format(options.getObjectLockRetainUntilDate());
        }

        String body = xmlMapper.writeValueAsString(retention);
        String bodyHash = StringUtils.isEmpty(body) ? AWS4SignerBase.EMPTY_BODY_SHA256 : BinaryUtils.toHex(AWS4SignerBase.hash(body));
        headers.put(HttpConstants.HEADER_AMZ_CONTENT_HASH, bodyHash);
        headers.put(HttpConstants.HEADER_CONTENT_MD5, BinaryUtils.md5(body));

        URL url = new URL(buildUrl(bucket) + "/" + objectKey + buildParams(params));

        AWS4SignerForAuthorizationHeader signer = new AWS4SignerForAuthorizationHeader(
                url, HttpConstants.HTTP_METHOD_PUT, HttpConstants.CONSTANT_AWS_SERVICENAME_S3, region);
        String authorization = signer.computeSignature(headers,
                params,
                bodyHash,
                accessKey,
                secretKey);

        headers.put(HttpConstants.HEADER_AUTHORIZATION, authorization);

        Request.Builder requestBuilder = new Request.Builder()
                .url(url)
                .put(RequestBody.create(body, MediaType.get("text/plain")));
        for (Map.Entry<String, String> header : headers.entrySet()) {
            requestBuilder.addHeader(header.getKey(), header.getValue());
        }
        Call call = okHttpClient.newCall(requestBuilder.build());
        call.timeout().timeout(timeout, TimeUnit.SECONDS);
        try (Response response = call.execute()) {
            if (!response.isSuccessful()) {
                throw new CandyS3Exception(CommonErrorCode.SERVER_ERROR.getCode(), parseErrorMessage(response));
            }
        }
    }

    /**
     * Applies a legal hold configuration to the specified object.
     * @param bucket the bucket name.
     * @param objectKey the object key.
     * @param options the options to put object legal hold.
     * @throws IOException if an error occurs when send request.
     * @throws NoSuchAlgorithmException if error occurs when compute signature.
     * @see <a href="https://docs.aws.amazon.com/AmazonS3/latest/API/API_PutObjectLegalHold.html">PutObjectLegalHold</a>
     */
    public void updateObjectLegalHold(String bucket, String objectKey, ObjectLegalHoldOptions options) throws IOException, NoSuchAlgorithmException {
        if (StringUtils.isEmpty(bucket)) {
            throw new IllegalArgumentException("bucket is required.");
        }
        if (StringUtils.isEmpty(objectKey)) {
            throw new IllegalArgumentException("objectKey is required.");
        }

        Map<String, String> params = new HashMap<>();
        params.put("legal-hold", "");
        if (StringUtils.isNotEmpty(options.getVersionId())) {
            params.put("versionId", options.getVersionId());
        }

        Map<String, String> headers = new HashMap<String, String>();

        LegalHold legalHold = new LegalHold();
        if (options.isLegalHold()) {
            legalHold.status = "ON";
        } else {
            legalHold.status = "OFF";
        }

        String body = xmlMapper.writeValueAsString(legalHold);
        String bodyHash = StringUtils.isEmpty(body) ? AWS4SignerBase.EMPTY_BODY_SHA256 : BinaryUtils.toHex(AWS4SignerBase.hash(body));
        headers.put(HttpConstants.HEADER_AMZ_CONTENT_HASH, bodyHash);
        headers.put(HttpConstants.HEADER_CONTENT_MD5, BinaryUtils.md5(body));

        URL url = new URL(buildUrl(bucket) + "/" + objectKey + buildParams(params));

        AWS4SignerForAuthorizationHeader signer = new AWS4SignerForAuthorizationHeader(
                url, HttpConstants.HTTP_METHOD_PUT, HttpConstants.CONSTANT_AWS_SERVICENAME_S3, region);
        String authorization = signer.computeSignature(headers,
                params,
                bodyHash,
                accessKey,
                secretKey);

        headers.put(HttpConstants.HEADER_AUTHORIZATION, authorization);

        Request.Builder requestBuilder = new Request.Builder()
                .url(url)
                .put(RequestBody.create(body, MediaType.get("text/plain")));
        for (Map.Entry<String, String> header : headers.entrySet()) {
            requestBuilder.addHeader(header.getKey(), header.getValue());
        }
        Call call = okHttpClient.newCall(requestBuilder.build());
        call.timeout().timeout(timeout, TimeUnit.SECONDS);
        try (Response response = call.execute()) {
            if (!response.isSuccessful()) {
                throw new CandyS3Exception(CommonErrorCode.SERVER_ERROR.getCode(), parseErrorMessage(response));
            }
        }
    }

    /**
     * Gets an object's current legal hold status.
     * @param bucket the bucket name.
     * @param objectKey the object key.
     * @param options the options to get object legal hold.
     * @return the object legal hold status.
     * @throws IOException if an error occurs when send request.
     * @throws NoSuchAlgorithmException if error occurs when compute signature.
     * @see <a href="https://docs.aws.amazon.com/AmazonS3/latest/API/API_GetObjectLegalHold.html">GetObjectLegalHold</a>
     */
    public boolean isObjectLegalHold(String bucket, String objectKey, ObjectLegalHoldOptions options) throws IOException, NoSuchAlgorithmException {
        if (StringUtils.isEmpty(bucket)) {
            throw new IllegalArgumentException("bucket is required.");
        }
        if (StringUtils.isEmpty(objectKey)) {
            throw new IllegalArgumentException("objectKey is required.");
        }

        Map<String, String> params = new HashMap<>();
        params.put("legal-hold", "");
        if (StringUtils.isNotEmpty(options.getVersionId())) {
            params.put("versionId", options.getVersionId());
        }

        Map<String, String> headers = new HashMap<String, String>();
        headers.put(HttpConstants.HEADER_AMZ_CONTENT_HASH, AWS4SignerBase.EMPTY_BODY_SHA256);

        URL url = new URL(buildUrl(bucket) + "/" + objectKey + buildParams(params));

        AWS4SignerForAuthorizationHeader signer = new AWS4SignerForAuthorizationHeader(
                url, HttpConstants.HTTP_METHOD_GET, HttpConstants.CONSTANT_AWS_SERVICENAME_S3, region);
        String authorization = signer.computeSignature(headers,
                params,
                AWS4SignerBase.EMPTY_BODY_SHA256,
                accessKey,
                secretKey);

        headers.put(HttpConstants.HEADER_AUTHORIZATION, authorization);

        Request.Builder requestBuilder = new Request.Builder()
                .url(url)
                .get();
        for (Map.Entry<String, String> header : headers.entrySet()) {
            requestBuilder.addHeader(header.getKey(), header.getValue());
        }
        Call call = okHttpClient.newCall(requestBuilder.build());
        call.timeout().timeout(timeout, TimeUnit.SECONDS);
        try (Response response = call.execute()) {
            if (!response.isSuccessful()) {
                S3ServerError serverError = parseErrorMessage(response);
                if (response.code() == 404 && "NoSuchObjectLockConfiguration".equals(serverError.getCode())) {
                    throw new CandyS3Exception(CommonErrorCode.NO_SUCH_OBJECT_LOCK_CONFIGURATION.getCode(), serverError);
                } else {
                    throw new CandyS3Exception(CommonErrorCode.SERVER_ERROR.getCode(), serverError);
                }
            }
            JsonNode root = xmlMapper.readTree(response.body().string());
            return "ON".equals(root.path("Status").asText(""));
        }
    }

    /**
     * Retrieves an object from Amazon S3.
     * @param bucket the bucket name.
     * @param objectKey the object key.
     * @param options the options to download object.
     * @return use return S3Object to retrieve response headers(like Cache-Control), and get content bytes if neither outputFile or outputStream used.
     * @throws IOException if an error occurs when send request.
     * @see <a href="https://docs.aws.amazon.com/AmazonS3/latest/API/API_GetObject.html">GetObject</a>
     */
    public S3Object downloadObject(String bucket, String objectKey, DownloadObjectOptions options) throws IOException {
        if (StringUtils.isEmpty(bucket)) {
            throw new IllegalArgumentException("bucket is required.");
        }
        if (StringUtils.isEmpty(objectKey)) {
            throw new IllegalArgumentException("objectKey is required.");
        }
        if (options.getDataOutput() != null) {
            if (StringUtils.isNotEmpty(options.getDataOutput().getOutputFile())) {
                File file = new File(options.getDataOutput().getOutputFile());
                if (file.exists() && !options.getDataOutput().isCanOverwrite()) {
                    throw new CandyS3Exception(CommonErrorCode.OUTPUT_FILE_ALREADY_EXISTS.getCode(),
                            CommonErrorCode.OUTPUT_FILE_ALREADY_EXISTS.getMsg());
                }
            }
        }

        Map<String, String> params = new HashMap<>();
        if (options.getPartNumber() != null) {
            params.put("partNumber", options.getPartNumber() + "");
        }
        if (StringUtils.isNotEmpty(options.getVersionId())) {
            params.put("versionId", options.getVersionId());
        }

        if (options.getResponseHeaderOptions() != null) {
            DownloadObjectOptions.DownloadObjectResponseHeaderOptions responseHeaderOptions =
                    options.getResponseHeaderOptions();
            if (StringUtils.isNotEmpty(responseHeaderOptions.getResponseCacheControl())) {
                params.put("response-cache-control", responseHeaderOptions.getResponseCacheControl());
            }
            if (StringUtils.isNotEmpty(responseHeaderOptions.getResponseContentDisposition())) {
                params.put("response-content-disposition", responseHeaderOptions.getResponseContentDisposition());
            }
            if (StringUtils.isNotEmpty(responseHeaderOptions.getResponseContentEncoding())) {
                params.put("response-content-encoding", responseHeaderOptions.getResponseContentEncoding());
            }
            if (StringUtils.isNotEmpty(responseHeaderOptions.getResponseContentLanguage())) {
                params.put("response-content-language", responseHeaderOptions.getResponseContentLanguage());
            }
            if (StringUtils.isNotEmpty(responseHeaderOptions.getResponseContentType())) {
                params.put("response-content-type", responseHeaderOptions.getResponseContentType());
            }
            if (responseHeaderOptions.getResponseExpires() != null) {
                params.put("response-expires", formatHttpDate(responseHeaderOptions.getResponseExpires()));
            }
        }

        Map<String, String> headers = new HashMap<String, String>();
        if (StringUtils.isNotEmpty(options.getRange())) {
            headers.put("Range", "bytes=" + options.getRange());
        }
        headers.putAll(parseDownloadHeaders(options.getCondition()));

        headers.put(HttpConstants.HEADER_AMZ_CONTENT_HASH, AWS4SignerBase.EMPTY_BODY_SHA256);

        URL url = new URL(buildUrl(bucket) + "/" + objectKey + buildParams(params));

        AWS4SignerForAuthorizationHeader signer = new AWS4SignerForAuthorizationHeader(
                url, HttpConstants.HTTP_METHOD_GET, HttpConstants.CONSTANT_AWS_SERVICENAME_S3, region);
        String authorization = signer.computeSignature(headers,
                params,
                AWS4SignerBase.EMPTY_BODY_SHA256,
                accessKey,
                secretKey);

        headers.put(HttpConstants.HEADER_AUTHORIZATION, authorization);

        Request.Builder requestBuilder = new Request.Builder()
                .url(url)
                .get();
        for (Map.Entry<String, String> header : headers.entrySet()) {
            requestBuilder.addHeader(header.getKey(), header.getValue());
        }
        Call call = okHttpClient.newCall(requestBuilder.build());
        call.timeout().timeout(timeout, TimeUnit.SECONDS);
        try (Response response = call.execute()) {
            if (!response.isSuccessful()) {
                if (response.code() == 304) {
                    throw new CandyS3Exception(CommonErrorCode.OBJECT_NOT_MODIFIED.getCode(),
                            CommonErrorCode.OBJECT_NOT_MODIFIED.getMsg());
                } else if (response.code() == 404) {
                    throw new CandyS3Exception(CommonErrorCode.NO_SUCH_OBJECT.getCode(),
                            CommonErrorCode.NO_SUCH_OBJECT.getMsg());
                } else {
                    S3ServerError serverError = parseErrorMessage(response);
                    if (response.code() == 412 && "PreconditionFailed".equals(serverError.getCode())) {
                        throw new CandyS3Exception(CommonErrorCode.OBJECT_PRECONDITION_FAILED.getCode(), serverError);
                    } else {
                        throw new CandyS3Exception(CommonErrorCode.SERVER_ERROR.getCode(), serverError);
                    }
                }
            }

            Headers responseHeaders = response.headers();

            S3Object s3Object = new S3Object();
            s3Object.setKey(objectKey);
            s3Object.setLastModified(responseHeaders.getDate("Last-Modified"));
            s3Object.seteTag(responseHeaders.get("ETag"));
            s3Object.setSize(Long.parseLong(responseHeaders.get("Content-Length")));
            s3Object.setStorageClass(responseHeaders.get("x-amz-storage-class"));
            s3Object.setVersionId(responseHeaders.get("x-amz-version-id"));
            if (responseHeaders.names().contains("x-amz-mp-parts-count")) {
                s3Object.setPartsCount(Integer.parseInt(responseHeaders.get("x-amz-mp-parts-count")));
            }
            if (responseHeaders.names().contains("x-amz-tagging-count")) {
                s3Object.setTagCount(Integer.parseInt(responseHeaders.get("x-amz-tagging-count")));
            }
            parseObjectLockPropertiesFromResponseHeaders(s3Object, responseHeaders);
            parseObjectSseConfigurationFromResponseHeaders(s3Object, responseHeaders);
            s3Object.setObjectMetadata(parseObjectMetadataFromResponseHeaders(responseHeaders));

            if (response.body() == null) {
                return s3Object;
            }

            DownloadObjectOptions.DownloadObjectDataOutput dataOutput = options.getDataOutput();
            if (dataOutput == null || (StringUtils.isEmpty(dataOutput.getOutputFile()) && dataOutput.getOutputStream() == null)) {
                s3Object.setContentBytes(response.body().bytes());
            } else {
                BufferedOutputStream out = null;
                if (StringUtils.isNotEmpty(dataOutput.getOutputFile())) {
                    File file = new File(dataOutput.getOutputFile());
                    // re-check if file can be overwritten when already exists.
                    if (file.exists() && !dataOutput.isCanOverwrite()) {
                        throw new CandyS3Exception(CommonErrorCode.OUTPUT_FILE_ALREADY_EXISTS.getCode(),
                                CommonErrorCode.OUTPUT_FILE_ALREADY_EXISTS.getMsg());
                    }
                    out = new BufferedOutputStream(new FileOutputStream(file));
                } else if (dataOutput.getOutputStream() != null) {
                    out = new BufferedOutputStream(dataOutput.getOutputStream());
                }

                assert out != null;

                InputStream in = response.body().byteStream();
                byte[] buffer = new byte[1024];
                int len = -1;
                while ((len = in.read(buffer)) != -1) {
                    out.write(buffer, 0, len);
                }
                in.close();
                out.flush();
                out.close();
            }

            return s3Object;
        }
    }

    /**
     * Removes an object from a bucket.
     * @param bucket the bucket name.
     * @param options the options to delete object.
     * @throws IOException if an error occurs when send request.
     * @see <a href="https://docs.aws.amazon.com/AmazonS3/latest/API/API_DeleteObject.html">DeleteObject</a>
     */
    public void deleteObject(String bucket, DeleteObjectOptions options) throws IOException {
        if (StringUtils.isEmpty(bucket)) {
            throw new IllegalArgumentException("bucket is required.");
        }
        if (StringUtils.isEmpty(options.getKey())) {
            throw new IllegalArgumentException("objectKey is required.");
        }

        Map<String, String> params = new HashMap<>();
        if (StringUtils.isNotEmpty(options.getVersionId())) {
            params.put("versionId", options.getVersionId());
        }

        Map<String, String> headers = new HashMap<String, String>();

        if (options.isBypassGovernanceRetention()) {
            headers.put(HttpConstants.HEADER_AMZ_BYPASS_GOVERNANCE_RETENTION, true + "");
        }

        if (options.getConditionalOptions() != null) {
            headers.put(HttpConstants.HEADER_IF_MATCH, options.getConditionalOptions().getIfMatch());
        }

        headers.put(HttpConstants.HEADER_AMZ_CONTENT_HASH, AWS4SignerBase.EMPTY_BODY_SHA256);

        URL url = new URL(buildUrl(bucket) + "/" + options.getKey() + buildParams(params));

        AWS4SignerForAuthorizationHeader signer = new AWS4SignerForAuthorizationHeader(
                url, HttpConstants.HTTP_METHOD_DELETE, HttpConstants.CONSTANT_AWS_SERVICENAME_S3, region);
        String authorization = signer.computeSignature(headers,
                params,
                AWS4SignerBase.EMPTY_BODY_SHA256,
                accessKey,
                secretKey);

        headers.put(HttpConstants.HEADER_AUTHORIZATION, authorization);

        Request.Builder requestBuilder = new Request.Builder()
                .url(url)
                .delete();
        for (Map.Entry<String, String> header : headers.entrySet()) {
            requestBuilder.addHeader(header.getKey(), header.getValue());
        }
        Call call = okHttpClient.newCall(requestBuilder.build());
        call.timeout().timeout(timeout, TimeUnit.SECONDS);
        try (Response response = call.execute()) {
            if (!response.isSuccessful()) {
                S3ServerError serverError = parseErrorMessage(response);
                if (response.code() == 412 && "PreconditionFailed".equals(serverError.getCode())) {
                    throw new CandyS3Exception(CommonErrorCode.OBJECT_PRECONDITION_FAILED.getCode(), serverError);
                } else {
                    throw new CandyS3Exception(CommonErrorCode.SERVER_ERROR.getCode(), serverError);
                }
            }
        }
    }

    /**
     * This operation enables you to delete multiple objects from a bucket using a single HTTP request. If you know the object keys that you want to delete, then this operation provides a suitable alternative to sending individual delete requests, reducing per-request overhead.
     * @param bucket the bucket name.
     * @param options the options to delete objects batch.
     * @return the delete objects batch result.
     * @throws IOException if an error occurs when send request.
     * @throws NoSuchAlgorithmException if error occurs when compute signature.
     * @see <a href="https://docs.aws.amazon.com/AmazonS3/latest/API/API_DeleteObjects.html">DeleteObjects</a>
     */
    public DeleteObjectsBatchResult deleteObjectsBatch(String bucket, DeleteObjectsBatchOptions options) throws IOException, NoSuchAlgorithmException {
        if (StringUtils.isEmpty(bucket)) {
            throw new IllegalArgumentException("bucket is required.");
        }

        Map<String, String> params = new HashMap<>();
        params.put("delete", "");

        Map<String, String> headers = new HashMap<String, String>();
        if (options.isBypassGovernanceRetention()) {
            headers.put(HttpConstants.HEADER_AMZ_BYPASS_GOVERNANCE_RETENTION, true + "");
        }

        Delete delete = new Delete();
        delete.deleteObjects = new ArrayList<>(options.getDeleteObjectKeys().size());
        for (DeleteObjectsBatchOptions.DeleteObjectsBatchItem deleteObjectItem : options.getDeleteObjectKeys()) {
            DeleteObject deleteObject = new DeleteObject();
            if (StringUtils.isEmpty(deleteObjectItem.getKey())) {
                throw new IllegalArgumentException("objectKey is required.");
            }
            deleteObject.key = deleteObjectItem.getKey();
            deleteObject.versionId = deleteObjectItem.getVersionId();
            if (StringUtils.isNotBlank(deleteObjectItem.getIfMatch())) {
                deleteObject.eTag = deleteObjectItem.getIfMatch();
            }
            delete.deleteObjects.add(deleteObject);
        }

        String body = xmlMapper.writeValueAsString(delete);
        headers.put(HttpConstants.HEADER_CONTENT_MD5, BinaryUtils.md5(body));
        String bodyHash = BinaryUtils.toHex(AWS4SignerBase.hash(body));
        headers.put(HttpConstants.HEADER_AMZ_CONTENT_HASH, bodyHash);

        URL url = new URL(buildUrl(bucket) + "/" + buildParams(params));

        AWS4SignerForAuthorizationHeader signer = new AWS4SignerForAuthorizationHeader(
                url, HttpConstants.HTTP_METHOD_POST, HttpConstants.CONSTANT_AWS_SERVICENAME_S3, region);
        String authorization = signer.computeSignature(headers,
                params,
                bodyHash,
                accessKey,
                secretKey);

        headers.put(HttpConstants.HEADER_AUTHORIZATION, authorization);

        Request.Builder requestBuilder = new Request.Builder()
                .url(url)
                .post(RequestBody.create(body, MediaType.get("text/plain")));
        for (Map.Entry<String, String> header : headers.entrySet()) {
            requestBuilder.addHeader(header.getKey(), header.getValue());
        }
        Call call = okHttpClient.newCall(requestBuilder.build());
        call.timeout().timeout(timeout, TimeUnit.SECONDS);
        try (Response response = call.execute()) {
            if (!response.isSuccessful()) {
                throw new CandyS3Exception(CommonErrorCode.SERVER_ERROR.getCode(), parseErrorMessage(response));
            }

            JsonNode root = xmlMapper.readTree(response.body().string());

            DeleteObjectsBatchResult result = new DeleteObjectsBatchResult();

            JsonNode errorsNode = root.path("Error");
            if (errorsNode.isMissingNode()) {
                result.setSuccessful(true);
            } else {
                result.setSuccessful(false);
                List<DeleteObjectsBatchResult.DeleteError> errors = new ArrayList<>(0);
                if (errorsNode.isArray()) {
                    errors = xmlMapper.treeToValue(errorsNode,
                            xmlMapper.getTypeFactory()
                                    .constructCollectionType(List.class, DeleteObjectsBatchResult.DeleteError.class));
                } else {
                    errors.add(xmlMapper.treeToValue(errorsNode, DeleteObjectsBatchResult.DeleteError.class));
                }
                result.setErrors(errors);
            }

            JsonNode deletedObjectsNode = root.path("Deleted");
            if (!deletedObjectsNode.isMissingNode()) {
                List<DeleteObjectsBatchResult.DeletedObject> deletedObjects = new ArrayList<>(0);
                if (deletedObjectsNode.isArray()) {
                    deletedObjects = xmlMapper.treeToValue(deletedObjectsNode,
                            xmlMapper.getTypeFactory()
                                    .constructCollectionType(List.class, DeleteObjectsBatchResult.DeletedObject.class));
                } else {
                    deletedObjects.add(xmlMapper.treeToValue(deletedObjectsNode, DeleteObjectsBatchResult.DeletedObject.class));
                }
                result.setDeleted(deletedObjects);
            }

            return result;
        }
    }


    /**
     * Creates a copy of an object that is already stored in Amazon S3.
     * @param bucket the bucket name.
     * @param objectKey the object key.
     * @param options the options to copy object.
     * @return the eTag of the copied object.
     * @throws IOException if an error occurs when send request.
     * @see <a href="https://docs.aws.amazon.com/AmazonS3/latest/API/API_CopyObject.html">CopyObject</a>
     */
    public String copyObject(String bucket, String objectKey, CopyObjectOptions options) throws IOException {
        if (StringUtils.isEmpty(bucket)) {
            throw new IllegalArgumentException("bucket is required.");
        }
        if (StringUtils.isEmpty(objectKey)) {
            throw new IllegalArgumentException("objectKey is required.");
        }

        Map<String, String> headers = parseUploadHeaders(options.getHeaderProperties());
        if (options.isReplaceMetadataDirective()) {
            headers.put(HttpConstants.HEADER_AMZ_METADATA_DIRECTIVE, "REPLACE");
        } else {
            headers.put(HttpConstants.HEADER_AMZ_METADATA_DIRECTIVE, "COPY");
        }

        headers.putAll(parseUploadHeaders(options.getObjectLockOptions()));
        headers.putAll(parseUploadHeaders(options.getServerSideEncryptionOptions()));

        if (options.getStorageClass() != null) {
            headers.put(HttpConstants.HEADER_AMZ_STORAGE_CLASS, options.getStorageClass());
        }

        if (options.getTagSet() != null && !options.getTagSet().isEmpty()) {
            headers.put(HttpConstants.HEADER_AMZ_TAGGING, buildUrlKVVariables(options.getTagSet()));
        }
        if (!options.isExcludeTaggingDirective()) {
            if (options.isReplaceTaggingDirective()) {
                headers.put(HttpConstants.HEADER_AMZ_TAGGING_DIRECTIVE, "REPLACE");
            } else {
                headers.put(HttpConstants.HEADER_AMZ_TAGGING_DIRECTIVE, "COPY");
            }
        }

        headers.put("x-amz-copy-source", HttpUtils.urlEncode(options.getCopySource(), true));
        if (StringUtils.isNotEmpty(options.getCopySourceVersionId())) {
            headers.put("x-amz-copy-source", headers.get("x-amz-copy-source") + "?versionId=" + options.getCopySourceVersionId());
        }

        headers.putAll(parseCopySourceConditionHeaders(options.getCopySourceCondition()));
        headers.putAll(parseUploadHeaders(options.getWriteTargetCondition()));

        String bodyHash = AWS4SignerBase.EMPTY_BODY_SHA256;
        headers.put(HttpConstants.HEADER_AMZ_CONTENT_HASH, bodyHash);

        URL url = new URL(buildUrl(bucket) + "/" + objectKey);

        AWS4SignerForAuthorizationHeader signer = new AWS4SignerForAuthorizationHeader(
                url, HttpConstants.HTTP_METHOD_PUT, HttpConstants.CONSTANT_AWS_SERVICENAME_S3, region);
        String authorization = signer.computeSignature(headers,
                null,
                bodyHash,
                accessKey,
                secretKey);

        headers.put(HttpConstants.HEADER_AUTHORIZATION, authorization);

        Request.Builder requestBuilder = new Request.Builder()
                .url(url)
                .put(RequestBody.create(new byte[0]));
        for (Map.Entry<String, String> header : headers.entrySet()) {
            requestBuilder.addHeader(header.getKey(), header.getValue());
        }
        Call call = okHttpClient.newCall(requestBuilder.build());
        call.timeout().timeout(timeout, TimeUnit.SECONDS);
        try (Response response = call.execute()) {
            if (!response.isSuccessful()) {
                S3ServerError serverError = parseErrorMessage(response);
                if (response.code() == 403 && "ObjectNotInActiveTierError".equals(serverError.getCode())) {
                    throw new CandyS3Exception(CommonErrorCode.COPY_SOURCE_NOT_IN_ACTIVE_TIRE.getCode(), serverError);
                } else if (response.code() == 412 && "PreconditionFailed".equals(serverError.getCode())) {
                    throw new CandyS3Exception(CommonErrorCode.OBJECT_PRECONDITION_FAILED.getCode(), serverError);
                } else {
                    throw new CandyS3Exception(CommonErrorCode.SERVER_ERROR.getCode(), serverError);
                }
            }

            JsonNode root = xmlMapper.readTree(response.body().string());
            return root.path("ETag").asText();
        }
    }

    /**
     * Get a presigned URL to upload or download an object.
     * @param bucket the bucket name.
     * @param objectKey the object key.
     * @param options the options to calculate presigned url.
     * @return A presigned URL with authorization query parameters, use it to do operations like upload or download.
     * @throws MalformedURLException if the bucket or objectKey is invalid.
     */
    public String calculatePresignedUrl(String bucket, String objectKey, PresignUrlOptions options) throws MalformedURLException {
        if (StringUtils.isEmpty(bucket)) {
            throw new IllegalArgumentException("bucket is required.");
        }
        if (StringUtils.isEmpty(objectKey)) {
            throw new IllegalArgumentException("objectKey is required.");
        }

        Map<String, String> params = new HashMap<>();
        params.put("X-Amz-Expires", options.getTtl() + "");

        URL url = new URL(buildUrl(bucket) + "/" + objectKey);

        AWS4SignerForQueryParameterAuth signer = new AWS4SignerForQueryParameterAuth(
                url, options.getMethod(), HttpConstants.CONSTANT_AWS_SERVICENAME_S3, region);
        String authorizationQueryParams = signer.computeSignature(new HashMap<>(),
                params,
                "UNSIGNED-PAYLOAD",
                accessKey,
                secretKey);

        return url + "?" + authorizationQueryParams;
    }

    /**
     * This action initiates a multipart upload and returns an upload ID.
     * @param bucket the bucket name.
     * @param objectKey the object key.
     * @param options the options to create multipart upload.
     * @return the upload ID of the multipart upload, use it to do operations like upload parts, or complete/abort the upload.
     * @throws IOException if an error occurs when send request.
     * @see <a href="https://docs.aws.amazon.com/AmazonS3/latest/API/API_CreateMultipartUpload.html">CreateMultipartUpload</a>
     */
    public String createMultipartUpload(String bucket, String objectKey, CreateMultipartUploadOptions options) throws IOException {
        if (StringUtils.isEmpty(bucket)) {
            throw new IllegalArgumentException("bucket is required.");
        }
        if (StringUtils.isEmpty(objectKey)) {
            throw new IllegalArgumentException("objectKey is required.");
        }

        Map<String, String> headers = new HashMap<>();
        if (options.getObjectHeaderOptions() != null) {
            headers.putAll(parseUploadHeaders(options.getObjectHeaderOptions()));
        }
        if (options.getStorageClass() != null) {
            headers.put(HttpConstants.HEADER_AMZ_STORAGE_CLASS, options.getStorageClass());
        }
        if (options.getTagSet() != null && !options.getTagSet().isEmpty()) {
            headers.put(HttpConstants.HEADER_AMZ_TAGGING, buildUrlKVVariables(options.getTagSet()));
        }
        if (options.getServerSideEncryptionOptions() != null) {
            headers.putAll(parseUploadHeaders(options.getServerSideEncryptionOptions()));
        }
        if (options.getObjectLockOptions() != null) {
            headers.putAll(parseUploadHeaders(options.getObjectLockOptions()));
        }

        return createMultipartUpload(bucket, objectKey, headers);
    }

    private String createMultipartUpload(String bucket, String objectKey, Map<String, String> includedHeaders) throws IOException {
        Map<String, String> params = new HashMap<>();
        params.put("uploads", "");

        String bodyHash = AWS4SignerBase.EMPTY_BODY_SHA256;
        includedHeaders.put(HttpConstants.HEADER_AMZ_CONTENT_HASH, bodyHash);

        URL url = new URL(buildUrl(bucket) + "/" + objectKey + buildParams(params));

        AWS4SignerForAuthorizationHeader signer = new AWS4SignerForAuthorizationHeader(
                url, HttpConstants.HTTP_METHOD_POST, HttpConstants.CONSTANT_AWS_SERVICENAME_S3, region);
        String authorization = signer.computeSignature(includedHeaders,
                params,
                bodyHash,
                accessKey,
                secretKey);

        includedHeaders.put(HttpConstants.HEADER_AUTHORIZATION, authorization);

        Request.Builder requestBuilder = new Request.Builder()
                .url(url)
                .post(RequestBody.create(new byte[0]));
        for (Map.Entry<String, String> header : includedHeaders.entrySet()) {
            requestBuilder.addHeader(header.getKey(), header.getValue());
        }
        Call call = okHttpClient.newCall(requestBuilder.build());
        call.timeout().timeout(timeout, TimeUnit.SECONDS);
        try (Response response = call.execute()) {
            if (!response.isSuccessful()) {
                throw new CandyS3Exception(CommonErrorCode.SERVER_ERROR.getCode(), parseErrorMessage(response));
            }

            JsonNode root = xmlMapper.readTree(response.body().string());
            return root.path("UploadId").asText("");
        }
    }

    /**
     * This operation aborts a multipart upload.
     * @param bucket the bucket name.
     * @param objectKey the object key.
     * @param uploadId the upload ID.
     * @param options the options to abort multipart upload.
     * @throws IOException if an error occurs when send request.
     * @see <a href="https://docs.aws.amazon.com/AmazonS3/latest/API/API_AbortMultipartUpload.html">AbortMultipartUpload</a>
     */
    public void abortMultipartUpload(String bucket, String objectKey, String uploadId, AbortMultipartUploadOptions options) throws IOException {
        if (StringUtils.isEmpty(bucket)) {
            throw new IllegalArgumentException("bucket is required.");
        }
        if (StringUtils.isEmpty(objectKey)) {
            throw new IllegalArgumentException("objectKey is required.");
        }

        Map<String, String> params = new HashMap<>();
        params.put("uploadId", uploadId);

        Map<String, String> headers = new HashMap<String, String>();

        headers.put(HttpConstants.HEADER_AMZ_CONTENT_HASH, AWS4SignerBase.EMPTY_BODY_SHA256);

        URL url = new URL(buildUrl(bucket) + "/" + objectKey + buildParams(params));

        AWS4SignerForAuthorizationHeader signer = new AWS4SignerForAuthorizationHeader(
                url, HttpConstants.HTTP_METHOD_DELETE, HttpConstants.CONSTANT_AWS_SERVICENAME_S3, region);
        String authorization = signer.computeSignature(headers,
                params,
                AWS4SignerBase.EMPTY_BODY_SHA256,
                accessKey,
                secretKey);

        headers.put(HttpConstants.HEADER_AUTHORIZATION, authorization);

        Request.Builder requestBuilder = new Request.Builder()
                .url(url)
                .delete();
        for (Map.Entry<String, String> header : headers.entrySet()) {
            requestBuilder.addHeader(header.getKey(), header.getValue());
        }
        Call call = okHttpClient.newCall(requestBuilder.build());
        call.timeout().timeout(timeout, TimeUnit.SECONDS);
        try (Response response = call.execute()) {
            if (!response.isSuccessful()) {
                throw new CandyS3Exception(CommonErrorCode.SERVER_ERROR.getCode(), parseErrorMessage(response));
            }
        }
    }

    /**
     * Completes a multipart upload by assembling previously uploaded parts.
     * @param bucket the bucket name.
     * @param objectKey the object key.
     * @param uploadId the upload ID.
     * @param s3Parts the list of S3Part.
     * @param options the options to complete multipart upload.
     * @return the eTag of the completed object.
     * @throws IOException if an error occurs when send request.
     * @see <a href="https://docs.aws.amazon.com/AmazonS3/latest/API/API_CompleteMultipartUpload.html">CompleteMultipartUpload</a>
     */
    public String completeMultipartUpload(String bucket, String objectKey, String uploadId, List<S3Part> s3Parts,
                                          CompleteMultipartUploadOptions options) throws IOException {
        if (StringUtils.isEmpty(bucket)) {
            throw new IllegalArgumentException("bucket is required.");
        }
        if (StringUtils.isEmpty(objectKey)) {
            throw new IllegalArgumentException("objectKey is required.");
        }

        Map<String, String> params = new HashMap<>();
        params.put("uploadId", uploadId);

        Map<String, String> headers = new HashMap<>();
        if (options.getCondition() != null) {
            headers.putAll(parseUploadHeaders(options.getCondition()));
        }

        CompleteMultipartUpload completeMultipartUpload = new CompleteMultipartUpload();
        completeMultipartUpload.part = new ArrayList<>();
        for (S3Part p : s3Parts) {
            CompleteMultipartUploadPart part = new CompleteMultipartUploadPart();
            part.partNumber = p.getPartNum();
            part.etag = p.getEtag();
            completeMultipartUpload.part.add(part);
        }

        String body = xmlMapper.writeValueAsString(completeMultipartUpload);
        String bodyHash = BinaryUtils.toHex(AWS4SignerBase.hash(body));
        headers.put(HttpConstants.HEADER_AMZ_CONTENT_HASH, bodyHash);

        URL url = new URL(buildUrl(bucket) + "/" + objectKey + buildParams(params));

        AWS4SignerForAuthorizationHeader signer = new AWS4SignerForAuthorizationHeader(
                url, HttpConstants.HTTP_METHOD_POST, HttpConstants.CONSTANT_AWS_SERVICENAME_S3, region);
        String authorization = signer.computeSignature(headers,
                params,
                bodyHash,
                accessKey,
                secretKey);

        headers.put(HttpConstants.HEADER_AUTHORIZATION, authorization);

        Request.Builder requestBuilder = new Request.Builder()
                .url(url)
                .post((RequestBody.create(body, MediaType.get("text/plain"))));
        for (Map.Entry<String, String> header : headers.entrySet()) {
            requestBuilder.addHeader(header.getKey(), header.getValue());
        }
        Call call = okHttpClient.newCall(requestBuilder.build());
        call.timeout().timeout(timeout, TimeUnit.SECONDS);
        try (Response response = call.execute()) {
            if (!response.isSuccessful()) {
                S3ServerError serverError = parseErrorMessage(response);
                if (response.code() == 412 && "PreconditionFailed".equals(serverError.getCode())) {
                    throw new CandyS3Exception(CommonErrorCode.OBJECT_PRECONDITION_FAILED.getCode(), serverError);
                } else if (response.code() == 409 && "ConditionalRequestConflict".equals(serverError.getCode())) {
                    throw new CandyS3Exception(CommonErrorCode.OBJECT_CONDITIONAL_REQUEST_CONFLICT.getCode(), serverError);
                } else if (response.code() == 400 && "EntityTooSmall".equals(serverError.getCode())) {
                    throw new CandyS3Exception(CommonErrorCode.MULTIPART_UPLOAD_ENTITY_TOO_SMALL.getCode(), serverError);
                } else if (response.code() == 404 && "NoSuchUpload".equals(serverError.getCode())) {
                    throw new CandyS3Exception(CommonErrorCode.MULTIPART_UPLOAD_NOT_EXISTS.getCode(), serverError);
                } else if (response.code() == 400 && "InvalidPart".equals(serverError.getCode())) {
                    throw new CandyS3Exception(CommonErrorCode.MULTIPART_UPLOAD_INVALID_PART.getCode(), serverError);
                } else if (response.code() == 400 && "InvalidPartOrder".equals(serverError.getCode())) {
                    throw new CandyS3Exception(CommonErrorCode.MULTIPART_UPLOAD_INVALID_PART_ORDER.getCode(), serverError);
                } else {
                    throw new CandyS3Exception(CommonErrorCode.SERVER_ERROR.getCode(), serverError);
                }
            }

            JsonNode root = xmlMapper.readTree(response.body().string());
            return root.path("ETag").asText("");
        }
    }

    /**
     * This operation lists in-progress multipart uploads in a bucket. An in-progress multipart upload is a multipart upload that has been initiated by the CreateMultipartUpload request, but has not yet been completed or aborted.
     * @param bucket the bucket name.
     * @param options the options to list multipart uploads.
     * @return the list pagination result of multipart uploads.
     * @throws IOException if an error occurs when send request.
     * @see <a href="https://docs.aws.amazon.com/AmazonS3/latest/API/API_ListMultipartUploads.html">ListMultipartUploads</a>
     */
    public ListPaginationResult<S3MultipartUpload> listMultipartUploads(String bucket, ListMultipartUploadOptions options) throws IOException {
        if (StringUtils.isEmpty(bucket)) {
            throw new IllegalArgumentException("bucket is required.");
        }

        Map<String, String> params = new HashMap<>();
        params.put("uploads", "");
        if (options.getDelimiter() != null && !StringUtils.isEmpty(options.getDelimiter().toString())) {
            params.put("delimiter", options.getDelimiter().toString());
        }
        if (options.getMaxUploads() != null) {
            params.put("max-uploads", options.getMaxUploads() + "");
        }
        if (!StringUtils.isEmpty(options.getPrefix())) {
            params.put("prefix", options.getPrefix());
        }
        if (!StringUtils.isEmpty(options.getKeyMarker())) {
            params.put("key-marker", options.getKeyMarker());
        }
        if (!StringUtils.isEmpty(options.getUploadIdMarker())) {
            params.put("upload-id-marker", options.getUploadIdMarker());
        }

        Map<String, String> headers = new HashMap<String, String>();
        headers.put(HttpConstants.HEADER_AMZ_CONTENT_HASH, AWS4SignerBase.EMPTY_BODY_SHA256);

        URL url = new URL(buildUrl(bucket) + "/" + buildParams(params));

        AWS4SignerForAuthorizationHeader signer = new AWS4SignerForAuthorizationHeader(
                url, HttpConstants.HTTP_METHOD_GET, HttpConstants.CONSTANT_AWS_SERVICENAME_S3, region);
        String authorization = signer.computeSignature(headers,
                params,
                AWS4SignerBase.EMPTY_BODY_SHA256,
                accessKey,
                secretKey);

        headers.put(HttpConstants.HEADER_AUTHORIZATION, authorization);

        Request.Builder requestBuilder = new Request.Builder()
                .url(url)
                .get();
        for (Map.Entry<String, String> header : headers.entrySet()) {
            requestBuilder.addHeader(header.getKey(), header.getValue());
        }
        Call call = okHttpClient.newCall(requestBuilder.build());
        call.timeout().timeout(timeout, TimeUnit.SECONDS);
        try (Response response = call.execute()) {
            if (!response.isSuccessful()) {
                S3ServerError serverError = parseErrorMessage(response);
                throw new CandyS3Exception(CommonErrorCode.SERVER_ERROR.getCode(), serverError);
            }

            JsonNode root = xmlMapper.readTree(response.body().string());

            JsonNode uploadsNode = root.path("Upload");
            List<S3MultipartUpload> s3MultipartUploads = new ArrayList<>(0);
            if (!uploadsNode.isMissingNode()) {
                // jackson cant recognize node as an array when it has only one child.
                if (uploadsNode.isArray()) {
                    s3MultipartUploads = xmlMapper.treeToValue(uploadsNode,
                            xmlMapper.getTypeFactory().constructCollectionType(List.class, S3MultipartUpload.class));
                } else {
                    s3MultipartUploads = new ArrayList<>(1);
                    s3MultipartUploads.add(xmlMapper.treeToValue(uploadsNode, S3MultipartUpload.class));
                }
            }

            ListPaginationResult<S3MultipartUpload> paginationResult =
                    new ListPaginationResult<>(s3MultipartUploads,
                            root.path("NextKeyMarker").asText(""),
                            root.path("NextUploadIdMarker").asText(""));

            JsonNode commonPrefixesNode = root.path("CommonPrefixes");
            List<ListPaginationResult.CommonPrefix> commonPrefixes = new ArrayList<>();
            if (!commonPrefixesNode.isMissingNode()) {
                if (commonPrefixesNode.isArray()) {
                    commonPrefixes = xmlMapper.treeToValue(commonPrefixesNode,
                            xmlMapper.getTypeFactory().constructCollectionType(List.class, ListPaginationResult.CommonPrefix.class));
                } else {
                    commonPrefixes.add(xmlMapper.treeToValue(commonPrefixesNode, ListPaginationResult.CommonPrefix.class));
                }
            }
            paginationResult.setCommonPrefixes(commonPrefixes);

            return paginationResult;
        }
    }

    /**
     * Uploads a part in a multipart upload.
     * S3 part size can be 5MB to 5GB. Should use a reasonable size when call this part because it read all data into memory directly.
     *
     * @param bucket the bucket name.
     * @param objectKey the object key.
     * @param uploadId the upload ID.
     * @param partNum  Part number of part being uploaded. This is a positive integer between 1 and 10,000.
     * @param options the options to upload part.
     * @return the S3Part.
     * @throws IOException if an error occurs when send request.
     * @throws NoSuchAlgorithmException if error occurs when compute signature.
     * @see <a href="https://docs.aws.amazon.com/AmazonS3/latest/API/API_UploadPart.html">UploadPart</a>
     */
    public S3Part uploadPart(String bucket, String objectKey,
                             String uploadId, int partNum,
                             UploadPartOptions options) throws IOException, NoSuchAlgorithmException {

        if (StringUtils.isEmpty(bucket)) {
            throw new IllegalArgumentException("bucket is required.");
        }
        if (StringUtils.isEmpty(objectKey)) {
            throw new IllegalArgumentException("objectKey is required.");
        }
        if (partNum < 1 || partNum > 10000) {
            throw new IllegalArgumentException("Part number must be an integer between 1 and 10000, inclusive.");
        }

        checkDataInput(options.getDataContentOptions());

        String resultEtag = null;

        if (ArrayUtils.isNotEmpty(options.getInputBytes())) {
            resultEtag = this.doUpload(bucket, objectKey, null, uploadId, partNum,
                    options.getInputBytes());
        } else {
            BufferedInputStream bin;
            long len;
            if (options.getInputStream() != null) {
                bin = new BufferedInputStream(options.getInputStream());
                if (options.getInputStream() instanceof FileInputStream) {
                    len = ((FileInputStream) options.getInputStream()).getChannel().size();
                } else {
                    // available may be not accurate when used as length.
                    len = options.getInputStream().available();
                }
            } else {
                File file = new File(options.getInputFile());
                bin = new BufferedInputStream(new FileInputStream(file));
                len = file.length();
            }

            ByteArrayOutputStream bout = new ByteArrayOutputStream();
            byte[] bytes = new byte[(int) len];
            int l = -1;
            while ((l = bin.read(bytes)) != -1) {
                bout.write(bytes, 0, l);
            }
            bin.close();

            resultEtag = this.doUpload(bucket, objectKey, null, uploadId, partNum,
                    bout.toByteArray());
            bout.close();
        }

        S3Part part = new S3Part();
        part.setPartNum(partNum);
        part.setEtag(resultEtag);
        return part;
    }

    /**
     * Uploads a part by copying data from an existing object as data source.
     *
     * @param bucket the bucket name.
     * @param objectKey the object key.
     * @param uploadId the upload ID.
     * @param partNum  Part number of part being copied. This is a positive integer between 1 and 10,000.
     * @param options the options to copy part.
     * @return the eTag of the copied part.
     * @throws IOException if an error occurs when send request.
     * @see <a href="https://docs.aws.amazon.com/AmazonS3/latest/API/API_UploadPartCopy.html">UploadPartCopy</a>
     */
    public String copyPart(String bucket, String objectKey, String uploadId, int partNum, CopyPartOptions options) throws IOException {
        if (StringUtils.isEmpty(bucket)) {
            throw new IllegalArgumentException("bucket is required.");
        }
        if (StringUtils.isEmpty(objectKey)) {
            throw new IllegalArgumentException("objectKey is required.");
        }

        Map<String, String> headers = parseCopySourceConditionHeaders(options.getCopySourceCondition());
        headers.put("x-amz-copy-source", HttpUtils.urlEncode(options.getCopySource(), true));
        if (StringUtils.isNotEmpty(options.getCopySourceVersionId())) {
            headers.put("x-amz-copy-source", headers.get("x-amz-copy-source") + "?versionId=" + options.getCopySourceVersionId());
        }
        if (!StringUtils.isEmpty(options.getCopySourceRange())) {
            headers.put("x-amz-copy-source-range", "bytes=" + options.getCopySourceRange());
        }

        Map<String, String> params = new HashMap<>();
        params.put("uploadId", uploadId);
        params.put("partNumber", partNum + "");

        String bodyHash = AWS4SignerBase.EMPTY_BODY_SHA256;
        headers.put(HttpConstants.HEADER_AMZ_CONTENT_HASH, bodyHash);

        URL url = new URL(buildUrl(bucket) + "/" + objectKey + buildParams(params));

        AWS4SignerForAuthorizationHeader signer = new AWS4SignerForAuthorizationHeader(
                url, HttpConstants.HTTP_METHOD_PUT, HttpConstants.CONSTANT_AWS_SERVICENAME_S3, region);
        String authorization = signer.computeSignature(headers,
                params,
                bodyHash,
                accessKey,
                secretKey);

        headers.put(HttpConstants.HEADER_AUTHORIZATION, authorization);

        Request.Builder requestBuilder = new Request.Builder()
                .url(url)
                .put(RequestBody.create(new byte[0]));
        for (Map.Entry<String, String> header : headers.entrySet()) {
            requestBuilder.addHeader(header.getKey(), header.getValue());
        }
        Call call = okHttpClient.newCall(requestBuilder.build());
        call.timeout().timeout(timeout, TimeUnit.SECONDS);
        try (Response response = call.execute()) {
            if (!response.isSuccessful()) {
                S3ServerError serverError = parseErrorMessage(response);
                if (response.code() == 404 && "NoSuchUpload".equals(serverError.getCode())) {
                    throw new CandyS3Exception(CommonErrorCode.MULTIPART_UPLOAD_NOT_EXISTS.getCode(), serverError);
                } else if (response.code() == 400 && "InvalidRequest".equals(serverError.getCode())) {
                    throw new CandyS3Exception(CommonErrorCode.COPY_SOURCE_RANGE_INVALID.getCode(), serverError);
                } else if (response.code() == 412 && "PreconditionFailed".equals(serverError.getCode())) {
                    throw new CandyS3Exception(CommonErrorCode.OBJECT_PRECONDITION_FAILED.getCode(), serverError);
                } else {
                    throw new CandyS3Exception(CommonErrorCode.SERVER_ERROR.getCode(), serverError);
                }
            }

            JsonNode root = xmlMapper.readTree(response.body().string());
            return root.path("ETag").asText();
        }
    }

    /**
     * Lists the parts that have been uploaded for a specific multipart upload.
     *
     * @param bucket the bucket name.
     * @param objectKey the object key.
     * @param options the options to list parts.
     * @return the list pagination result of parts.
     * @throws IOException if an error occurs when send request.
     * @see <a href="https://docs.aws.amazon.com/AmazonS3/latest/API/API_ListParts.html">ListParts</a>
     */
    public ListPaginationResult<S3Part> listParts(String bucket, String objectKey, ListPartsOptions options) throws IOException {
        if (StringUtils.isEmpty(bucket)) {
            throw new IllegalArgumentException("bucket is required.");
        }
        if (StringUtils.isEmpty(objectKey)) {
            throw new IllegalArgumentException("objectKey is required.");
        }
        if (StringUtils.isEmpty(options.getUploadId())) {
            throw new IllegalArgumentException("uploadId is required.");
        }

        Map<String, String> params = new HashMap<>();
        params.put("uploadId", options.getUploadId());

        if (options.getMaxParts() != null) {
            params.put("max-parts", options.getMaxParts() + "");
        }
        if (options.getStartAfterPartNumber() != null) {
            params.put("part-number-marker", options.getStartAfterPartNumber() + "");
        }

        Map<String, String> headers = new HashMap<String, String>();
        headers.put(HttpConstants.HEADER_AMZ_CONTENT_HASH, AWS4SignerBase.EMPTY_BODY_SHA256);

        URL url = new URL(buildUrl(bucket) + "/" + objectKey + buildParams(params));

        AWS4SignerForAuthorizationHeader signer = new AWS4SignerForAuthorizationHeader(
                url, HttpConstants.HTTP_METHOD_GET, HttpConstants.CONSTANT_AWS_SERVICENAME_S3, region);
        String authorization = signer.computeSignature(headers,
                params,
                AWS4SignerBase.EMPTY_BODY_SHA256,
                accessKey,
                secretKey);

        headers.put(HttpConstants.HEADER_AUTHORIZATION, authorization);

        Request.Builder requestBuilder = new Request.Builder()
                .url(url)
                .get();
        for (Map.Entry<String, String> header : headers.entrySet()) {
            requestBuilder.addHeader(header.getKey(), header.getValue());
        }
        Call call = okHttpClient.newCall(requestBuilder.build());
        call.timeout().timeout(timeout, TimeUnit.SECONDS);
        try (Response response = call.execute()) {
            if (!response.isSuccessful()) {
                S3ServerError serverError = parseErrorMessage(response);
                if (response.code() == 404) {
                    throw new CandyS3Exception(CommonErrorCode.NO_SUCH_BUCKET.getCode(), serverError);
                }
                throw new CandyS3Exception(CommonErrorCode.SERVER_ERROR.getCode(), serverError);
            }

            JsonNode root = xmlMapper.readTree(response.body().string());

            JsonNode partsNode = root.path("Part");
            List<S3Part> s3Parts = new ArrayList<>(0);
            if (!partsNode.isMissingNode()) {
                // jackson cant recognize node as an array when it has only one child.
                if (partsNode.isArray()) {
                    s3Parts = xmlMapper.treeToValue(partsNode,
                            xmlMapper.getTypeFactory().constructCollectionType(List.class, S3Part.class));
                } else {
                    s3Parts = new ArrayList<>(1);
                    s3Parts.add(xmlMapper.treeToValue(partsNode, S3Part.class));
                }
            }

            JsonNode ownerNode = root.path("Owner");
            Owner owner = null;
            if (!ownerNode.isMissingNode()) {
                owner = xmlMapper.treeToValue(ownerNode, Owner.class);
            }
            JsonNode initiatorNode = root.path("Initiator");
            Owner initiator = null;
            if (!initiatorNode.isMissingNode()) {
                initiator = xmlMapper.treeToValue(initiatorNode, Owner.class);
            }
            for (S3Part part : s3Parts) {
                part.setOwner(owner);
                part.setInitiator(initiator);
            }

            boolean isTruncated = root.path("IsTruncated").asBoolean(false);
            String nextPartNumberMarker = "";
            if (isTruncated) {
                nextPartNumberMarker = root.path("NextPartNumberMarker").asText();
            }
            return new ListPaginationResult<>(s3Parts, nextPartNumberMarker);
        }
    }


    private void checkDataInput(ObjectDataContentOptions dataInput) {
        if (dataInput != null
                && dataInput.getInputBytes() == null
                && dataInput.getInputStream() == null
                && StringUtils.isEmpty(dataInput.getInputFile())) {
            throw new IllegalArgumentException("No input data bytes or inputStream or file configured.");
        }
        if (dataInput != null
                && dataInput.getInputBytes() == null
                && dataInput.getInputStream() == null
                && StringUtils.isNotEmpty(dataInput.getInputFile())) {
            // When use file as input, check if it exists.
            if (!new File(dataInput.getInputFile()).exists()) {
                throw new CandyS3Exception(CommonErrorCode.INPUT_FILE_NOT_EXISTS.getCode(),
                        CommonErrorCode.INPUT_FILE_NOT_EXISTS.getMsg());
            }
        }
    }

    private Map<String, String> parseUploadHeaders(PutObjectHeaderOptions objectHeaderOptions) {
        Map<String, String> parsedHeaders = new HashMap<>();
        if (objectHeaderOptions != null) {
            if (StringUtils.isNotEmpty(objectHeaderOptions.getCacheControl())) {
                parsedHeaders.put("Cache-Control", objectHeaderOptions.getCacheControl());
            }
            if (StringUtils.isNotEmpty(objectHeaderOptions.getContentDisposition())) {
                parsedHeaders.put("Content-Disposition", objectHeaderOptions.getContentDisposition());
            }
            if (StringUtils.isNotEmpty(objectHeaderOptions.getContentEncoding())) {
                parsedHeaders.put("Content-Encoding", objectHeaderOptions.getContentEncoding());
            }
            if (StringUtils.isNotEmpty(objectHeaderOptions.getContentLanguage())) {
                parsedHeaders.put("Content-Language", objectHeaderOptions.getContentLanguage());
            }
            if (StringUtils.isNotEmpty(objectHeaderOptions.getContentType())) {
                parsedHeaders.put("Content-Type", objectHeaderOptions.getContentType());
            }
            if (objectHeaderOptions.getExpires() != null) {
                parsedHeaders.put("Expires", formatHttpDate(objectHeaderOptions.getExpires()));
            }
        }

        return parsedHeaders;
    }

    private Map<String, String> parseUploadHeaders(ObjectConditionalWriteOptions condition) {
        Map<String, String> parsedHeaders = new HashMap<>();
        if (condition != null) {
            if (StringUtils.isNotEmpty(condition.getIfMatch())) {
                parsedHeaders.put(HttpConstants.HEADER_IF_MATCH, condition.getIfMatch());
            }
            if (StringUtils.isNotEmpty(condition.getIfNoneMatch())) {
                parsedHeaders.put("If-None-Match", condition.getIfNoneMatch());
            }
        }

        return parsedHeaders;
    }

    private Map<String, String> parseDownloadHeaders(DownloadObjectOptions.DownloadObjectCondition condition) {
        Map<String, String> parsedHeaders = new HashMap<>();
        if (condition != null) {
            if (StringUtils.isNotEmpty(condition.getIfMatch())) {
                parsedHeaders.put(HttpConstants.HEADER_IF_MATCH, condition.getIfMatch());
            }
            if (StringUtils.isNotEmpty(condition.getIfNoneMatch())) {
                parsedHeaders.put("If-None-Match", condition.getIfNoneMatch());
            }
            if (condition.getIfModifiedSince() != null) {
                parsedHeaders.put("If-Modified-Since", formatHttpDate(condition.getIfModifiedSince()));
            }
            if (condition.getIfUnmodifiedSince() != null) {
                parsedHeaders.put("If-Unmodified-Since", formatHttpDate(condition.getIfUnmodifiedSince()));
            }
        }

        return parsedHeaders;
    }

    private Map<String, String> parseCopySourceConditionHeaders(DownloadObjectOptions.DownloadObjectCondition condition) {
        Map<String, String> parsedHeaders = new HashMap<>();
        if (condition != null) {
            if (StringUtils.isNotEmpty(condition.getIfMatch())) {
                parsedHeaders.put(HttpConstants.HEADER_AMZ_COPY_SOURCE_IF_MATCH, condition.getIfMatch());
            }
            if (StringUtils.isNotEmpty(condition.getIfNoneMatch())) {
                parsedHeaders.put(HttpConstants.HEADER_AMZ_COPY_SOURCE_IF_NONE_MATCH, condition.getIfNoneMatch());
            }
            if (condition.getIfModifiedSince() != null) {
                parsedHeaders.put(HttpConstants.HEADER_AMZ_COPY_SOURCE_IF_MODIFIED_SINCE, formatHttpDate(condition.getIfModifiedSince()));
            }
            if (condition.getIfUnmodifiedSince() != null) {
                parsedHeaders.put(HttpConstants.HEADER_AMZ_COPY_SOURCE_IF_UNMODIFIED_SINCE, formatHttpDate(condition.getIfUnmodifiedSince()));
            }
        }

        return parsedHeaders;
    }

    private Map<String, String> parseUploadHeaders(ObjectLockOptions objectLockOptions) {
        Map<String, String> parsedHeaders = new HashMap<>();
        if (objectLockOptions != null) {
            if (objectLockOptions.getObjectLockMode() != null) {
                parsedHeaders.put(HttpConstants.HEADER_AMZ_OBJECT_LOCK_MODE, objectLockOptions.getObjectLockMode().name());
            }
            if (objectLockOptions.getObjectLockRetainUntilDate() != null) {
                parsedHeaders.put(HttpConstants.HEADER_AMZ_OBJECT_LOCK_RETAIN_UNTIL_DATE,
                        formatISO8601Date(objectLockOptions.getObjectLockRetainUntilDate()));
            }
            if (objectLockOptions.isObjectLockLegalHold()) {
                parsedHeaders.put(HttpConstants.HEADER_AMZ_OBJECT_LEGAL_HOLD, "ON");
            } else {
                parsedHeaders.put(HttpConstants.HEADER_AMZ_OBJECT_LEGAL_HOLD, "OFF");
            }
        }
        return parsedHeaders;
    }

    private Map<String, String> parseUploadHeaders(UpdateServerSideEncryptionOptions sseOptions) {
        Map<String, String> parsedHeaders = new HashMap<>();
        if (sseOptions != null) {
//            if (sseOptions.isEnableBucketKey()) {
//                parsedHeaders.put(HttpConstants.HEADER_AMZ_SSE_BUCKET_KEY_ENABLED, true + "");
//            } else {
//                parsedHeaders.put(HttpConstants.HEADER_AMZ_SSE_BUCKET_KEY_ENABLED, false + "");
//            }
            if (sseOptions.getSseAlgorithm() != null) {
                parsedHeaders.put(HttpConstants.HEADER_AMZ_SSE_ALGORITHM, sseOptions.getSseAlgorithm());
            }
        }
        return parsedHeaders;
    }

    private S3Object.S3ObjectMetadata parseObjectMetadataFromResponseHeaders(Headers responseHeaders) {
        S3Object.S3ObjectMetadata objectMetadata = new S3Object.S3ObjectMetadata();
        objectMetadata.setCacheControl(responseHeaders.get("Cache-Control"));
        objectMetadata.setContentDisposition(responseHeaders.get("Content-Disposition"));
        objectMetadata.setContentEncoding(responseHeaders.get("Content-Encoding"));
        objectMetadata.setContentLanguage(responseHeaders.get("Content-Language"));
        objectMetadata.setContentRange(responseHeaders.get("Content-Range"));
        objectMetadata.setContentType(responseHeaders.get("Content-Type"));
        objectMetadata.setExpires(responseHeaders.getDate("Expires"));
        return objectMetadata;
    }

    private void parseObjectLockPropertiesFromResponseHeaders(S3Object s3Object, Headers responseHeaders) {
        ObjectLockProperties objectLockProperties = null;
        if (responseHeaders.names().contains("x-amz-object-lock-mode")) {
            if (objectLockProperties == null) {
                objectLockProperties = new ObjectLockProperties();
            }
            objectLockProperties.setObjectLockMode(ObjectRetentionMode.valueOf(responseHeaders.get("x-amz-object-lock-mode")));
        }
        if (responseHeaders.names().contains("x-amz-object-lock-retain-until-date")) {
            if (objectLockProperties == null) {
                objectLockProperties = new ObjectLockProperties();
            }
            SimpleDateFormat simpleDateFormatSec = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss'Z'");
            simpleDateFormatSec.setTimeZone(new SimpleTimeZone(0, "UTC"));
            try {
                objectLockProperties.setObjectLockRetainUntilDate(simpleDateFormatSec.parse(responseHeaders.get("x-amz-object-lock-retain-until-date")));
            } catch (ParseException e) {
                ParseException finalParseException = e;
                SimpleDateFormat simpleDateFormatMill = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSS'Z'");
                simpleDateFormatMill.setTimeZone(new SimpleTimeZone(0, "UTC"));
                try {
                    objectLockProperties.setObjectLockRetainUntilDate(simpleDateFormatMill.parse(responseHeaders.get("x-amz-object-lock-retain-until-date")));
                    finalParseException = null;
                } catch (ParseException ex) {
                    finalParseException = ex;
                    SimpleDateFormat simpleDateFormatHttpDate = new SimpleDateFormat("EEE, dd MMM yyyy HH:mm:ss zzz", Locale.ENGLISH);
                    try {
                        objectLockProperties.setObjectLockRetainUntilDate(simpleDateFormatHttpDate.parse(responseHeaders.get("x-amz-object-lock-retain-until-date")));
                        finalParseException = null;
                    } catch (ParseException ex2) {
                        finalParseException = ex2;
                    }
                }

                if (finalParseException != null) {
                    throw new CandyS3Exception(CommonErrorCode.SERVER_ERROR.getCode(), e.getMessage());
                }
            }
        }
        if (responseHeaders.names().contains("x-amz-object-lock-legal-hold")) {
            if (objectLockProperties == null) {
                objectLockProperties = new ObjectLockProperties();
            }
            objectLockProperties.setObjectLockLegalHold("ON".equals(responseHeaders.get("x-amz-object-lock-legal-hold")));
        }
        s3Object.setObjectLockConfiguration(objectLockProperties);
    }

    private void parseObjectSseConfigurationFromResponseHeaders(S3Object s3Object, Headers responseHeaders) {
        ServerSideEncryptionProperties sseProperties = new ServerSideEncryptionProperties();
        if (responseHeaders.names().contains("x-amz-server-side-encryption-bucket-key-enabled")) {
            if (sseProperties == null) {
                sseProperties = new ServerSideEncryptionProperties();
            }
        }
        if (responseHeaders.names().contains("x-amz-server-side-encryption")) {
            if (sseProperties == null) {
                sseProperties = new ServerSideEncryptionProperties();
            }
            sseProperties.setSseAlgorithm(responseHeaders.get("x-amz-server-side-encryption"));
        }

        s3Object.setServerSideEncryptionConfiguration(sseProperties);
    }

    @JsonInclude(value = JsonInclude.Include.NON_EMPTY)
    private static class CreateBucketConfiguration {
        @JacksonXmlProperty(isAttribute = true)
        private String xmlns = "http://s3.amazonaws.com/doc/2006-03-01/"; // for serialize

        @JacksonXmlProperty(localName = "LocationConstraint")
        private String locationConstraint;

    }

    @JsonInclude(value = JsonInclude.Include.NON_EMPTY)
    private static class VersioningConfiguration {
        @JacksonXmlProperty(isAttribute = true)
        private String xmlns = "http://s3.amazonaws.com/doc/2006-03-01/"; // for serialize

        @JacksonXmlProperty(localName = "Status")
        private String status;

    }

    @JsonInclude(value = JsonInclude.Include.NON_EMPTY)
    private static class AccelerateConfiguration {
        @JacksonXmlProperty(isAttribute = true)
        private String xmlns = "http://s3.amazonaws.com/doc/2006-03-01/"; // for serialize

        @JacksonXmlProperty(localName = "Status")
        private String status;

    }

    @JsonInclude(value = JsonInclude.Include.NON_EMPTY)
    private static class ObjectLockConfiguration {

        @JacksonXmlProperty(isAttribute = true)
        private final String xmlns = "http://s3.amazonaws.com/doc/2006-03-01/"; // for serialize

        @JacksonXmlProperty(localName = "ObjectLockEnabled")
        String objectLockEnabled;

        @JacksonXmlProperty(localName = "Rule")
        ObjectLockConfigurationLockRule lockRule;

    }

    @JsonIgnoreProperties(ignoreUnknown = true)
    @JsonInclude(value = JsonInclude.Include.NON_EMPTY)
    private static class ObjectLockConfigurationLockRule {
        @JacksonXmlProperty(localName = "DefaultRetention")
        ObjectLockConfigurationObjectRetention retention;
    }

    @JsonIgnoreProperties(ignoreUnknown = true)
    @JsonInclude(value = JsonInclude.Include.NON_EMPTY)
    private static class ObjectLockConfigurationObjectRetention {
        @JacksonXmlProperty(localName = "Mode")
        String mode;
        @JacksonXmlProperty(localName = "Years")
        Integer years;
        @JacksonXmlProperty(localName = "Days")
        Integer days;

    }

    @JsonInclude(value = JsonInclude.Include.NON_EMPTY)
    private static class Retention {
        @JacksonXmlProperty(isAttribute = true)
        private final String xmlns = "http://s3.amazonaws.com/doc/2006-03-01/"; // for serialize

        @JacksonXmlProperty(localName = "Mode")
        String mode;

        @JacksonXmlProperty(localName = "RetainUntilDate")
        String retainUntilDate;
    }

    @JsonInclude(value = JsonInclude.Include.NON_EMPTY)
    private static class LegalHold {
        @JacksonXmlProperty(isAttribute = true)
        private final String xmlns = "http://s3.amazonaws.com/doc/2006-03-01/"; // for serialize

        @JacksonXmlProperty(localName = "Status")
        String status;
    }

    @JsonInclude(value = JsonInclude.Include.NON_EMPTY)
    private static class PublicAccessBlockConfiguration {

        @JacksonXmlProperty(isAttribute = true)
        private final String xmlns = "http://s3.amazonaws.com/doc/2006-03-01/"; // for serialize

        @JacksonXmlProperty(localName = "BlockPublicAcls")
        private boolean blockPublicAcls;
        @JacksonXmlProperty(localName = "BlockPublicPolicy")
        private boolean blockPublicPolicy;
        @JacksonXmlProperty(localName = "IgnorePublicAcls")
        private boolean ignorePublicAcls;
        @JacksonXmlProperty(localName = "RestrictPublicBuckets")
        private boolean restrictPublicBuckets;
    }

    @JsonInclude(value = JsonInclude.Include.NON_EMPTY)
    private static class Tagging {
        @JacksonXmlProperty(isAttribute = true)
        private final String xmlns = "http://s3.amazonaws.com/doc/2006-03-01/"; // for serialize

        @JacksonXmlProperty(localName = "TagSet")
        private TagSet tagSet;
    }

    @JsonInclude(value = JsonInclude.Include.NON_EMPTY)
    private static class TagSet {
        @JacksonXmlProperty(localName = "Tag")
        @JacksonXmlElementWrapper(useWrapping = false)
        private List<Tag> tag;
    }

    private static class Tag {
        @JacksonXmlProperty(localName = "Key")
        private String key;
        @JacksonXmlProperty(localName = "Value")
        private String value; // value can be null

        public Tag(String key, String value) {
            this.key = key;
            this.value = value;
        }
    }

    @JsonInclude(value = JsonInclude.Include.NON_EMPTY)
    private static class ServerSideEncryptionConfiguration {
        @JacksonXmlProperty(isAttribute = true)
        private final String xmlns = "http://s3.amazonaws.com/doc/2006-03-01/"; // for serialize

        @JacksonXmlProperty(localName = "Rule")
        private ServerSideEncryptionRule rule;
    }

    @JsonInclude(value = JsonInclude.Include.NON_EMPTY)
    private static class ServerSideEncryptionRule {

//        // Not support bucket key now
//        @JacksonXmlProperty(localName = "BucketKeyEnabled")
//        private Boolean bucketKeyEnabled;

        @JacksonXmlProperty(localName = "ApplyServerSideEncryptionByDefault")
        private ServerSideEncryptionRuleApplyConfig applyConfig;
    }

    @JsonInclude(value = JsonInclude.Include.NON_EMPTY)
    private static class ServerSideEncryptionRuleApplyConfig {
        @JacksonXmlProperty(localName = "SSEAlgorithm")
        private String sseAlgorithm;
    }

    @JsonInclude(value = JsonInclude.Include.NON_EMPTY)
    private static class CompleteMultipartUpload {
        @JacksonXmlProperty(localName = "Part")
        @JacksonXmlElementWrapper(useWrapping = false)
        private List<CompleteMultipartUploadPart> part;
    }

    @JsonInclude(value = JsonInclude.Include.NON_EMPTY)
    private static class CompleteMultipartUploadPart {
        @JacksonXmlProperty(localName = "PartNumber")
        private Integer partNumber;
        @JacksonXmlProperty(localName = "ETag")
        private String etag;

        @JacksonXmlProperty(localName = "ChecksumCRC32")
        private String checksumCRC32;
        @JacksonXmlProperty(localName = "ChecksumCRC32C")
        private String checksumCRC32C;
        @JacksonXmlProperty(localName = "ChecksumSHA1")
        private String checksumSHA1;
        @JacksonXmlProperty(localName = "ChecksumSHA256")
        private String checksumSHA256;
    }

    @JsonInclude(value = JsonInclude.Include.NON_EMPTY)
    private static class Delete {
        @JacksonXmlProperty(isAttribute = true)
        private final String xmlns = "http://s3.amazonaws.com/doc/2006-03-01/"; // for serialize

        @JacksonXmlProperty(localName = "Object")
        @JacksonXmlElementWrapper(useWrapping = false)
        private List<DeleteObject> deleteObjects;
    }

    @JsonInclude(value = JsonInclude.Include.NON_EMPTY)
    private static class DeleteObject {
        @JacksonXmlProperty(localName = "Key")
        private String key;

        @JacksonXmlProperty(localName = "VersionId")
        private String versionId;

        @JacksonXmlProperty(localName = "ETag")
        private String eTag;
    }

    private String buildParams(Map<String, String> params) {
        if (params.isEmpty()) {
            return "";
        }
        return "?" + buildUrlKVVariables(params);
    }

    private String buildUrlKVVariables(Map<String, String> params) {
        StringBuilder builder = new StringBuilder();
        for (Map.Entry<String, String> entry : params.entrySet()) {
            builder.append(entry.getKey());
            if (!StringUtils.isEmpty(entry.getValue())) {
                builder.append("=").append(HttpUtils.urlEncode(entry.getValue(), false));
            }
            builder.append("&");
        }

        builder.deleteCharAt(builder.length() - 1);
        return builder.toString();
    }


    private S3ServerError parseErrorMessage(Response response) throws IOException {
        ResponseBody body = response.body();
        String bodyStr = "";
        S3ServerError serverError;
        if (body != null) {
            bodyStr = body.string();
            try {
                JsonNode root = xmlMapper.readTree(bodyStr);
                serverError = xmlMapper.treeToValue(root, S3ServerError.class);
            } catch (Exception ex) {
                serverError = new S3ServerError();
                serverError.setMessage(StringUtils.isBlank(bodyStr) ? response.message() : bodyStr);
            }
        } else {
            serverError = new S3ServerError();
        }
        return serverError;
    }

    private static String formatHttpDate(Date date) {
        SimpleDateFormat dateFormat = new SimpleDateFormat("EEE, dd MMM yyyy HH:mm:ss zzz", Locale.ENGLISH);
        dateFormat.setTimeZone(TimeZone.getTimeZone("GMT"));
        return dateFormat.format(date);
    }

    private static String formatISO8601Date(Date date) {
        SimpleDateFormat dateTimeFormat = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss'Z'");
        dateTimeFormat.setTimeZone(new SimpleTimeZone(0, "UTC"));
        return dateTimeFormat.format(date);
    }

}
