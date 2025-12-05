# Candy-S3

[![License](https://img.shields.io/badge/license-Apache%202-blue.svg)](LICENSE)

[English Version](README.md) | [中文版本](README_zh.md)

Candy-S3 is a unified SDK for interacting with S3-compatible object storage services from multiple cloud service
providers. It provides a set of simple and easy-to-use APIs that shield the differences between different cloud service
providers, allowing developers to easily switch between multiple cloud platforms and manage object storage resources.

## Features

### Multi-cloud Support

- **Amazon S3**: Full support for all core features of AWS S3
- **Cloudflare R2**: Compatible with Cloudflare R2 service
- **Alibaba Cloud OSS**: Compatible with Alibaba Cloud OSS service
- **Tencent Cloud COS**: Supports Tencent Cloud COS service
- **Custom S3**: Supports any S3 protocol-compatible object storage service

### Core Features

- Bucket management (create, delete, list)
- Object management (upload, download, delete, copy)
- Multipart upload support
- Presigned URLs (for uploading and downloading objects and other operations)
- Batch operation support

The following advanced features are also supported in Candy-S3 and have been extensively tested and verified for AWS S3,
but the level of support varies across other cloud providers' S3 services:

- Object versioning
- Conditional writes (such as: If-Match, If-None-Match, If-Modified-Since, If-Unmodified-Since)
- Tag management
- Object locking and compliance retention
- Server-side encryption

### Technical Advantages

- Unified API interface, no need to write different code for different cloud providers
- High-performance HTTP client based on OkHttp
- Complete error handling mechanism
- Supports most S3 standard features
- Easy to integrate and extend
- Rich unit tests covering core functions and exception scenarios, ensuring code quality and stability while serving as
  usage references and examples

## Quick Start

### Requirements

- Java 8 or higher
- Maven 3.6+ (for building the project)

### Add Dependency

Add the following dependency to your `pom.xml`:

```xml

<dependency>
    <groupId>io.github.matian2014</groupId>
    <artifactId>candy-s3</artifactId>
    <version>[latest version]</version>
</dependency>
```

> Candy-S3's code is simple enough, with core functionality in a single class file. You can also directly integrate the
> source code into your project and customize and extend it as needed.

### Basic Usage

We provide comprehensive unit test code in `src/test/java/io/github/matian2014/candys3/CandyS3Test.java` to help you quickly
understand and use various features of Candy-S3.

#### Initialize Client

```java
// Create AWS S3 client
CandyS3 client = new CandyS3(S3Provider.AWS);
client.setAccessKey("your-access-key");
client.setSecretKey("your-secret-key");
client.setRegion("us-east-1");
```

> Some cloud providers' S3 services require additional parameters.
> For example, Cloudflare R2 requires specifying `account-id`, which can be set through the `setCloudflareR2AccountId`
> method when initializing the client.
> You can refer to the example in the unit test code `src/test/java/io/github/matian2014/candys3/CandyS3Test.java` to understand
> how to use Candy-S3 with different cloud providers' S3 services.

#### Bucket Operations

```java
// Create bucket
CreateBucketOptions options = new CreateBucketOptions.CreateBucketOptionsBuilder("my-bucket").build();
client.createBucket(options);

// List buckets
List<Bucket> buckets = client.listBucket(new ListBucketOptions()).getResults();

// Delete bucket
client.deleteBucket("my-bucket");
```

#### Object Operations

```java
// Upload object
String content = "Hello, World!";
PutObjectOptions putOptions = new PutObjectOptions.PutObjectOptionsBuilder()
        .configureUploadData().withData(content.getBytes()).endConfigureDataContent()
        .build();
String etag = client.putObject("my-bucket", "my-object", putOptions);

// Download object
DownloadObjectOptions downloadOptions = new DownloadObjectOptions.DownloadObjectOptionsBuilder()
        .configureDataOutput().toBytes().endConfigureDataOutput()
        .build();
S3Object object = client.getObject("my-bucket", "my-object", downloadOptions);

// Delete object
client.deleteObject("my-bucket",new DeleteObjectOptions("my-object"));
```

## Feature Support Matrix

| Feature                | AWS S3 |
|------------------------|--------|
| Bucket Create/Delete   | ✅      |
| Bucket Listing         | ✅      |
| Object Upload/Download | ✅      |
| Multipart Upload       | ✅      |
| Versioning             | ✅      |
| Object Locking         | ✅      |
| Conditional Writes     | ✅      |
| Server-side Encryption | ✅      |
| Presigned URLs         | ✅      |
| Tag Management         | ✅      |
| Access Policies        | ✅      |

We will continue to track AWS S3 updates and add new features.

For other cloud providers, due to varying levels of support, we cannot guarantee that all operations will be available.
You can confirm by running unit tests (typically, core functions like bucket creation/deletion and object
upload/download are widely supported).

## Build Project

```bash
mvn clean install
```

## Run Tests

Refer to the ini file examples in test/resources to configure necessary access credentials and parameters for each cloud
provider, then execute the unit tests:

```bash
# Run all tests
mvn test

# Run tests for specific cloud providers
mvn test -Dtest=AwsS3Test
mvn test -Dtest=AliyunOSSTest
mvn test -Dtest=TencentcloudCosTest
mvn test -Dtest=CloudflareR2Test
mvn test -Dtest=CustomS3Test
```

## Contributing

Issues and Pull Requests are welcome to improve this project.

1. Fork the repository
2. Create a feature branch (`git checkout -b feature/AmazingFeature`)
3. Commit your changes (`git commit -m 'Add some AmazingFeature'`)
4. Push to the branch (`git push origin feature/AmazingFeature`)
5. Open a Pull Request

## License

This project is licensed under the Apache License 2.0

## Contact

Connor Ma - com.tianma@gmail.com

Project link: [https://github.com/matian2014/candy-s3](https://github.com/matian2014/candy-s3)
