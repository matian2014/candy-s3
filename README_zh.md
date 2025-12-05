# Candy-S3

[![License](https://img.shields.io/badge/license-Apache%202-blue.svg)](LICENSE)

[English Version](README.md) | [中文版本](README_zh.md)

Candy-S3 是一个统一的SDK，用于与多种云服务提供商的S3兼容对象存储服务进行交互。它提供了一套简洁易用的API，屏蔽了不同云服务商之间的差异，让开发者能够轻松地在多个云平台之间切换和管理对象存储资源。

## 功能特点

### 多云支持

- **Amazon S3**: 完整支持AWS S3的所有核心功能
- **Cloudflare R2**: 兼容Cloudflare R2服务
- **阿里云 OSS**: 兼容阿里云OSS服务
- **腾讯云 COS**: 支持腾讯云COS服务
- **自定义S3**: 支持任何兼容S3协议的对象存储服务

### 核心功能

- 存储桶管理（创建、删除、列举）
- 对象管理（上传、下载、删除、复制）
- 分片上传支持
- 预签名URL（用于上传和下载对象以及其他操作）
- 批量操作支持

以下高级特性在Candy-S3中也得到了支持，并对AWS S3进行了详尽的测试和验证，但在其他云服务商的S3服务中支持程度参差不齐：
- 对象版本控制
- 条件写入（如：If-Match, If-None-Match, If-Modified-Since, If-Unmodified-Since）
- 标签管理
- 对象锁定和合规保留
- 服务端加密

### 技术优势

- 统一API接口，无需针对不同云服务商编写不同代码
- 基于OkHttp的高性能HTTP客户端
- 完整的错误处理机制
- 支持绝大多数S3标准功能
- 易于集成和扩展
- 丰富的单元测试，覆盖核心功能和异常场景，确保代码质量和稳定性的同时可作为使用参考

## 快速开始

### 环境要求

- Java 8 或更高版本
- Maven 3.6+ (构建项目)

### 添加依赖

在你的 `pom.xml` 中添加以下依赖：

```xml

<dependency>
    <groupId>io.github.matian2014</groupId>
    <artifactId>candy-s3</artifactId>
    <version>1.0.0</version>
</dependency>
```

> Candy-S3的代码足够简单，核心的功能只有一个类文件，你也可以直接将源代码集成到你的项目中，根据需要进行定制和扩展。

### 基本使用

我们提供了丰富的单元测试代码 `src/test/java/io/github/matian2014/candys3/CandyS3Test.java` ，帮助你快速了解和使用Candy-S3的各种功能。

#### 初始化客户端

```java
// 创建AWS S3客户端
CandyS3 client = new CandyS3(S3Provider.AWS);
client.setAccessKey("your-access-key");
client.setSecretKey("your-secret-key");
client.setRegion("us-east-1");
```

> 有部分云服务商的S3服务需要额外的参数，
> 如Cloudflare R2需要指定`account-id`，可以在初始化客户端时通过`setCloudflareR2AccountId`方法设置。
> 你可以参考单元测试代码 `src/test/java/io/github/matian2014/candys3/CandyS3Test.java` 中的示例，了解如何在不同云服务商的S3服务中使用Candy-S3。

#### 存储桶操作

```java
// 创建存储桶
CreateBucketOptions options = new CreateBucketOptions.CreateBucketOptionsBuilder("my-bucket").build();
client.createBucket(options);

// 列举存储桶
List<Bucket> buckets = client.listBucket(new ListBucketOptions()).getResults();

// 删除存储桶
client.deleteBucket("my-bucket");
```

#### 对象操作

```java
// 上传对象
String content = "Hello, World!";
PutObjectOptions putOptions = new PutObjectOptions.PutObjectOptionsBuilder()
        .configureUploadData().withData(content.getBytes()).endConfigureDataContent()
        .build();
String etag = client.putObject("my-bucket", "my-object", putOptions);

// 下载对象
DownloadObjectOptions downloadOptions = new DownloadObjectOptions.DownloadObjectOptionsBuilder()
        .configureDataOutput().toBytes().endConfigureDataOutput()
        .build();
S3Object object = client.getObject("my-bucket", "my-object", downloadOptions);

// 删除对象
client.deleteObject("my-bucket",new DeleteObjectOptions("my-object"));
```

## 支持的功能列表

| 功能       | AWS S3 |
|----------|--------|
| 存储桶创建/删除 | ✅      |
| 存储桶列举    | ✅      |
| 对象上传/下载  | ✅      |
| 分片上传     | ✅      |
| 版本控制     | ✅      |
| 对象锁定     | ✅      |
| 条件写入     | ✅      |
| 服务端加密    | ✅      |
| 预签名URL   | ✅      |
| 标签管理     | ✅      |
| 访问策略     | ✅      |

我们将持续跟踪AWS S3的更新资讯并添加新功能。

对于其他云服务商，因为各自支持情况的差异，并不能保证所有操作均可用，你可以通过运行单元测试来确认（通常来说，核心的桶创建及删除、对象上传及下载功能是广泛支持的）。

## 构建项目

```bash
mvn clean install
```

## 运行测试

参考test/resources中的ini文件示例，为每个云服务商配置必要的访问凭证等参数，然后执行单元测试：

```bash
# 运行所有测试
mvn test

# 运行特定云服务商的测试
mvn test -Dtest=AwsS3Test
mvn test -Dtest=AliyunOSSTest
mvn test -Dtest=TencentcloudCosTest
mvn test -Dtest=CloudflareR2Test
mvn test -Dtest=CustomS3Test
```

## 贡献指南

欢迎提交Issue和Pull Request来改进这个项目。

1. Fork 本仓库
2. 创建功能分支 (`git checkout -b feature/AmazingFeature`)
3. 提交更改 (`git commit -m 'Add some AmazingFeature'`)
4. 推送到分支 (`git push origin feature/AmazingFeature`)
5. 开启 Pull Request

## 许可证

本项目采用 Apache License 2.0 许可证

## 联系方式

Connor Ma - com.tianma@gmail.com

项目链接: [https://github.com/matian2014/candy-s3](https://github.com/matian2014/candy-s3)
