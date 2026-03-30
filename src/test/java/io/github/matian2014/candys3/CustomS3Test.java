package io.github.matian2014.candys3;

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.security.NoSuchAlgorithmException;

/**
 * Example test class for custom S3 provider
 */
public class CustomS3Test {

    @BeforeAll
    public static void setUpTempFiles() {
        CandyS3Test.mkTempFiles();
    }

    @AfterAll
    public static void customS3RemoveTestsBucket() throws IOException {
//        new CandyS3Test().removeTestBuckets(S3Provider.CUSTOM);
        CandyS3Test.cleanUpTempFiles();
        System.out.println("customS3RemoveTestsBucket done.");
    }

    @Test
    public void customS3CreateBucketTest() throws IOException {
        new CandyS3Test().createBucketTest(S3Provider.CUSTOM);
    }

    @Test
    public void awsPutDownloadSmallObjectTest() throws IOException, NoSuchAlgorithmException {
        new CandyS3Test().putDownloadSmallObjectTest(S3Provider.CUSTOM);
    }

    @Test
    public void awsPutDownloadLargeObjectTest() throws IOException, NoSuchAlgorithmException {
        new CandyS3Test().putDownloadLargeObjectTest(S3Provider.CUSTOM);
    }

    // Add more tests for custom S3 provider.

}
