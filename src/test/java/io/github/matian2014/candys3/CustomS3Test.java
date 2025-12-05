package io.github.matian2014.candys3;

import org.junit.AfterClass;
import org.junit.Test;

import java.io.IOException;

/**
 * Example test class for custom S3 provider
 */
public class CustomS3Test {

    @AfterClass
    public static void customS3RemoveTestsBucket() throws IOException {
        new CandyS3Test().removeTestBuckets(S3Provider.CUSTOM);
        System.out.println("customS3RemoveTestsBucket done.");
    }

    @Test
    public void customS3CreateBucketTest() throws IOException {
        new CandyS3Test().createBucketTest(S3Provider.CUSTOM);
    }

    // Add more tests for custom S3 provider.

}
