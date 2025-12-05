package io.github.matian2014.candys3.options;

public final class UpdateBucketPolicyOptions {

    private String policyJson;

    public UpdateBucketPolicyOptions removePolicy() {
        this.policyJson = null;
        return this;
    }

    public UpdateBucketPolicyOptions updatePolicy(String policyJson) {
        this.policyJson = policyJson;
        return this;
    }

    public String getPolicyJson() {
        return policyJson;
    }
}
