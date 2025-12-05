package io.github.matian2014.candys3;

import com.fasterxml.jackson.annotation.JsonAlias;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;

@JsonIgnoreProperties(ignoreUnknown = true)
public class BucketPublicAccessBlock {

    @JsonAlias("BlockPublicAcls")
    private boolean blockPublicAcls;
    @JsonAlias("BlockPublicPolicy")
    private boolean blockPublicPolicy;
    @JsonAlias("IgnorePublicAcls")
    private boolean ignorePublicAcls;
    @JsonAlias("RestrictPublicBuckets")
    private boolean restrictPublicBuckets;

    public boolean isBlockPublicAcls() {
        return blockPublicAcls;
    }

    public void setBlockPublicAcls(boolean blockPublicAcls) {
        this.blockPublicAcls = blockPublicAcls;
    }

    public boolean isBlockPublicPolicy() {
        return blockPublicPolicy;
    }

    public void setBlockPublicPolicy(boolean blockPublicPolicy) {
        this.blockPublicPolicy = blockPublicPolicy;
    }

    public boolean isIgnorePublicAcls() {
        return ignorePublicAcls;
    }

    public void setIgnorePublicAcls(boolean ignorePublicAcls) {
        this.ignorePublicAcls = ignorePublicAcls;
    }

    public boolean isRestrictPublicBuckets() {
        return restrictPublicBuckets;
    }

    public void setRestrictPublicBuckets(boolean restrictPublicBuckets) {
        this.restrictPublicBuckets = restrictPublicBuckets;
    }
}
