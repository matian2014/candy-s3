package io.github.matian2014.candys3.options;

public class GetObjectRetentionOptions {

    private String versionId;

    public String getVersionId() {
        return versionId;
    }

    public GetObjectRetentionOptions versionId(String versionId) {
        this.versionId = versionId;
        return this;
    }

    @Override
    public String toString() {
        return "GetObjectRetentionOptions{" +
                "versionId='" + versionId + '\'' +
                '}';
    }
}
