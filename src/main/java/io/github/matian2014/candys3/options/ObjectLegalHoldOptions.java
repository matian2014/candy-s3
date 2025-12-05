package io.github.matian2014.candys3.options;

public final class ObjectLegalHoldOptions {
    private String versionId;

    // Use when update object legal hold
    private boolean legalHold = false;

    public String getVersionId() {
        return versionId;
    }

    public ObjectLegalHoldOptions versionId(String versionId) {
        this.versionId = versionId;
        return this;
    }

    public ObjectLegalHoldOptions legalHold(boolean legalHold) {
        this.legalHold = legalHold;
        return this;
    }

    public boolean isLegalHold() {
        return legalHold;
    }
}
