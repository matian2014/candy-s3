package io.github.matian2014.candys3.options;

import io.github.matian2014.candys3.ObjectRetentionMode;

import java.util.Date;

public final class UpdateObjectRetentionOptions {
    private String versionId;

    private ObjectRetentionMode retentionMode;
    private Date objectLockRetainUntilDate;
    private boolean bypassGovernanceRetention = false;

    public String getVersionId() {
        return versionId;
    }

    public UpdateObjectRetentionOptions versionId(String versionId) {
        this.versionId = versionId;
        return this;
    }

    public ObjectRetentionMode getRetentionMode() {
        return retentionMode;
    }

    public UpdateObjectRetentionOptions retentionMode(ObjectRetentionMode objectLockMode) {
        this.retentionMode = objectLockMode;
        return this;
    }

    public Date getObjectLockRetainUntilDate() {
        return objectLockRetainUntilDate;
    }

    public UpdateObjectRetentionOptions retainUntilDate(Date objectLockRetainUntilDate) {
        this.objectLockRetainUntilDate = objectLockRetainUntilDate;
        return this;
    }

    public boolean isBypassGovernanceRetention() {
        return bypassGovernanceRetention;
    }

    public UpdateObjectRetentionOptions bypassGovernanceRetention() {
        this.bypassGovernanceRetention = true;
        return this;
    }
}
