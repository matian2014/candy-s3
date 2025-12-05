package io.github.matian2014.candys3.options;

import io.github.matian2014.candys3.ObjectRetentionMode;

import java.util.Date;

public final class ObjectLockOptions {

    private ObjectRetentionMode objectLockMode;
    private Date objectLockRetainUntilDate;
    private boolean objectLockLegalHold;

    ObjectLockOptions() {
    }

    public ObjectLockOptions lockMode(ObjectRetentionMode lockMode) {
        this.objectLockMode = lockMode;
        return this;
    }

    public ObjectLockOptions retainUntilDate(Date retainUntilDate) {
        this.objectLockRetainUntilDate = retainUntilDate;
        return this;
    }

    public ObjectLockOptions legalHold(boolean legalHold) {
        this.objectLockLegalHold = legalHold;
        return this;
    }

    public ObjectRetentionMode getObjectLockMode() {
        return objectLockMode;
    }

    public Date getObjectLockRetainUntilDate() {
        return objectLockRetainUntilDate;
    }

    public boolean isObjectLockLegalHold() {
        return objectLockLegalHold;
    }

}
