package io.github.matian2014.candys3;

import java.util.Date;

public class ObjectLockProperties {

    private ObjectRetentionMode objectLockMode;
    private Date objectLockRetainUntilDate;
    private boolean objectLockLegalHold;

    public ObjectRetentionMode getObjectLockMode() {
        return objectLockMode;
    }

    public void setObjectLockMode(ObjectRetentionMode objectLockMode) {
        this.objectLockMode = objectLockMode;
    }

    public Date getObjectLockRetainUntilDate() {
        return objectLockRetainUntilDate;
    }

    public void setObjectLockRetainUntilDate(Date objectLockRetainUntilDate) {
        this.objectLockRetainUntilDate = objectLockRetainUntilDate;
    }

    public boolean isObjectLockLegalHold() {
        return objectLockLegalHold;
    }

    public void setObjectLockLegalHold(boolean objectLockLegalHold) {
        this.objectLockLegalHold = objectLockLegalHold;
    }
}
