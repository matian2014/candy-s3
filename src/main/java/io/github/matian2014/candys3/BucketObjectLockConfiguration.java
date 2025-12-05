package io.github.matian2014.candys3;

public class BucketObjectLockConfiguration {

    private boolean objectLockEnabled;
    private ObjectRetentionMode mode;
    private Integer years;
    private Integer days;

    public boolean isObjectLockEnabled() {
        return objectLockEnabled;
    }

    public void setObjectLockEnabled(boolean objectLockEnabled) {
        this.objectLockEnabled = objectLockEnabled;
    }

    public ObjectRetentionMode getMode() {
        return mode;
    }

    public void setMode(ObjectRetentionMode mode) {
        this.mode = mode;
    }

    public Integer getYears() {
        return years;
    }

    public void setYears(Integer years) {
        this.years = years;
    }

    public Integer getDays() {
        return days;
    }

    public void setDays(Integer days) {
        this.days = days;
    }
}
