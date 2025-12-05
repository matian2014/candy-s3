package io.github.matian2014.candys3.options;

import io.github.matian2014.candys3.ObjectRetentionMode;

public final class UpdateBucketObjectLockOptions {
    private final ObjectRetentionMode retentionMode;
    private final Integer years;
    private final Integer days;

    private UpdateBucketObjectLockOptions(ObjectRetentionMode retentionMode, Integer years, Integer days) {
        this.retentionMode = retentionMode;
        this.years = years;
        this.days = days;
    }

    public ObjectRetentionMode getRetentionMode() {
        return retentionMode;
    }

    public Integer getYears() {
        return years;
    }

    public Integer getDays() {
        return days;
    }

    public static final class UpdateBucketObjectLockOptionsBuilder {
        private ObjectRetentionMode retentionMode;
        private Integer years;
        private Integer days;

        public UpdateBucketObjectLockOptionsBuilder() {
        }

        public UpdateBucketObjectLockOptions build() {
            if (retentionMode == null) {
                throw new IllegalArgumentException("Object retention mode required.");
            } else {
                if (years == null && days == null) {
                    throw new IllegalArgumentException("Object retention mode must be used with either Days or Years.");
                }
            }

            return new UpdateBucketObjectLockOptions(retentionMode, years, days);
        }

        public UpdateBucketObjectLockOptions buildWithoutRetention() {
            return new UpdateBucketObjectLockOptions(null, null, null);
        }

        public UpdateBucketObjectLockOptionsBuilder retentionYears(ObjectRetentionMode retentionMode, Integer years) {
            this.retentionMode = retentionMode;
            this.years = years;
            return this;
        }

        public UpdateBucketObjectLockOptionsBuilder retentionDays(ObjectRetentionMode retentionMode, Integer days) {
            this.retentionMode = retentionMode;
            this.days = days;
            return this;
        }
    }

}
