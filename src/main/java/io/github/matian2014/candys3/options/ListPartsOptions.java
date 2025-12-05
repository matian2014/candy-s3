package io.github.matian2014.candys3.options;

import org.apache.commons.lang3.StringUtils;

public final class ListPartsOptions {

    private final String uploadId;
    private Integer startAfterPartNumber;
    private Integer maxParts;

    public ListPartsOptions(String uploadId) {
        this.uploadId = uploadId;
    }

    public ListPartsOptions startAfterPartNumber(Integer startAfter) {
        this.startAfterPartNumber = startAfter;
        return this;
    }

    public ListPartsOptions startAfterPartNumber(String startAfterStr) {
        if (StringUtils.isNotBlank(startAfterStr)) {
            this.startAfterPartNumber = Integer.valueOf(startAfterStr);
        } else {
            this.startAfterPartNumber = null;
        }
        return this;
    }

    public ListPartsOptions maxParts(Integer maxParts) {
        this.maxParts = maxParts;
        return this;
    }

    public String getUploadId() {
        return uploadId;
    }

    public Integer getStartAfterPartNumber() {
        return startAfterPartNumber;
    }

    public Integer getMaxParts() {
        return maxParts;
    }
}
