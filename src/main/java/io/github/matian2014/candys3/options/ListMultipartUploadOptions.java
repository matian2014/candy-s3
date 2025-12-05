package io.github.matian2014.candys3.options;

public class ListMultipartUploadOptions {

    private Character delimiter;
    private Integer maxUploads;
    private String prefix;
    private String keyMarker;
    private String uploadIdMarker;

    public ListMultipartUploadOptions() {
    }

    public ListMultipartUploadOptions delimiter(Character delimiter) {
        this.delimiter = delimiter;
        return this;
    }

    public ListMultipartUploadOptions maxUploads(Integer maxUploads) {
        this.maxUploads = maxUploads;
        return this;
    }

    public ListMultipartUploadOptions prefix(String prefix) {
        this.prefix = prefix;
        return this;
    }

    public ListMultipartUploadOptions keyMarker(String keyMarker) {
        this.keyMarker = keyMarker;
        return this;
    }

    public ListMultipartUploadOptions uploadIdMarker(String uploadIdMarker) {
        this.uploadIdMarker = uploadIdMarker;
        return this;
    }

    public Character getDelimiter() {
        return delimiter;
    }

    public Integer getMaxUploads() {
        return maxUploads;
    }

    public String getPrefix() {
        return prefix;
    }

    public String getKeyMarker() {
        return keyMarker;
    }

    public String getUploadIdMarker() {
        return uploadIdMarker;
    }
}
