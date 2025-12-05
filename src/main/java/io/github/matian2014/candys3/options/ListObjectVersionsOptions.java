package io.github.matian2014.candys3.options;

public class ListObjectVersionsOptions {

    private String keyMarker;
    private String versionIdMarker;
    private Character delimiter;
    private Integer maxKeys;
    private String prefix;

    public ListObjectVersionsOptions() {
    }


    public String getKeyMarker() {
        return keyMarker;
    }

    public String getVersionIdMarker() {
        return versionIdMarker;
    }

    public Integer getMaxKeys() {
        return maxKeys;
    }

    public String getPrefix() {
        return prefix;
    }

    public ListObjectVersionsOptions keyMarker(String keyMarker) {
        this.keyMarker = keyMarker;
        return this;
    }

    public ListObjectVersionsOptions versionIdMarker(String versionIdMarker) {
        this.versionIdMarker = versionIdMarker;
        return this;
    }

    public Character getDelimiter() {
        return delimiter;
    }

    public ListObjectVersionsOptions maxKeys(Integer maxKeys) {
        this.maxKeys = maxKeys;
        return this;
    }

    public ListObjectVersionsOptions delimiter(Character delimiter) {
        this.delimiter = delimiter;
        return this;
    }

    public ListObjectVersionsOptions prefix(String prefix) {
        this.prefix = prefix;
        return this;
    }
}
