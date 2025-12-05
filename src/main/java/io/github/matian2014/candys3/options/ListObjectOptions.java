package io.github.matian2014.candys3.options;

public class ListObjectOptions {

    private String startAfter;
    private String continuationToken;
    private Character delimiter;
    private Integer maxKeys;
    private String prefix;
    private boolean fetchOwner;

    public ListObjectOptions() {
    }

    public String getStartAfter() {
        return startAfter;
    }

    public String getContinuationToken() {
        return continuationToken;
    }

    public Integer getMaxKeys() {
        return maxKeys;
    }

    public String getPrefix() {
        return prefix;
    }

    public ListObjectOptions startAfter(String startAfter){
        this.startAfter = startAfter;
        return this;
    }

    public Character getDelimiter() {
        return delimiter;
    }

    public boolean isFetchOwner() {
        return fetchOwner;
    }

    public ListObjectOptions continuationToken(String continuationToken){
        this.continuationToken = continuationToken;
        return this;
    }

    public ListObjectOptions maxKeys(Integer maxKeys){
        this.maxKeys = maxKeys;
        return this;
    }

    public ListObjectOptions delimiter(Character delimiter){
        this.delimiter = delimiter;
        return this;
    }

    public ListObjectOptions fetchOwner(boolean fetchOwner){
        this.fetchOwner = fetchOwner;
        return this;
    }

    public ListObjectOptions prefix(String prefix){
        this.prefix = prefix;
        return this;
    }
}
