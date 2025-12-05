package io.github.matian2014.candys3.options;

public class ListBucketOptions {

    private String continuationToken;

    /**
     * Cloudflare R2: ListBuckets search parameter max-buckets not implemented.
     */
    private Integer maxBuckets;
    private String prefix;
    private boolean filterBucketRegion;

    public ListBucketOptions() {
    }

    public String getContinuationToken() {
        return continuationToken;
    }

    public Integer getMaxBuckets() {
        return maxBuckets;
    }

    public String getPrefix() {
        return prefix;
    }

    public boolean isFilterBucketRegion() {
        return filterBucketRegion;
    }

    public ListBucketOptions continuationToken(String continuationToken){
        this.continuationToken = continuationToken;
        return this;
    }

    public ListBucketOptions maxBuckets(Integer maxBuckets){
        this.maxBuckets = maxBuckets;
        return this;
    }

    public ListBucketOptions prefix(String prefix){
        this.prefix = prefix;
        return this;
    }

    public ListBucketOptions filterBucketRegion(boolean filterBucketRegion){
        this.filterBucketRegion = filterBucketRegion;
        return this;
    }

}
