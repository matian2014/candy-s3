package io.github.matian2014.candys3;

import com.fasterxml.jackson.annotation.JsonAlias;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;

import java.util.ArrayList;
import java.util.List;

public class ListPaginationResult<T> {

    private List<T> results;

    /**
     * When use ListBuckets/ListObjects, it's 'ContinuationToken'
     * When use ListParts, it's 'NextPartNumberMarker' ( a integer)
     * When use ListMultipartUploads, it's 'NextKeyMarker'
     * When use ListObjectVersions, it's 'NextKeyMarker'
     */
    private String nextPaginationMarker;

    /**
     * When use ListMultipartUploads, it's 'NextUploadIdMarker'
     * When use ListObjectVersions, it's 'NextVersionIdMarker'
     * Ignore it for other list requests.
     */
    private String nextPaginationMarker2;

    // for listObjectVersions
    private List<T> deleteMarkers = new ArrayList<>();

    // for listObjects/listMultipartUploads/listObjectVersions
    private List<CommonPrefix> commonPrefixes = new ArrayList<>();

    public ListPaginationResult(List<T> results) {
        this.results = results;
    }

    public ListPaginationResult(List<T> results, String nextPaginationMarker) {
        this.results = results;
        this.nextPaginationMarker = nextPaginationMarker;
    }

    public ListPaginationResult(List<T> results, String nextPaginationMarker, String nextPaginationMarker2) {
        this.results = results;
        this.nextPaginationMarker = nextPaginationMarker;
        this.nextPaginationMarker2 = nextPaginationMarker2;
    }

    public ListPaginationResult(List<T> results, List<T> deleteMarkers, String nextPaginationMarker, String nextPaginationMarker2) {
        this.results = results;
        this.deleteMarkers = deleteMarkers;
        this.nextPaginationMarker = nextPaginationMarker;
        this.nextPaginationMarker2 = nextPaginationMarker2;
    }

    public List<T> getResults() {
        return results;
    }

    public void setResults(List<T> results) {
        this.results = results;
    }

    public List<T> getDeleteMarkers() {
        return deleteMarkers;
    }

    public void setDeleteMarkers(List<T> deleteMarkers) {
        this.deleteMarkers = deleteMarkers;
    }

    public String getNextPaginationMarker() {
        return nextPaginationMarker;
    }

    public void setNextPaginationMarker(String nextPaginationMarker) {
        this.nextPaginationMarker = nextPaginationMarker;
    }

    public String getNextPaginationMarker2() {
        return nextPaginationMarker2;
    }

    public void setNextPaginationMarker2(String nextPaginationMarker2) {
        this.nextPaginationMarker2 = nextPaginationMarker2;
    }

    public List<CommonPrefix> getCommonPrefixes() {
        return commonPrefixes;
    }

    public void setCommonPrefixes(List<CommonPrefix> commonPrefixes) {
        this.commonPrefixes = commonPrefixes;
    }

    @Override
    public String toString() {
        return "ListPaginationResult{" +
                "results=" + results +
                ", nextPaginationMarker='" + nextPaginationMarker + '\'' +
                ", objectCommonPrefixes=" + commonPrefixes +
                '}';
    }

    @JsonIgnoreProperties(ignoreUnknown = true)
    public static class CommonPrefix {
        @JsonAlias("Prefix")
        private String prefix;

        public String getPrefix() {
            return prefix;
        }

        public void setPrefix(String prefix) {
            this.prefix = prefix;
        }

        @Override
        public String toString() {
            return "CommonPrefix{" +
                    "prefix='" + prefix + '\'' +
                    '}';
        }
    }
}
