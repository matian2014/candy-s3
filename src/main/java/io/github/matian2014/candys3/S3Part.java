package io.github.matian2014.candys3;

import com.fasterxml.jackson.annotation.JsonAlias;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;

import java.util.Date;

@JsonIgnoreProperties(ignoreUnknown = true)
public class S3Part {

    // Required when complete multipart upload
    @JsonAlias("PartNumber")
    private Integer partNum;

    // Required when complete multipart upload
    @JsonAlias("ETag")
    private String etag;

    @JsonAlias("LastModified")
    private Date lastModified;

    @JsonAlias("Size")
    private Long size;

    @JsonAlias("Owner")
    private Owner owner;
    @JsonAlias("Initiator")
    private Owner initiator;

    public Integer getPartNum() {
        return partNum;
    }

    public void setPartNum(Integer partNum) {
        this.partNum = partNum;
    }

    public String getEtag() {
        return etag;
    }

    public void setEtag(String etag) {
        this.etag = etag;
    }

    public Date getLastModified() {
        return lastModified;
    }

    public void setLastModified(Date lastModified) {
        this.lastModified = lastModified;
    }

    public Long getSize() {
        return size;
    }

    public void setSize(Long size) {
        this.size = size;
    }

    public Owner getOwner() {
        return owner;
    }

    public void setOwner(Owner owner) {
        this.owner = owner;
    }

    public Owner getInitiator() {
        return initiator;
    }

    public void setInitiator(Owner initiator) {
        this.initiator = initiator;
    }
}
