package io.github.matian2014.candys3.options;

import java.util.Date;

public final class PutObjectHeaderOptions {

    private String cacheControl;
    private String contentDisposition;
    private String contentEncoding;
    private String contentLanguage;
    private String contentType;
    private Date expires;

    PutObjectHeaderOptions() {
    }

    public PutObjectHeaderOptions cacheControl(String cacheControl) {
        this.cacheControl = cacheControl;
        return this;
    }

    public PutObjectHeaderOptions contentDisposition(String contentDisposition) {
        this.contentDisposition = contentDisposition;
        return this;
    }

    public PutObjectHeaderOptions contentEncoding(String contentEncoding) {
        this.contentEncoding = contentEncoding;
        return this;
    }

    public PutObjectHeaderOptions contentLanguage(String contentLanguage) {
        this.contentLanguage = contentLanguage;
        return this;
    }

    public PutObjectHeaderOptions contentType(String contentType) {
        this.contentType = contentType;
        return this;
    }

    public PutObjectHeaderOptions expires(Date expires) {
        this.expires = expires;
        return this;
    }

    public String getCacheControl() {
        return cacheControl;
    }

    public String getContentDisposition() {
        return contentDisposition;
    }

    public String getContentEncoding() {
        return contentEncoding;
    }

    public String getContentLanguage() {
        return contentLanguage;
    }

    public String getContentType() {
        return contentType;
    }

    public Date getExpires() {
        return expires == null ? null : new Date(expires.getTime());
    }


}
