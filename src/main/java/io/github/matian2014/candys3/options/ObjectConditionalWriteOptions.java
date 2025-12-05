package io.github.matian2014.candys3.options;

public final class ObjectConditionalWriteOptions {

    private String ifMatch;
    private String ifNoneMatch; // use '*'to check if object key exists

    ObjectConditionalWriteOptions() {
    }

    public ObjectConditionalWriteOptions ifMatch(String etag) {
        this.ifMatch = etag;
        return this;
    }

    /**
     * PutObject: Uploads the object only if the object key name does not already exist in the bucket specified.
     * CopyObject: Copies the object only if the object key name at the destination does not already exist in the bucket specified.
     * Expects the '*' (asterisk) character.
     * @return this builder.
     */
    public ObjectConditionalWriteOptions ifNotExists() {
        this.ifNoneMatch = "*";
        return this;
    }

    public String getIfMatch() {
        return ifMatch;
    }

    public String getIfNoneMatch() {
        return ifNoneMatch;
    }

}
