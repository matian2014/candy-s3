package io.github.matian2014.candys3.options;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

public final class UpdateBucketTagOptions {

    private boolean removeTags = false;
    private Map<String, String> tags;

    public UpdateBucketTagOptions removeTags() {
        this.removeTags = true;
        this.tags = null;
        return this;
    }

    public UpdateBucketTagOptions replaceTags(Map<String, String> tags) {
        this.tags = tags;
        return this;
    }

    public UpdateBucketTagOptions addTags(Map<String, String> tags) {
        if (this.tags == null) {
            this.tags = new HashMap<>();
        }
        this.tags.putAll(tags);
        return this;
    }

    public UpdateBucketTagOptions addTag(String key, String val) {
        if (this.tags == null) {
            this.tags = new HashMap<>();
        }
        this.tags.put(key, val);
        return this;
    }

    public boolean isRemoveTags() {
        return removeTags;
    }

    public Map<String, String> getTags() {
        return tags == null ? null : Collections.unmodifiableMap(tags);
    }


}
