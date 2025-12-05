package io.github.matian2014.candys3.options;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.stream.Collectors;

public final class DeleteObjectsBatchOptions {

    private final List<DeleteObjectsBatchItem> deleteObjectKeys;
    private final boolean bypassGovernanceRetention;

    private DeleteObjectsBatchOptions(List<DeleteObjectsBatchItem> deleteObjectKeys, boolean bypassGovernanceRetention) {
        this.deleteObjectKeys = deleteObjectKeys;
        this.bypassGovernanceRetention = bypassGovernanceRetention;
    }

    public List<DeleteObjectsBatchItem> getDeleteObjectKeys() {
        return deleteObjectKeys;
    }

    public boolean isBypassGovernanceRetention() {
        return bypassGovernanceRetention;
    }

    public static class DeleteObjectsBatchItem {
        private final String key;
        private final String versionId;
        private String ifMatch;

        public DeleteObjectsBatchItem(String key) {
            this(key, null);
        }

        public DeleteObjectsBatchItem(String key, String versionId) {
            this.key = key;
            this.versionId = versionId;
        }

        public String getKey() {
            return key;
        }

        public String getVersionId() {
            return versionId;
        }

        public String getIfMatch() {
            return ifMatch;
        }

        public DeleteObjectsBatchItem ifMatch(String etag) {
            this.ifMatch = etag;
            return this;
        }
    }

    public static final class DeleteObjectsBatchOptionsBuilder {
        List<DeleteObjectsBatchItem> deleteObjectKeys;
        private boolean bypassGovernanceRetention;

        public DeleteObjectsBatchOptions build() {
            if (deleteObjectKeys == null || deleteObjectKeys.isEmpty()) {
                throw new IllegalArgumentException("At least one delete object option is required when do delete objects batch.");
            }
            return new DeleteObjectsBatchOptions(deleteObjectKeys, bypassGovernanceRetention);
        }

        public DeleteObjectsBatchOptionsBuilder addDeleteObject(String deleteObjectKey) {
            if (this.deleteObjectKeys == null) {
                this.deleteObjectKeys = new ArrayList<>();
            }

            this.deleteObjectKeys.add(new DeleteObjectsBatchItem(deleteObjectKey));
            return this;
        }

        public DeleteObjectsBatchOptionsBuilder addDeleteObjects(Collection<String> deleteObjectKeys) {
            if (this.deleteObjectKeys == null) {
                this.deleteObjectKeys = new ArrayList<>();
            }

            this.deleteObjectKeys.addAll(deleteObjectKeys.stream()
                    .map(DeleteObjectsBatchItem::new)
                    .collect(Collectors.toList()));
            return this;
        }

        public DeleteObjectsBatchOptionsBuilder addDeleteObject(String deleteObjectKey, String deleteVersionId) {
            if (this.deleteObjectKeys == null) {
                this.deleteObjectKeys = new ArrayList<>();
            }

            this.deleteObjectKeys.add(new DeleteObjectsBatchItem(deleteObjectKey, deleteVersionId));
            return this;
        }

        /**
         * Add a delete object option with conditional options.
         * @param deleteObjectKey The key of the object to delete.
         * @param deleteObjectEtag The ETag of the object to delete, or "*" to match any.
         * @return this builder.
         */
        public DeleteObjectsBatchOptionsBuilder addConditionalDeleteObject(String deleteObjectKey, String deleteObjectEtag) {
            if (this.deleteObjectKeys == null) {
                this.deleteObjectKeys = new ArrayList<>();
            }

            this.deleteObjectKeys.add(new DeleteObjectsBatchItem(deleteObjectKey).ifMatch(deleteObjectEtag));
            return this;
        }

        public DeleteObjectsBatchOptionsBuilder bypassGovernanceRetention() {
            this.bypassGovernanceRetention = true;
            return this;
        }
    }

}
