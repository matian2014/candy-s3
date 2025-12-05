package io.github.matian2014.candys3.options;

import io.github.matian2014.candys3.ServerSideEncryptionAlgorithm;

public final class UpdateServerSideEncryptionOptions {

    // Not support kms key now
    private final String sseAlgorithm;


    private UpdateServerSideEncryptionOptions(String sseAlgorithm) {
        this.sseAlgorithm = sseAlgorithm;
    }

    public String getSseAlgorithm() {
        return sseAlgorithm;
    }

    public static final class UpdateServerSideEncryptionOptionsBuilder {
        private String sseAlgorithm;

        public UpdateServerSideEncryptionOptions build() {
            return new UpdateServerSideEncryptionOptions(this.sseAlgorithm);
        }

        public UpdateServerSideEncryptionOptionsBuilder sseAlgorithm(String algo) {
            this.sseAlgorithm = algo;
            return this;
        }
        public UpdateServerSideEncryptionOptionsBuilder sseAlgorithm(ServerSideEncryptionAlgorithm algo) {
            this.sseAlgorithm = algo.getAlgorithm();
            return this;
        }

    }
}
