package io.github.matian2014.candys3;

// Not support kms and bucket key now
public enum ServerSideEncryptionAlgorithm {

    AES256("AES256");

    private final String algorithm;

    ServerSideEncryptionAlgorithm(String algorithm) {
        this.algorithm = algorithm;
    }

    public String getAlgorithm() {
        return algorithm;
    }

    public static ServerSideEncryptionAlgorithm valueOfAlgorithm(String algo) {
        for (ServerSideEncryptionAlgorithm item : ServerSideEncryptionAlgorithm.values()) {
            if (item.algorithm.equals(algo)) {
                return item;
            }
        }

        return null;
    }
}
