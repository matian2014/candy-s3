package io.github.matian2014.candys3.options;

import org.apache.commons.lang3.StringUtils;

import java.io.InputStream;
import java.util.Arrays;

public final class ObjectDataContentOptions {

    // inputBytes is advanced to inputStream and inputFile
    private final byte[] inputBytes;
    // inputStream is advanced to inputFile
    private final InputStream inputStream;
    private final String inputFile;


    private ObjectDataContentOptions(byte[] inputBytes, InputStream inputStream, String inputFile) {
        this.inputBytes = inputBytes;
        this.inputStream = inputStream;
        this.inputFile = inputFile;
    }

    public byte[] getInputBytes() {
        return inputBytes == null ? null : Arrays.copyOf(inputBytes, inputBytes.length);
    }

    public InputStream getInputStream() {
        return inputStream;
    }

    public String getInputFile() {
        return inputFile;
    }

    public static final class ObjectDataContentOptionsBuilder {
        // inputBytes is advanced to inputStream and inputFile
        private byte[] inputBytes;
        // inputStream is advanced to inputFile
        private InputStream inputStream;
        private String inputFile;

        public ObjectDataContentOptions build() {
            if (inputBytes == null && inputStream == null && StringUtils.isEmpty(inputFile)) {
                throw new IllegalArgumentException("No input data bytes or inputStream or file configured.");
            }
            return new ObjectDataContentOptions(inputBytes, inputStream, inputFile);
        }

        public ObjectDataContentOptionsBuilder withData(byte[] bytes) {
            this.inputBytes = bytes;
            this.inputStream = null;
            this.inputFile = null;
            return this;
        }

        public ObjectDataContentOptionsBuilder withData(InputStream in) {
            this.inputStream = in;
            this.inputFile = null;
            return this;
        }

        public ObjectDataContentOptionsBuilder withData(String file) {
            this.inputFile = file;
            return this;
        }

    }

}
