package io.github.matian2014.candys3.signer;

import java.io.UnsupportedEncodingException;
import java.net.URLEncoder;
import java.nio.charset.StandardCharsets;

/**
 * Various Http helper routines
 */
public class HttpUtils {

    public static String urlEncode(String url, boolean keepPathSlash) {
        String encoded;
        try {
            encoded = URLEncoder.encode(url, "UTF-8");
        } catch (UnsupportedEncodingException e) {
            throw new RuntimeException("UTF-8 encoding is not supported.", e);
        }
        if ( keepPathSlash ) {
            encoded = encoded.replace("%2F", "/");
        }
        return encoded;
    }

    /**
     * Encodes a query parameter name or value for SigV4 canonical query strings and for request URLs.
     * {@link URLEncoder} encodes spaces as {@code +}; AWS Signature V4 requires spaces as {@code %20}.
     * @param s the original string
     * @return uriEncode query string
     */
    public static String uriEncodeQueryComponent(String s) {
        if (s == null) {
            return "";
        }
        try {
            return URLEncoder.encode(s, "UTF-8").replace("+", "%20");
        } catch (UnsupportedEncodingException e) {
            throw new RuntimeException("UTF-8 encoding is not supported.", e);
        }
    }

    /**
     * Percent-encodes an S3 object key for use in the request URI path (virtual-hosted or path-style).
     * Slash ({@code /}) is preserved as a delimiter; each segment is UTF-8 encoded per URI path rules
     * (spaces become {@code %20}, not {@code +}). This aligns with SigV4 canonical URI expectations.
     * @param objectKey objectKey
     * @return uriEncode objectKey
     */
    public static String uriEncodeS3ObjectKey(String objectKey) {
        if (objectKey == null || objectKey.isEmpty()) {
            return objectKey == null ? null : "";
        }
        String[] segments = objectKey.split("/", -1);
        StringBuilder result = new StringBuilder();
        for (int i = 0; i < segments.length; i++) {
            if (i > 0) {
                result.append('/');
            }
            result.append(percentEncodePathSegmentUtf8(segments[i]));
        }
        return result.toString();
    }

    /**
     * Builds {@code x-amz-copy-source} value {@code /bucket/key} with the key path encoded like {@link #uriEncodeS3ObjectKey(String)}.
     * ARN-style copy sources are returned with a leading slash and without path encoding.
     * @param copySource copySource
     * @return formatted s3 copy source
     */
    public static String formatAmzCopySource(String copySource) {
        if (copySource == null) {
            return null;
        }
        String s = copySource.trim();
        if (s.isEmpty()) {
            return "/";
        }
        if (s.startsWith("arn:")) {
            return s.startsWith("/") ? s : "/" + s;
        }
        s = s.startsWith("/") ? s.substring(1) : s;
        int slash = s.indexOf('/');
        if (slash < 0) {
            return "/" + uriEncodeS3ObjectKey(s);
        }
        String bucket = s.substring(0, slash);
        String key = s.substring(slash + 1);
        return "/" + bucket + "/" + uriEncodeS3ObjectKey(key);
    }

    private static String percentEncodePathSegmentUtf8(String segment) {
        byte[] bytes = segment.getBytes(StandardCharsets.UTF_8);
        StringBuilder sb = new StringBuilder(bytes.length * 3);
        for (byte b : bytes) {
            int v = b & 0xFF;
            if (isUriUnreserved(v)) {
                sb.append((char) v);
            } else {
                sb.append(String.format("%%%02X", v));
            }
        }
        return sb.toString();
    }

    private static boolean isUriUnreserved(int codePoint) {
        return (codePoint >= 'a' && codePoint <= 'z')
                || (codePoint >= 'A' && codePoint <= 'Z')
                || (codePoint >= '0' && codePoint <= '9')
                || codePoint == '-'
                || codePoint == '_'
                || codePoint == '.'
                || codePoint == '~';
    }
}
