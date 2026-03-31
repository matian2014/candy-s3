package io.github.matian2014.candys3;

import org.junit.platform.engine.TestTag;
import org.junit.platform.engine.TestExecutionResult;
import org.junit.platform.launcher.TestExecutionListener;
import org.junit.platform.launcher.TestIdentifier;
import org.junit.platform.launcher.TestPlan;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Print a single execution summary grouped by {@link org.junit.jupiter.api.Tag}.
 *
 * Notes:
 * - Uses {@link TestIdentifier#getTags()} for tag names.
 * - Uses {@link TestIdentifier#getUniqueId()} to infer provider from the test class name.
 * - Output is printed once after the whole TestPlan finishes.
 */
public class TagStatsTestExecutionListener implements TestExecutionListener {

    private static final Pattern CLASS_PATTERN = Pattern.compile("\\[class:([^\\]]+)\\]");

    private static class Stats {
        long run;
        long passed;
        long failed;
        long errors;
        long skipped;
    }

    private final Map<String, Stats> globalByTag = new HashMap<>();
    private final Map<String, Stats> byProviderAndTag = new HashMap<>();
    private volatile boolean printed = false;

    private static String providerFromUniqueId(String uniqueId) {
        if (uniqueId == null) {
            return "unknown-provider";
        }
        Matcher m = CLASS_PATTERN.matcher(uniqueId);
        if (!m.find()) {
            return "unknown-provider";
        }
        String className = m.group(1);
        int idx = className.lastIndexOf('.');
        if (idx >= 0 && idx + 1 < className.length()) {
            return className.substring(idx + 1);
        }
        return className;
    }

    private static void addStats(Map<String, Stats> map, String key, boolean passed, boolean failed, boolean error, boolean skipped) {
        Stats s = map.get(key);
        if (s == null) {
            s = new Stats();
            map.put(key, s);
        }
        s.run++;
        if (passed) s.passed++;
        if (failed) s.failed++;
        if (error) s.errors++;
        if (skipped) s.skipped++;
    }

    @Override
    public void executionFinished(TestIdentifier testIdentifier, TestExecutionResult testExecutionResult) {
        if (printed) {
            return;
        }
        if (testIdentifier == null || !testIdentifier.isTest()) {
            return;
        }

        Set<TestTag> tags = testIdentifier.getTags();
        if (tags == null || tags.isEmpty()) {
            tags = Collections.singleton(TestTag.create("untagged"));
        }

        String provider = providerFromUniqueId(testIdentifier.getUniqueId());
        TestExecutionResult.Status status = testExecutionResult.getStatus();
        Optional<Throwable> throwable = testExecutionResult.getThrowable();
        Throwable t = throwable == null ? null : throwable.orElse(null);

        boolean passed = status == TestExecutionResult.Status.SUCCESSFUL;
        boolean skipped = false;
        boolean failed = false;
        boolean error = false;

        if (passed) {
            failed = false;
            error = false;
        } else if (status == TestExecutionResult.Status.FAILED) {
            // JUnit 5 uses the launcher as a generic failure; detect assertions vs other errors.
            // In this project, assertion failures typically throw org.opentest4j.AssertionFailedError.
            if (t != null && "org.opentest4j.AssertionFailedError".equals(t.getClass().getName())) {
                failed = true;
            } else {
                error = true;
            }
        } else {
            // ABORTED is not necessarily "skipped"; treat it as an error bucket by default.
            error = true;
        }

        for (TestTag tag : tags) {
            String tagName = tag.getName();
            addStats(globalByTag, tagName, passed, failed, error, skipped);
            addStats(byProviderAndTag, provider + "|" + tagName, passed, failed, error, skipped);
        }
    }

    @Override
    public void executionSkipped(TestIdentifier testIdentifier, String reason) {
        if (printed) {
            return;
        }
        if (testIdentifier == null || !testIdentifier.isTest()) {
            return;
        }

        Set<TestTag> tags = testIdentifier.getTags();
        if (tags == null || tags.isEmpty()) {
            tags = Collections.singleton(TestTag.create("untagged"));
        }
        String provider = providerFromUniqueId(testIdentifier.getUniqueId());

        for (TestTag tag : tags) {
            String tagName = tag.getName();
            addStats(globalByTag, tagName, false, false, false, true);
            addStats(byProviderAndTag, provider + "|" + tagName, false, false, false, true);
        }
    }

    @Override
    public void testPlanExecutionFinished(TestPlan testPlan) {
        if (printed) {
            return;
        }
        printed = true;

        System.out.println("");
        System.out.println("=== Tag Stats (All Providers) ===");
        printMap(globalByTag);

        System.out.println("");
        System.out.println("=== Tag Stats (By Provider) ===");
        printMap(byProviderAndTag);
    }

    private static void printMap(Map<String, Stats> map) {
        map.entrySet().stream()
                .sorted(Map.Entry.comparingByKey())
                .forEach(e -> {
                    Stats s = e.getValue();
                    System.out.println(String.format(
                            "%-26s run=%4d pass=%4d fail=%4d err=%4d skip=%4d",
                            e.getKey(), s.run, s.passed, s.failed, s.errors, s.skipped
                    ));
                });
    }
}

