package org.opennms.timeseries.impl.pgtimeseries.partition;

import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

/** What one partition maintenance run did (or why it did nothing). */
public final class MaintenanceReport {

    private final Instant startedAt;
    private final boolean dryRun;
    private Instant finishedAt;
    private String skippedReason;
    private final List<String> created = new ArrayList<>();
    private final List<String> rescued = new ArrayList<>();
    private final List<String> dropped = new ArrayList<>();
    private final List<String> compressed = new ArrayList<>();
    private final List<String> errors = new ArrayList<>();

    public MaintenanceReport(final Instant startedAt) {
        this(startedAt, false);
    }

    public MaintenanceReport(final Instant startedAt, final boolean dryRun) {
        this.startedAt = startedAt;
        this.dryRun = dryRun;
    }

    /** True when nothing was changed and the lists describe what a real run would have done. */
    public boolean isDryRun() {
        return dryRun;
    }

    void skipped(final String reason) {
        this.skippedReason = reason;
    }

    void error(final String step, final Throwable t) {
        errors.add(step + ": " + t);
    }

    void finished(final Instant at) {
        this.finishedAt = at;
    }

    public Instant getStartedAt() {
        return startedAt;
    }

    public Instant getFinishedAt() {
        return finishedAt;
    }

    public Duration getDuration() {
        return finishedAt == null ? Duration.ZERO : Duration.between(startedAt, finishedAt);
    }

    /** Non-null when the run did no work at all, with the reason (e.g. another instance held the lock). */
    public String getSkippedReason() {
        return skippedReason;
    }

    public boolean wasSkipped() {
        return skippedReason != null;
    }

    public List<String> getCreated() {
        return Collections.unmodifiableList(created);
    }

    public List<String> getRescued() {
        return Collections.unmodifiableList(rescued);
    }

    public List<String> getDropped() {
        return Collections.unmodifiableList(dropped);
    }

    public List<String> getCompressed() {
        return Collections.unmodifiableList(compressed);
    }

    public List<String> getErrors() {
        return Collections.unmodifiableList(errors);
    }

    public boolean hasErrors() {
        return !errors.isEmpty();
    }

    /** True if any partition was created, rescued, dropped or compressed. */
    public boolean didWork() {
        return !created.isEmpty() || !rescued.isEmpty() || !dropped.isEmpty() || !compressed.isEmpty();
    }

    List<String> created() {
        return created;
    }

    List<String> rescued() {
        return rescued;
    }

    List<String> dropped() {
        return dropped;
    }

    List<String> compressed() {
        return compressed;
    }

    /** One-line human readable summary for logs and the shell. */
    public String summary() {
        if (wasSkipped()) {
            return "skipped (" + skippedReason + ")";
        }
        final StringBuilder b = new StringBuilder();
        if (dryRun) {
            b.append("dry run: would have ");
        }
        b.append("created=").append(created.size())
         .append(", rescued=").append(rescued.size())
         .append(", dropped=").append(dropped.size())
         .append(", compressed=").append(compressed.size())
         .append(", errors=").append(errors.size())
         .append(", took=").append(getDuration().toMillis()).append("ms");
        if (!created.isEmpty()) {
            b.append("; created ").append(created);
        }
        if (!rescued.isEmpty()) {
            b.append("; rescued ").append(rescued);
        }
        if (!dropped.isEmpty()) {
            b.append("; dropped ").append(dropped);
        }
        if (!compressed.isEmpty()) {
            b.append("; compressed ").append(compressed);
        }
        if (!errors.isEmpty()) {
            b.append("; errors ").append(errors);
        }
        return b.toString();
    }

    @Override
    public String toString() {
        return "MaintenanceReport[" + startedAt + ": " + summary() + "]";
    }
}
