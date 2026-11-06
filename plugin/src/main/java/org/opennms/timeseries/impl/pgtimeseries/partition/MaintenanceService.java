package org.opennms.timeseries.impl.pgtimeseries.partition;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.function.BooleanSupplier;

import com.codahale.metrics.Meter;
import com.codahale.metrics.MetricRegistry;
import com.codahale.metrics.Timer;

import lombok.extern.slf4j.Slf4j;

/**
 * Runs {@link PartitionManager#runMaintenance()} on a dedicated single thread: once at start-up and then every
 * {@code intervalMinutes} with a fixed delay. This is the plugin-side replacement for the two hourly pg_cron
 * jobs that pg_timeseries installed. It is kept separate from the sample flush executor so a slow
 * {@code DROP TABLE} can never delay a flush.
 */
@Slf4j
public class MaintenanceService {

    private static final String LEGACY_WARNING = "The pg_timeseries extension (pg_partman) still manages the "
            + "time series table in this database. The plugin now manages partitions itself; both are safe to "
            + "run together but you should migrate with the opennms-pgtimeseries:migrate-from-extension command.";

    private final PartitionManager partitionManager;
    private final long intervalMinutes;
    private final BooleanSupplier legacyManagedCheck;

    private final Meter partitionsCreated;
    private final Meter partitionsRescued;
    private final Meter partitionsDropped;
    private final Meter partitionsCompressed;
    private final Meter failures;
    private final Meter skipped;
    private final Timer duration;

    private volatile ScheduledExecutorService executor;
    private volatile MaintenanceReport lastReport;
    /** null: not checked yet; TRUE: warned once, keep reminding at DEBUG; FALSE: not legacy, never check again. */
    private volatile Boolean legacyManaged;

    /**
     * @param partitionManager   does the work
     * @param intervalMinutes    minutes between runs; values below 1 are treated as 60
     * @param metrics            registry the maintenance meters are added to (under {@code maintenance.*})
     * @param legacyManagedCheck optional; when it returns true a warning about the legacy extension is logged
     *                           before each run
     */
    public MaintenanceService(final PartitionManager partitionManager, final long intervalMinutes,
                              final MetricRegistry metrics, final BooleanSupplier legacyManagedCheck) {
        this.partitionManager = Objects.requireNonNull(partitionManager);
        this.intervalMinutes = intervalMinutes < 1 ? 60 : intervalMinutes;
        this.legacyManagedCheck = legacyManagedCheck;
        final MetricRegistry registry = metrics != null ? metrics : new MetricRegistry();
        this.partitionsCreated = registry.meter("maintenance.partitionsCreated");
        this.partitionsRescued = registry.meter("maintenance.partitionsRescued");
        this.partitionsDropped = registry.meter("maintenance.partitionsDropped");
        this.partitionsCompressed = registry.meter("maintenance.partitionsCompressed");
        this.failures = registry.meter("maintenance.failures");
        this.skipped = registry.meter("maintenance.skipped");
        this.duration = registry.timer("maintenance.duration");
    }

    public PartitionManager getPartitionManager() {
        return partitionManager;
    }

    public long getIntervalMinutes() {
        return intervalMinutes;
    }

    /** Starts the scheduler: an immediate run, then one every interval. Calling it twice is a no-op. */
    public synchronized void start() {
        if (executor != null) {
            return;
        }
        final ScheduledExecutorService ex = Executors.newSingleThreadScheduledExecutor(r -> {
            final Thread t = new Thread(r, "pgtimeseries-maintenance");
            t.setDaemon(true);
            return t;
        });
        ex.scheduleWithFixedDelay(this::runQuietly, 0, intervalMinutes, TimeUnit.MINUTES);
        this.executor = ex;
        log.info("Partition maintenance scheduled every {} minute(s)", intervalMinutes);
    }

    /** Stops the scheduler, waiting briefly for an in-flight run. */
    public synchronized void stop() {
        final ScheduledExecutorService ex = this.executor;
        this.executor = null;
        if (ex == null) {
            return;
        }
        ex.shutdown();
        try {
            if (!ex.awaitTermination(30, TimeUnit.SECONDS)) {
                ex.shutdownNow();
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            ex.shutdownNow();
        }
    }

    public boolean isRunning() {
        return executor != null;
    }

    /**
     * Runs a maintenance pass now and returns its report. When the scheduler is running the pass is executed on
     * the maintenance thread (serialised with scheduled runs); otherwise it runs on the caller's thread.
     */
    public MaintenanceReport runNow() {
        final ScheduledExecutorService ex = this.executor;
        if (ex != null) {
            try {
                return ex.submit(this::run).get();
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                throw new IllegalStateException("Interrupted while waiting for partition maintenance", e);
            } catch (ExecutionException e) {
                throw new IllegalStateException("Partition maintenance failed", e.getCause());
            } catch (RejectedExecutionException e) {
                // shutting down; fall through and run inline
            }
        }
        return run();
    }

    /** The most recent report, or null if no run has completed yet. */
    public MaintenanceReport getLastReport() {
        return lastReport;
    }

    /**
     * Creates the partitions needed to cover the lead window, right now, on the maintenance thread, waiting at
     * most {@code timeoutSeconds}. This is the cheap subset of a run that the write path uses when the database
     * reports that no partition accepts a row; a full run (retention, compression) could hold up flushing.
     *
     * @return the partitions created, or an empty list if the step was skipped or timed out
     */
    public List<Partition> ensurePartitionsNow(final long timeoutSeconds) {
        final ScheduledExecutorService ex = this.executor;
        if (ex == null) {
            return runEnsure();
        }
        Future<List<Partition>> f = null;
        try {
            f = ex.submit(this::runEnsure);
            return f.get(timeoutSeconds, TimeUnit.SECONDS);
        } catch (TimeoutException e) {
            log.warn("Partition creation did not finish within {}s; the flush will proceed without it", timeoutSeconds);
            return List.of();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            return List.of();
        } catch (ExecutionException e) {
            log.error("Partition creation failed", e.getCause());
            return List.of();
        } catch (RejectedExecutionException e) {
            return runEnsure();
        }
    }

    private List<Partition> runEnsure() {
        try (Connection c = partitionManager.openConnection()) {
            final List<Partition> created = partitionManager.ensureFuturePartitions(c);
            partitionsCreated.mark(created.size());
            return created;
        } catch (SQLException e) {
            failures.mark();
            throw new IllegalStateException("Partition creation failed", e);
        }
    }

    private void runQuietly() {
        try {
            run();
        } catch (Throwable t) {
            // run() already records failures; this guards the executor against an unexpected escape (including an
            // Error such as NoClassDefFoundError after a bundle refresh), which would cancel all future runs.
            log.error("Unexpected error in partition maintenance", t);
            failures.mark();
        }
    }

    private void warnIfLegacyManaged() {
        if (legacyManagedCheck == null || Boolean.FALSE.equals(legacyManaged)) {
            return; // once the extension is gone it does not come back; stop checking
        }
        try {
            final boolean managed = legacyManagedCheck.getAsBoolean();
            if (managed && legacyManaged == null) {
                log.warn(LEGACY_WARNING);
            } else if (managed) {
                log.debug(LEGACY_WARNING);
            }
            legacyManaged = managed;
        } catch (RuntimeException e) {
            log.debug("Could not determine whether the legacy extension manages the table", e);
        }
    }

    private MaintenanceReport run() {
        warnIfLegacyManaged();
        final Timer.Context time = duration.time();
        final MaintenanceReport report;
        try {
            report = partitionManager.runMaintenance();
        } finally {
            time.stop();
        }
        partitionsCreated.mark(report.getCreated().size());
        partitionsRescued.mark(report.getRescued().size());
        partitionsDropped.mark(report.getDropped().size());
        partitionsCompressed.mark(report.getCompressed().size());
        failures.mark(report.getErrors().size());
        if (report.wasSkipped()) {
            skipped.mark();
        }
        this.lastReport = report;
        if (report.hasErrors()) {
            log.warn("Partition maintenance finished with errors: {}", report.summary());
        } else if (report.didWork()) {
            log.info("Partition maintenance: {}", report.summary());
        } else {
            log.debug("Partition maintenance: {}", report.summary());
        }
        return report;
    }
}
