
package org.opennms.timeseries.impl.pgtimeseries;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Queue;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executors;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import javax.sql.DataSource;

import org.opennms.integration.api.v1.timeseries.Aggregation;
import org.opennms.integration.api.v1.timeseries.MetaTagNames;
import org.opennms.integration.api.v1.timeseries.Metric;
import org.opennms.integration.api.v1.timeseries.Sample;
import org.opennms.integration.api.v1.timeseries.StorageException;
import org.opennms.integration.api.v1.timeseries.Tag;
import org.opennms.integration.api.v1.timeseries.TagMatcher;
import org.opennms.integration.api.v1.timeseries.TimeSeriesFetchRequest;
import org.opennms.integration.api.v1.timeseries.TimeSeriesStorage;
import org.opennms.integration.api.v1.timeseries.immutables.ImmutableMetric;
import org.opennms.integration.api.v1.timeseries.immutables.ImmutableSample;
import org.opennms.integration.api.v1.timeseries.immutables.ImmutableTag;
import org.opennms.timeseries.impl.pgtimeseries.config.PGTimeseriesConfig;
import org.opennms.timeseries.impl.pgtimeseries.util.DBUtils;
import org.opennms.timeseries.impl.pgtimeseries.util.PGTimeseriesDatabaseHelpers;
import org.opennms.timeseries.impl.pgtimeseries.util.PGTimeseriesDatabaseInitializer;
import org.slf4j.Logger;

import com.codahale.metrics.jmx.JmxReporter;
import com.codahale.metrics.Meter;
import com.codahale.metrics.MetricRegistry;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;

@Slf4j
@RequiredArgsConstructor
public class PGTimeseriesStorage implements TimeSeriesStorage {

    private static final Logger RATE_LIMITED_LOGGER = log;

    private final DataSource dataSource;
    private final PGTimeseriesConfig config;
    public final Map<String, Integer> MetricCache = new HashMap<String, Integer>();

    // Write-behind buffer. Samples accumulate across store() calls and are flushed when either
    // the buffered count reaches flushMaxSamples or flushMaxIntervalMs elapses, whichever comes first.
    private final Queue<Sample> buffer = new ConcurrentLinkedQueue<>();
    private final AtomicInteger bufferedCount = new AtomicInteger(0);
    private volatile ScheduledExecutorService flushExecutor;

    private final MetricRegistry metrics = new MetricRegistry();
    private final Meter samplesWritten = metrics.meter("samplesWritten");
    private final Meter samplesRead = metrics.meter("samplesRead");
    private final Meter samplesLost = metrics.meter("samplesLost");
    private final Meter samplesBuffered = metrics.meter("samplesBuffered");
    private final Meter cacheSize = metrics.meter("metricCacheSize");
    private final Meter cacheHit = metrics.meter("metricCacheHit");
    private final Meter cacheMiss = metrics.meter("metricCacheMiss");
    final JmxReporter reporter = JmxReporter.forRegistry(metrics).inDomain("org.opennms.timeseries.impl.pgtimeseries").build();

    public PGTimeseriesStorage(final PGTimeseriesConfig config, final DataSource dataSource) {
        this.config = Objects.requireNonNull(config);
        this.dataSource = Objects.requireNonNull(dataSource);
    }

    // ------------------------------------------------------------------------
    // Write path
    // ------------------------------------------------------------------------

    /**
     * Buffers the supplied samples and returns immediately. The buffer is drained to the database
     * either when it reaches {@code flushMaxSamples} or by the periodic flusher (bounded by
     * {@code flushMaxIntervalMs}), so a single flush may span many {@code store()} invocations
     * (i.e. many collection sets).
     */
    @Override
    public void store(List<Sample> entries) throws StorageException {
        if (entries == null || entries.isEmpty()) {
            return;
        }
        buffer.addAll(entries);
        samplesBuffered.mark(entries.size());
        int count = bufferedCount.addAndGet(entries.size());
        if (count >= config.getFlushMaxSamples()) {
            triggerFlush();
        }
    }

    /** Asynchronously requests a flush on the dedicated flush thread; never blocks the caller. */
    private void triggerFlush() {
        final ScheduledExecutorService ex = this.flushExecutor;
        if (ex != null && !ex.isShutdown()) {
            try {
                ex.execute(this::flushQuietly);
            } catch (RejectedExecutionException ignore) {
                // executor is shutting down; the final drain in destroy() will handle the remainder
            }
        } else {
            // not initialized (e.g. unit test calling store() before init()) -> flush inline
            flushQuietly();
        }
    }

    private void flushQuietly() {
        try {
            drainBuffer();
        } catch (StorageException e) {
            RATE_LIMITED_LOGGER.error("Scheduled flush failed; some samples may be lost.", e);
        }
    }

    /**
     * Drains the entire buffer to the database in transactions no larger than the flush cap. Always
     * invoked on the single flush thread (or directly during shutdown), so it never runs concurrently
     * with itself.
     */
    private void drainBuffer() throws StorageException {
        final int cap = Math.max(1, Math.max(config.getFlushMaxSamples(), config.getMaxBatchSize()));
        while (true) {
            final List<Sample> batch = new ArrayList<>(Math.min(cap, 1024));
            Sample sample;
            while (batch.size() < cap && (sample = buffer.poll()) != null) {
                batch.add(sample);
                bufferedCount.decrementAndGet();
            }
            if (batch.isEmpty()) {
                return;
            }
            writeSamples(batch);
        }
    }

    /**
     * Ensures all currently-buffered samples are persisted before a read, preserving read-after-write
     * consistency regardless of the buffering settings.
     */
    private void flushPending() throws StorageException {
        if (buffer.isEmpty()) {
            return;
        }
        final ScheduledExecutorService ex = this.flushExecutor;
        if (ex == null || ex.isShutdown()) {
            drainBuffer();
            return;
        }
        try {
            // Run the drain on the flush thread so it serializes with periodic/triggered flushes.
            ex.submit((Callable<Void>) () -> {
                drainBuffer();
                return null;
            }).get();
        } catch (InterruptedException ie) {
            Thread.currentThread().interrupt();
            throw new StorageException(ie);
        } catch (ExecutionException ee) {
            final Throwable cause = ee.getCause();
            if (cause instanceof StorageException) {
                throw (StorageException) cause;
            }
            throw new StorageException(cause != null ? cause : ee);
        } catch (RejectedExecutionException ree) {
            // executor shut down between the check and submit; drain inline
            drainBuffer();
        }
    }

    /**
     * Forces all currently-buffered samples to be written to the database before returning. Intended for
     * callers that need a durability guarantee at a known point, such as a bulk import that wants to confirm
     * everything was persisted before it reports completion.
     */
    public void flush() throws StorageException {
        flushPending();
    }

    /** Persists one batch of samples within a single transaction. */
    private void writeSamples(final List<Sample> samples) throws StorageException {
        final String sampleSql = "INSERT INTO pgtimeseries_time_series(time, keyid, value) values (?, ?, ?)";
        // DO UPDATE (rather than DO NOTHING) guarantees the row is RETURNED even on a concurrent-insert conflict,
        // avoiding a null keyid. This branch only runs on a metric-cache miss (~once per metric per process).
        final String metricSql = "INSERT INTO pgtimeseries_metric(keyid, key) VALUES (nextval('pgtimeseries_metric_seq'), ?) "
                + "ON CONFLICT (key) DO UPDATE SET key = EXCLUDED.key RETURNING keyid";

        final DBUtils db = new DBUtils(this.getClass());
        Connection connection = null;
        int written = 0;
        try {
            connection = openConnection();
            db.watch(connection);
            connection.setAutoCommit(false);

            final PreparedStatement mps = connection.prepareStatement(metricSql);
            final PreparedStatement sps = connection.prepareStatement(sampleSql);
            db.watch(mps);
            db.watch(sps);

            int pending = 0;
            for (final Sample sample : samples) {
                final String key = sample.getMetric().getKey();
                Integer keyid = MetricCache.get(key);
                if (keyid != null) {
                    cacheHit.mark();
                } else {
                    cacheMiss.mark();
                    keyid = resolveOrCreateMetric(connection, mps, sample.getMetric());
                    if (keyid == null) {
                        RATE_LIMITED_LOGGER.warn("Could not resolve a keyid for metric {}; skipping sample.", key);
                        samplesLost.mark();
                        continue;
                    }
                    MetricCache.put(key, keyid);
                    cacheSize.mark();
                }
                sps.setTimestamp(1, new Timestamp(sample.getTime().toEpochMilli()));
                sps.setInt(2, keyid);
                sps.setDouble(3, sample.getValue());
                sps.addBatch();
                if (++pending >= config.getMaxBatchSize()) {
                    sps.executeBatch();
                    written += pending;
                    pending = 0;
                }
            }
            if (pending > 0) {
                sps.executeBatch();
                written += pending;
            }
            connection.commit();
            samplesWritten.mark(written);
            if (log.isDebugEnabled()) {
                log.debug("Flushed {} samples to the database", written);
            }
        } catch (SQLException e) {
            RATE_LIMITED_LOGGER.error("An error occurred while inserting samples. {} samples may be lost.",
                    samples.size() - written, e);
            samplesLost.mark(samples.size() - written);
            if (connection != null) {
                try {
                    connection.rollback();
                } catch (SQLException re) {
                    log.warn("Rollback failed after a failed flush.", re);
                }
            }
            throw new StorageException(e);
        } finally {
            db.cleanUp();
        }
    }

    /**
     * Resolves the keyid for a metric, creating the metric row if needed. On creation (or first sight of an
     * existing metric this process) the metric's tags are written once. Idempotent via ON CONFLICT.
     */
    private Integer resolveOrCreateMetric(final Connection connection, final PreparedStatement mps, final Metric metric)
            throws SQLException {
        final String key = metric.getKey();
        Integer keyid = getMetricID(connection, key);
        if (keyid == null) {
            mps.setString(1, key);
            try (ResultSet mrs = mps.executeQuery()) {
                if (mrs.next()) {
                    keyid = mrs.getInt("keyid");
                }
            }
        }
        if (keyid != null) {
            // Written once per metric per process (only reached on a cache miss), not once per sample.
            storeTags(connection, keyid, ImmutableMetric.TagType.intrinsic, metric.getIntrinsicTags());
            storeTags(connection, keyid, ImmutableMetric.TagType.meta, metric.getMetaTags());
            storeTags(connection, keyid, ImmutableMetric.TagType.external, metric.getExternalTags());
        }
        return keyid;
    }

    private void storeTags(final Connection connection, final Integer keyid, final ImmutableMetric.TagType tagType, final Collection<Tag> tags) throws SQLException {
        if (tags == null || tags.isEmpty()) {
            return;
        }
        final String sql = "INSERT INTO pgtimeseries_tag(keyid, key, value, type) values (?, ?, ?, ?) ON CONFLICT (keyid, key, value, type) DO NOTHING";
        try (PreparedStatement ps = connection.prepareStatement(sql)) {
            for (final Tag tag : tags) {
                ps.setInt(1, keyid);
                ps.setString(2, tag.getKey());
                ps.setString(3, tag.getValue());
                ps.setString(4, tagType.name());
                ps.addBatch();
            }
            ps.executeBatch();
        }
    }

    // ------------------------------------------------------------------------
    // Read path
    // ------------------------------------------------------------------------

    @Override
    public List<Metric> findMetrics(Collection<TagMatcher> matchers) throws StorageException {
        Objects.requireNonNull(matchers, "tags collection can not be null");
        if (matchers.isEmpty()) {
            throw new IllegalArgumentException("Collection<TagMatcher> can not be empty");
        }
        flushPending();

        final List<TagMatcher> matcherList = new ArrayList<>(matchers);
        final DBUtils db = new DBUtils(this.getClass());
        Connection connection = null;
        try {
            connection = openConnection();
            db.watch(connection);

            final String sql = createMetricsSQL(matcherList);
            final PreparedStatement ps = connection.prepareStatement(sql);
            db.watch(ps);
            int idx = 1;
            for (final TagMatcher matcher : matcherList) {
                ps.setString(idx++, matcher.getKey());
                ps.setString(idx++, matcher.getValue());
            }
            final ResultSet rs = ps.executeQuery();
            db.watch(rs);
            final Set<Integer> metricKeys = new HashSet<>();
            while (rs.next()) {
                metricKeys.add(rs.getInt("keyid"));
            }
            rs.close();

            return loadMetrics(connection, db, metricKeys);
        } catch (SQLException e) {
            throw new StorageException(e);
        } finally {
            db.cleanUp();
        }
    }

    private Integer getMetricID(final Connection connection, final String key) throws SQLException {
        final String sql = "select keyid from pgtimeseries_metric where key = ?";
        try (PreparedStatement ps = connection.prepareStatement(sql)) {
            ps.setString(1, key);
            try (ResultSet rs = ps.executeQuery()) {
                // unique constraint on key => at most one row
                if (rs.next()) {
                    return rs.getInt("keyid");
                }
            }
        }
        return null;
    }

    /** Loads the metrics (with their tags) for the supplied keyids in a single query. */
    private List<Metric> loadMetrics(Connection connection, DBUtils db, Collection<Integer> metricKeys) throws SQLException {
        final List<Metric> result = new ArrayList<>();
        if (metricKeys == null || metricKeys.isEmpty()) {
            return result;
        }
        final Long[] ids = metricKeys.stream()
                .filter(Objects::nonNull)
                .map(Integer::longValue)
                .toArray(Long[]::new);
        if (ids.length == 0) {
            return result;
        }

        final String sql = "SELECT keyid, key, value, type FROM pgtimeseries_tag WHERE keyid = ANY(?) ORDER BY keyid";
        final PreparedStatement ps = connection.prepareStatement(sql);
        db.watch(ps);
        ps.setArray(1, connection.createArrayOf("bigint", ids));
        final ResultSet rs = ps.executeQuery();
        db.watch(rs);

        Integer currentKeyid = null;
        ImmutableMetric.MetricBuilder metric = null;
        boolean intrinsicTagAvailable = false;
        while (rs.next()) {
            final int keyid = rs.getInt("keyid");
            if (currentKeyid == null || keyid != currentKeyid) {
                if (metric != null && intrinsicTagAvailable) {
                    // create a metric only if at least one intrinsic tag is available
                    result.add(metric.build());
                }
                currentKeyid = keyid;
                metric = ImmutableMetric.builder();
                intrinsicTagAvailable = false;
            }
            final Tag tag = new ImmutableTag(rs.getString("key"), rs.getString("value"));
            final ImmutableMetric.TagType type = ImmutableMetric.TagType.valueOf(rs.getString("type"));
            switch (type) {
                case intrinsic:
                    metric.intrinsicTag(tag);
                    intrinsicTagAvailable = true;
                    break;
                case meta:
                    metric.metaTag(tag);
                    break;
                case external:
                    metric.externalTag(tag);
                    break;
                default:
                    throw new IllegalArgumentException("Unknown ImmutableMetric.TagType " + type);
            }
        }
        if (metric != null && intrinsicTagAvailable) {
            result.add(metric.build());
        }
        rs.close();
        return result;
    }

    /**
     * Builds a parameterized query that returns the keyids matching every supplied matcher. Each matcher
     * contributes one {@code SELECT keyid ... WHERE key = ? AND value <op> ?} term; terms are combined with
     * INTERSECT so a metric must satisfy all matchers. Keys and values are always bound parameters (never
     * concatenated), so tag values cannot inject SQL.
     */
    String createMetricsSQL(Collection<TagMatcher> matchers) {
        Objects.requireNonNull(matchers, "matchers collection can not be null");
        final StringBuilder b = new StringBuilder();
        int i = 0;
        for (final TagMatcher matcher : matchers) {
            if (i++ > 0) {
                b.append(" INTERSECT ");
            }
            b.append("SELECT keyid FROM pgtimeseries_tag WHERE key = ? AND value ")
             .append(tagMatcherToComp(matcher))
             .append(" ?");
        }
        return b.toString();
    }

    private String tagMatcherToComp(final TagMatcher matcher) {
        // see https://www.postgresql.org/docs/current/functions-matching.html#FUNCTIONS-POSIX-REGEXP
        Objects.requireNonNull(matcher);
        switch (matcher.getType()) {
            case EQUALS:
                return "=";
            case NOT_EQUALS:
                return "!=";
            case EQUALS_REGEX:
                return "~";
            case NOT_EQUALS_REGEX:
                return "!~";
            default:
                throw new IllegalArgumentException("Unknown TagMatcher.Type " + matcher.getType().name());
        }
    }

    // Counter / gauge fetch templates. Raw rows are assigned to a bucket once via date_bin() (PG14+) and the
    // result is equi-joined to a generate_series() of bucket starts (RIGHT/LEFT join fills empty buckets with NaN).
    // This replaces the previous interval range-join, which scanned buckets x samples.
    private static final String COUNTER_SQL =
            "WITH buckets AS ( " +
                "SELECT n AS start_time FROM generate_series('%s', '%s', '%s seconds'::interval) AS n " +
            "), deltas AS ( " +
                "SELECT date_bin('%s seconds'::interval, time, '%s') AS bucket, " +
                       "value - lag(value) OVER (ORDER BY time) AS delta " +
                "FROM pgtimeseries_time_series " +
                "WHERE keyid = ? AND time > '%s' AND time < '%s' " +
            "), binned AS ( " +
                // RRDtool-style counter-wrap correction: a negative delta means the counter overflowed.
                // First add 2^32 (assume a 32-bit counter); if it is still negative it must have been a
                // 64-bit counter, so add 2^64. If the result is STILL negative the value is not a plausible
                // wrap (e.g. a counter reset), so emit NULL -> NaN rather than a bogus value. Values are
                // double precision, matching RRDtool's own (double-based) arithmetic.
                "SELECT bucket, " +
                       "CASE " +
                           "WHEN delta >= 0 THEN delta " +
                           "WHEN delta + 4294967296::double precision >= 0 THEN delta + 4294967296::double precision " +
                           "WHEN delta + 18446744073709551616::double precision >= 0 THEN delta + 18446744073709551616::double precision " +
                           "ELSE NULL " +
                       "END AS deltaval " +
                "FROM deltas " +
            ") " +
            "SELECT b.start_time AS step, COALESCE(%s(d.deltaval) / %s, 'NaN') AS aggregation " +
            "FROM buckets b LEFT JOIN binned d ON d.bucket = b.start_time " +
            "GROUP BY b.start_time ORDER BY b.start_time";

    private static final String GAUGE_AGGR_SQL =
            "WITH buckets AS ( " +
                "SELECT n AS start_time FROM generate_series('%s', '%s', '%s seconds'::interval) AS n " +
            "), binned AS ( " +
                "SELECT date_bin('%s seconds'::interval, time, '%s') AS bucket, value AS raw_value " +
                "FROM pgtimeseries_time_series " +
                "WHERE keyid = ? AND time > '%s' AND time < '%s' " +
            ") " +
            "SELECT b.start_time AS step, COALESCE(%s(d.raw_value), 'NaN') AS aggregation " +
            "FROM buckets b LEFT JOIN binned d ON d.bucket = b.start_time " +
            "GROUP BY b.start_time ORDER BY b.start_time";

    private static final String GAUGE_NONE_SQL =
            "WITH buckets AS ( " +
                "SELECT n AS start_time FROM generate_series('%s', '%s', '%s seconds'::interval) AS n " +
            "), binned AS ( " +
                "SELECT date_bin('%s seconds'::interval, time, '%s') AS bucket, value AS raw_value, time " +
                "FROM pgtimeseries_time_series " +
                "WHERE keyid = ? AND time > '%s' AND time < '%s' " +
            "), latest AS ( " +
                "SELECT DISTINCT ON (bucket) bucket, raw_value FROM binned ORDER BY bucket, time DESC " +
            ") " +
            "SELECT b.start_time AS step, COALESCE(d.raw_value, 'NaN') AS aggregation " +
            "FROM buckets b LEFT JOIN latest d ON d.bucket = b.start_time " +
            "ORDER BY b.start_time";

    /**
     * Builds the fetch SQL for a metric. Counters are returned as a rate (delta over the bucket divided by the
     * step); gauges return either the latest raw value per bucket (aggregation NONE) or the aggregate. The single
     * bind parameter ({@code ?}) is the keyid. Extracted from {@link #getTimeseries} so it can be unit-tested
     * without a database. A non-positive step is coerced to 1 second, since generate_series/date_bin require a
     * positive stride.
     */
    String buildFetchSql(final String mtype, final Aggregation aggregation, final Timestamp start, final Timestamp end, final long step) {
        final long stepInSeconds = step <= 0 ? 1 : step;
        if (Metric.Mtype.count.name().equals(mtype) || Metric.Mtype.counter.name().equals(mtype)) {
            final String aggr = (Aggregation.NONE == aggregation) ? "sum" : toSql(aggregation);
            return String.format(COUNTER_SQL, start, end, stepInSeconds, stepInSeconds, start, start, end, aggr, stepInSeconds);
        } else if (Aggregation.NONE == aggregation) {
            return String.format(GAUGE_NONE_SQL, start, end, stepInSeconds, stepInSeconds, start, start, end);
        } else {
            return String.format(GAUGE_AGGR_SQL, start, end, stepInSeconds, stepInSeconds, start, start, end, toSql(aggregation));
        }
    }

    @Override
    public List<Sample> getTimeseries(TimeSeriesFetchRequest request) throws StorageException {
        flushPending();

        final DBUtils db = new DBUtils(this.getClass());
        Connection connection = null;
        try {
            connection = openConnection();
            db.watch(connection);

            final String key = request.getMetric().getKey();
            Integer keyid = MetricCache.get(key);
            if (keyid != null) {
                cacheHit.mark();
            } else {
                keyid = getMetricID(connection, key);
                cacheMiss.mark();
                MetricCache.put(key, keyid);
                cacheSize.mark();
            }

            final List<Metric> metrics = loadMetrics(connection, db, Collections.singletonList(keyid));
            if (metrics.isEmpty()) {
                // we didn't find the metric => nothing to do.
                return Collections.emptyList();
            }
            final Metric metric = metrics.get(0);
            final Timestamp start = new Timestamp(request.getStart().toEpochMilli());
            final Timestamp end = new Timestamp(request.getEnd().toEpochMilli());
            final String type = metric.getFirstTagByKey(MetaTagNames.mtype).getValue();
            final String sql = buildFetchSql(type, request.getAggregation(), start, end, request.getStep().getSeconds());

            final PreparedStatement statement = connection.prepareStatement(sql);
            db.watch(statement);
            statement.setInt(1, keyid);
            final ResultSet rs = statement.executeQuery();
            db.watch(rs);

            final ArrayList<Sample> samples = new ArrayList<>();
            while (rs.next()) {
                final long timestamp = rs.getTimestamp("step").getTime();
                samples.add(ImmutableSample.builder()
                        .metric(metric)
                        .time(Instant.ofEpochMilli(timestamp))
                        .value(rs.getDouble("aggregation"))
                        .build());
                samplesRead.mark();
            }
            rs.close();
            return samples;
        } catch (SQLException e) {
            log.error("Could not retrieve FetchResults", e);
            throw new StorageException(e);
        } finally {
            db.cleanUp();
        }
    }

    @Override
    public void delete(final Metric metric) throws StorageException {
        flushPending();

        final DBUtils db = new DBUtils(this.getClass());
        Connection connection = null;
        try {
            connection = openConnection();
            db.watch(connection);

            Integer keyid = MetricCache.get(metric.getKey());
            if (keyid == null) {
                keyid = getMetricID(connection, metric.getKey());
            }
            if (keyid == null) {
                log.debug("No metric found for key {}; nothing to delete.", metric.getKey());
                return;
            }

            PreparedStatement statement = connection.prepareStatement("DELETE FROM pgtimeseries_time_series WHERE keyid = ?");
            db.watch(statement);
            statement.setInt(1, keyid);
            final int deletedTimeseriesEntries = statement.executeUpdate();

            statement = connection.prepareStatement("DELETE FROM pgtimeseries_tag WHERE keyid = ?");
            db.watch(statement);
            statement.setInt(1, keyid);
            final int deletedTimeseriesTags = statement.executeUpdate();

            statement = connection.prepareStatement("DELETE FROM pgtimeseries_metric WHERE keyid = ?");
            db.watch(statement);
            statement.setInt(1, keyid);
            statement.executeUpdate();

            MetricCache.remove(metric.getKey());
            log.debug("Deleted {} timeseries entries and {} timeseries tags for metric {}",
                    deletedTimeseriesEntries, deletedTimeseriesTags, metric);
        } catch (SQLException e) {
            log.error("Could not delete metric {}", metric, e);
            throw new StorageException(e);
        } finally {
            db.cleanUp();
        }
    }

    private String toSql(final Aggregation aggregation) {
        if (Aggregation.AVERAGE == aggregation) {
            return "avg";
        } else if (Aggregation.MAX == aggregation) {
            return "max";
        } else if (Aggregation.MIN == aggregation) {
            return "min";
        } else {
            throw new IllegalArgumentException("Unknown aggregation " + aggregation);
        }
    }

    @Override
    public boolean supportsAggregation(final Aggregation aggregation) {
        return aggregation == Aggregation.MAX || aggregation == Aggregation.MIN || aggregation == Aggregation.AVERAGE;
    }

    // ------------------------------------------------------------------------
    // Lifecycle / helpers
    // ------------------------------------------------------------------------

    private Connection openConnection() throws SQLException {
        if (PGTimeseriesDatabaseHelpers.isExternalDatasourceURLAvailable()) {
            final Connection c = PGTimeseriesDatabaseHelpers.getWhichDataSourceConnection();
            log.trace("Got external datasource connection: {}", c);
            return c;
        }
        return this.dataSource.getConnection();
    }

    public void init() throws StorageException {
        try {
            new PGTimeseriesDatabaseInitializer(this.dataSource, this.config)
                    .initializeIfNeeded();
        } catch (final SQLException e) {
            throw new StorageException(e);
        }

        final ScheduledExecutorService ex = Executors.newSingleThreadScheduledExecutor(r -> {
            final Thread t = new Thread(r, "pgtimeseries-flush");
            t.setDaemon(true);
            return t;
        });
        final long interval = Math.max(1L, config.getFlushMaxIntervalMs());
        // fixed delay (not fixed rate) so a slow flush can't cause flushes to pile up
        ex.scheduleWithFixedDelay(this::flushQuietly, interval, interval, TimeUnit.MILLISECONDS);
        this.flushExecutor = ex;

        reporter.start();
    }

    public void destroy() {
        final ScheduledExecutorService ex = this.flushExecutor;
        if (ex != null) {
            ex.shutdown();
            try {
                ex.awaitTermination(30, TimeUnit.SECONDS);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
            // Drain whatever remains on the shutdown thread (no flush thread is running now).
            try {
                drainBuffer();
            } catch (StorageException e) {
                log.error("Final flush on shutdown failed; buffered samples may be lost.", e);
            }
            ex.shutdownNow();
        }
        reporter.stop();
        PGTimeseriesDatabaseHelpers.ExtReporter.stop(); // janky
    }

    public MetricRegistry getMetrics() {
        return metrics;
    }
}
