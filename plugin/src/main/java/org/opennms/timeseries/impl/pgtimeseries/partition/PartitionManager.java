package org.opennms.timeseries.impl.pgtimeseries.partition;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Types;
import java.time.Clock;
import java.time.Instant;
import java.time.OffsetDateTime;
import java.time.ZoneId;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.Objects;
import java.util.stream.Collectors;

import lombok.extern.slf4j.Slf4j;

/**
 * Manages the range partitions of the time series table with plain SQL, replacing what pg_timeseries,
 * pg_partman and pg_cron used to do for this plugin.
 *
 * <p>Design points:</p>
 * <ul>
 *   <li>Partitions are discovered from the catalog by their <em>bounds</em>, never by name, so partitions
 *       created by pg_partman, by a DBA, or by another OpenNMS instance are all honoured.</li>
 *   <li>New partitions follow the pg_partman naming convention ({@code <parent>_pYYYYMMDD}, with
 *       {@code _HH24MISS} appended for sub-day widths) and are aligned to the existing partition lattice, so a
 *       database set up by the 0.2.x plugin keeps a uniform layout.</li>
 *   <li>Partition bounds are computed by the database, in the JDBC session's timezone, which is what
 *       pg_partman did in the pg_cron session. Names and day alignment therefore match the old scheme whenever
 *       the OpenNMS JVM and the PostgreSQL server share a timezone.</li>
 *   <li>Each maintenance step runs in its own transaction and is isolated from the others; a failure is
 *       recorded in the {@link MaintenanceReport} and never thrown into the write path.</li>
 *   <li>A session-level advisory lock serialises maintenance across OpenNMS instances sharing a database.</li>
 * </ul>
 *
 * <p>All "now" values come from the injected {@link Clock} and are passed to SQL as parameters, so the
 * behaviour is testable with a fixed clock.</p>
 */
@Slf4j
public class PartitionManager {

    /** Functional interface for whatever hands out JDBC connections (local pool, external pool, test datasource). */
    @FunctionalInterface
    public interface ConnectionProvider {
        Connection get() throws SQLException;
    }

    /**
     * Receives the failure of one item (one partition to create, drop, move into or compress) so the step can
     * carry on with the next item. The default sink used by the direct step methods rethrows.
     */
    @FunctionalInterface
    public interface ErrorSink {
        void failed(String item, SQLException e) throws SQLException;
    }

    private static final ErrorSink RETHROW = (item, e) -> {
        throw e;
    };

    /** Key of the session advisory lock held during a maintenance run. Arbitrary but fixed. */
    public static final long ADVISORY_LOCK_KEY = 0x7067_7473_6d61_696eL;

    /** Upper bound on partitions created by one {@link #ensureFuturePartitions} call, as a misconfiguration guard. */
    public static final int MAX_PARTITIONS_PER_RUN = 1000;

    /** Upper bound on default-partition rescues per run; the rest are picked up on the next run. */
    public static final int MAX_RESCUES_PER_RUN = 100;

    /** Formats an instant as a PostgreSQL timestamptz literal (always rendered in UTC with an explicit offset). */
    private static final DateTimeFormatter TS_LITERAL =
            DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss.SSSSSSxxx").withZone(ZoneOffset.UTC);

    /**
     * Partition name suffixes, as pg_partman's to_char(..., 'YYYYMMDD') / 'YYYYMMDD_HH24MISS' produced them.
     * Day-or-wider names are rendered in the JVM's default timezone (resolved at call time), which is also the
     * timezone pgjdbc gives the session, so they match what the database itself would render. Sub-day names are
     * rendered in UTC so that a DST fall-back cannot give two partitions the same name.
     */
    private static final DateTimeFormatter DAY_SUFFIX = DateTimeFormatter.ofPattern("yyyyMMdd");
    private static final DateTimeFormatter SUBDAY_SUFFIX = DateTimeFormatter.ofPattern("yyyyMMdd_HHmmss").withZone(ZoneOffset.UTC);

    private static final String NO_COLUMNAR_LOG_ONCE = "citus_columnar is not installed; compression policy is ignored. "
            + "Install the extension (or clear the policy with ts-config) to stop seeing this message.";

    private final ConnectionProvider connections;
    private final String parentTable;
    private final String configTable;
    private final String backfillStart;
    private final Clock clock;
    private volatile boolean columnarMissingLogged;

    /**
     * @param connections   source of JDBC connections
     * @param parentTable   unqualified name of the partitioned time series table (resolved via search_path)
     * @param configTable   unqualified name of the plugin's policy table
     * @param backfillStart optional timestamptz literal; when no range partitions exist yet, the first partition
     *                      starts here instead of {@code now - lead_time}
     * @param clock         source of "now"
     */
    public PartitionManager(final ConnectionProvider connections, final String parentTable, final String configTable,
                            final String backfillStart, final Clock clock) {
        this.connections = Objects.requireNonNull(connections);
        this.parentTable = Objects.requireNonNull(parentTable);
        this.configTable = Objects.requireNonNull(configTable);
        this.backfillStart = (backfillStart == null || backfillStart.isBlank()) ? null : backfillStart.trim();
        this.clock = Objects.requireNonNull(clock);
    }

    public String getParentTable() {
        return parentTable;
    }

    /** A connection from the configured provider; the caller closes it. */
    public Connection openConnection() throws SQLException {
        return connections.get();
    }

    // ------------------------------------------------------------------------
    // Maintenance run
    // ------------------------------------------------------------------------

    /**
     * Runs one full maintenance pass: rescue stray rows from the default partition, create partitions through
     * the lead time, drop partitions past retention, compress partitions past the compression offset. Each step
     * is independent; errors are collected in the report rather than thrown.
     */
    public MaintenanceReport runMaintenance() {
        return runMaintenance(false);
    }

    /**
     * As {@link #runMaintenance()}; with {@code dryRun} nothing is changed and the report lists what a real run
     * would do right now.
     */
    public MaintenanceReport runMaintenance(final boolean dryRun) {
        final boolean apply = !dryRun;
        final MaintenanceReport report = new MaintenanceReport(Instant.now(clock), dryRun);
        try (Connection c = connections.get()) {
            c.setAutoCommit(true);
            if (!schemaInstalled(c)) {
                report.skipped("table " + configTable + " or " + parentTable + " does not exist (schema not installed)");
                return report;
            }
            if (!tryLock(c)) {
                report.skipped("another maintenance run holds the advisory lock");
                return report;
            }
            try {
                final PartitionPolicy policy = loadPolicy(c);
                if (policy == null) {
                    report.skipped("no policy row for " + parentTable + " in " + configTable);
                    return report;
                }
                final Instant now = Instant.now(clock);
                final ErrorSink sink = (item, e) -> report.error(item, e);
                step(report, "rescue", () -> report.rescued().addAll(names(rescueDefaultPartition(c, policy, now, apply, sink))));
                step(report, "create", () -> report.created().addAll(names(ensureFuturePartitions(c, policy, now, apply, sink))));
                step(report, "retention", () -> report.dropped().addAll(names(applyRetention(c, policy, now, apply, sink))));
                step(report, "compression", () -> report.compressed().addAll(names(applyCompression(c, policy, now, apply, sink))));
            } finally {
                unlock(c);
            }
        } catch (SQLException e) {
            log.error("Partition maintenance could not obtain a database connection", e);
            report.error("connection", e);
        } finally {
            report.finished(Instant.now(clock));
        }
        return report;
    }

    @FunctionalInterface
    private interface Step {
        void run() throws Exception;
    }

    private void step(final MaintenanceReport report, final String name, final Step step) {
        try {
            step.run();
        } catch (Exception e) {
            log.error("Partition maintenance step '{}' failed", name, e);
            report.error(name, e);
        }
    }

    private static List<String> names(final List<Partition> partitions) {
        return partitions.stream().map(Partition::name).collect(Collectors.toList());
    }

    // ------------------------------------------------------------------------
    // Policy and catalog access
    // ------------------------------------------------------------------------

    /** Reads the policy row for the parent table; null if there is none. */
    public PartitionPolicy loadPolicy(final Connection c) throws SQLException {
        final String sql = "SELECT partition_duration::text, partition_lead_time::text, retention_duration::text, "
                + "compression_duration::text FROM " + quoteIdent(configTable) + " WHERE table_name = ?";
        try (PreparedStatement ps = c.prepareStatement(sql)) {
            ps.setString(1, parentTable);
            try (ResultSet rs = ps.executeQuery()) {
                if (!rs.next()) {
                    return null;
                }
                return new PartitionPolicy(rs.getString(1), rs.getString(2), rs.getString(3), rs.getString(4));
            }
        }
    }

    /** Lists every child partition of the parent table with sizes, default partition first, then by lower bound. */
    public List<Partition> listPartitions(final Connection c) throws SQLException {
        return listPartitions(c, true);
    }

    /**
     * As above; {@code withSizes = false} skips the three size functions (which stat every relation file), which
     * is what the maintenance steps use. Unbounded bounds written by hand as MINVALUE / MAXVALUE are reported as
     * -infinity / infinity (OffsetDateTime.MIN / MAX in pgjdbc).
     */
    public List<Partition> listPartitions(final Connection c, final boolean withSizes) throws SQLException {
        final String sizes = withSizes
                ? "pg_table_size(c.oid), pg_indexes_size(c.oid), pg_total_relation_size(c.oid), "
                : "0::bigint, 0::bigint, 0::bigint, ";
        final String sql = "SELECT n.nspname, c.relname, "
                + "pg_get_expr(c.relpartbound, c.oid) = 'DEFAULT' AS is_default, "
                + "CASE WHEN b[1] = 'MINVALUE' THEN '-infinity'::timestamptz ELSE btrim(b[1], '''')::timestamptz END AS lower_bound, "
                + "CASE WHEN b[2] = 'MAXVALUE' THEN 'infinity'::timestamptz ELSE btrim(b[2], '''')::timestamptz END AS upper_bound, "
                + sizes + "am.amname "
                + "FROM pg_inherits i "
                + "JOIN pg_class c ON c.oid = i.inhrelid "
                + "JOIN pg_namespace n ON n.oid = c.relnamespace "
                + "LEFT JOIN pg_am am ON am.oid = c.relam "
                + "CROSS JOIN LATERAL regexp_match(pg_get_expr(c.relpartbound, c.oid), "
                + "'FROM \\((MINVALUE|''[^'']+'')\\) TO \\((MAXVALUE|''[^'']+'')\\)') AS b "
                + "WHERE i.inhparent = ?::regclass "
                + "ORDER BY is_default DESC, lower_bound, c.relname";
        final List<Partition> result = new ArrayList<>();
        try (PreparedStatement ps = c.prepareStatement(sql)) {
            ps.setString(1, parentTable);
            try (ResultSet rs = ps.executeQuery()) {
                while (rs.next()) {
                    result.add(new Partition(
                            rs.getString(1),
                            rs.getString(2),
                            rs.getBoolean(3),
                            toInstant(rs.getObject(4, OffsetDateTime.class)),
                            toInstant(rs.getObject(5, OffsetDateTime.class)),
                            rs.getLong(6),
                            rs.getLong(7),
                            rs.getLong(8),
                            rs.getString(9)));
                }
            }
        }
        return result;
    }

    /** Convenience wrapper that opens its own connection. */
    public List<Partition> listPartitions() throws SQLException {
        try (Connection c = connections.get()) {
            return listPartitions(c);
        }
    }

    private static Instant toInstant(final OffsetDateTime odt) {
        return odt == null ? null : odt.toInstant();
    }

    private String parentSchema(final Connection c) throws SQLException {
        final String sql = "SELECT n.nspname FROM pg_class c JOIN pg_namespace n ON n.oid = c.relnamespace WHERE c.oid = ?::regclass";
        try (PreparedStatement ps = c.prepareStatement(sql)) {
            ps.setString(1, parentTable);
            try (ResultSet rs = ps.executeQuery()) {
                if (!rs.next()) {
                    throw new SQLException("Table " + parentTable + " does not exist");
                }
                return rs.getString(1);
            }
        }
    }

    private boolean schemaInstalled(final Connection c) throws SQLException {
        final String sql = "SELECT to_regclass(?) IS NOT NULL AND to_regclass(?) IS NOT NULL";
        try (PreparedStatement ps = c.prepareStatement(sql)) {
            ps.setString(1, parentTable);
            ps.setString(2, configTable);
            try (ResultSet rs = ps.executeQuery()) {
                return rs.next() && rs.getBoolean(1);
            }
        }
    }

    // ------------------------------------------------------------------------
    // Step 1: create partitions through the lead time
    // ------------------------------------------------------------------------

    /** Loads the policy and creates whatever partitions are missing through {@code now + lead_time}. */
    public List<Partition> ensureFuturePartitions(final Connection c) throws SQLException {
        final PartitionPolicy policy = loadPolicy(c);
        if (policy == null) {
            throw new SQLException("No policy row for " + parentTable + " in " + configTable);
        }
        return ensureFuturePartitions(c, policy, Instant.now(clock));
    }

    /**
     * Creates the partitions needed so that every instant from {@code now} to {@code now + lead_time} has a
     * partition. When no range partitions exist yet (first install) the set is bootstrapped the way pg_partman
     * did it: from {@code backfillStart}, or {@code now - lead_time}, truncated to a calendar boundary that suits
     * the partition width, through {@code now + lead_time}. Existing partitions anchor the lattice so new ones
     * line up with them. Gaps are filled, and partitions far in the future are left alone.
     *
     * @return the partitions created, in ascending order
     */
    public List<Partition> ensureFuturePartitions(final Connection c, final PartitionPolicy policy, final Instant now)
            throws SQLException {
        return ensureFuturePartitions(c, policy, now, true);
    }

    /** As above; with {@code apply = false} nothing is created and the partitions that would be are returned. */
    public List<Partition> ensureFuturePartitions(final Connection c, final PartitionPolicy policy, final Instant now,
                                                  final boolean apply) throws SQLException {
        return ensureFuturePartitions(c, policy, now, apply, RETHROW);
    }

    /** As above; a failure to create one partition goes to {@code errors} and the others are still attempted. */
    public List<Partition> ensureFuturePartitions(final Connection c, final PartitionPolicy policy, final Instant now,
                                                  final boolean apply, final ErrorSink errors) throws SQLException {
        final String schema = parentSchema(c);
        final List<Partition> ranges = rangePartitions(listPartitions(c, false));
        final boolean bootstrap = ranges.isEmpty();
        final Instant to = plusInterval(c, now, policy.partitionLeadTime());
        final Instant anchor;
        final Instant from;
        if (bootstrap) {
            anchor = bootstrapBase(c, policy, now);
            from = anchor;
        } else {
            // Continue the series from where the newest partition that starts at or before now ends (so a change
            // of partition width starts a fresh lattice right there), falling back to the earliest partition's
            // lower bound when every existing one is in the future.
            final Partition current = ranges.stream()
                    .filter(p -> !p.lowerBound().isAfter(now))
                    .max(Comparator.comparing(Partition::lowerBound))
                    .orElse(null);
            if (current != null && !current.upperBound().isBefore(to)) {
                return Collections.emptyList(); // the whole window is inside one partition (e.g. a MAXVALUE one)
            }
            anchor = current != null ? current.upperBound() : ranges.get(0).lowerBound();
            from = now;
        }

        final List<Candidate> candidates = lattice(c, anchor, from, to, policy.partitionDuration(), null,
                MAX_PARTITIONS_PER_RUN + ranges.size() + 2);
        final List<Partition> created = new ArrayList<>();
        outer:
        for (final Candidate cand : candidates) {
            // A candidate may be partly covered already (after a partition width change, or by a partition someone
            // created by hand); only the uncovered pieces are created, so the series stays contiguous.
            for (final Candidate part : uncovered(ranges, cand)) {
                if (!part.hi.isAfter(from)) {
                    // Entirely before the window: the lattice includes one range below 'from' (needed by the rescue
                    // step, which must cover the partition containing its earliest row), and after bootstrap we
                    // never create history.
                    continue;
                }
                if (created.size() >= MAX_PARTITIONS_PER_RUN) {
                    log.warn("Created {} partitions in one run, which is the per-run cap; the rest will be created on "
                            + "the next run. Check partitionDuration/partitionLeadTime if this is unexpected.", created.size());
                    break outer;
                }
                try {
                    created.add(apply ? createPartition(c, schema, part) : describe(schema, part));
                } catch (SQLException e) {
                    log.error("Could not create partition {} of {}", childName(part), parentTable, e);
                    errors.failed("create " + childName(part), e);
                }
            }
        }
        if (apply && !created.isEmpty()) {
            log.info("Created {} partition(s) of {}: {}", created.size(), parentTable, names(created));
        }
        return created;
    }

    /**
     * Mirrors pg_partman's calculate_time_partition_info(): the first partition starts at the configured
     * backfill start (or now minus the lead time), truncated to the calendar unit one step coarser than the
     * partition width.
     */
    private Instant bootstrapBase(final Connection c, final PartitionPolicy policy, final Instant now) throws SQLException {
        final String sql = "SELECT CASE WHEN d >= interval '1 year' THEN date_trunc('year', t) "
                + "WHEN d >= interval '1 month' THEN date_trunc('month', t) "
                + "WHEN d >= interval '1 day' THEN date_trunc('day', t) "
                + "WHEN d >= interval '1 minute' THEN date_trunc('hour', t) "
                + "ELSE date_trunc('minute', t) END "
                + "FROM (SELECT ?::interval AS d, COALESCE(?::timestamptz, ?::timestamptz - ?::interval) AS t) x";
        try (PreparedStatement ps = c.prepareStatement(sql)) {
            ps.setString(1, policy.partitionDuration());
            ps.setString(2, backfillStart);
            ps.setObject(3, now.atOffset(ZoneOffset.UTC));
            ps.setString(4, policy.partitionLeadTime());
            try (ResultSet rs = ps.executeQuery()) {
                rs.next();
                return rs.getObject(1, OffsetDateTime.class).toInstant();
            }
        }
    }

    // ------------------------------------------------------------------------
    // Step 2: rescue rows from the default partition
    // ------------------------------------------------------------------------

    /**
     * Rows whose timestamp matched no range partition were routed to the default partition. For every partition
     * width (aligned to the existing lattice) that holds such rows, this creates the partition, moves the rows
     * into it, and attaches it, all in one transaction. pg_partman refused to do this and would instead fail to
     * create overlapping partitions forever; here a backfill older than the first partition, or a collector with
     * a wrong clock, heals itself on the next run.
     *
     * @return the partitions created and populated, in ascending order
     */
    public List<Partition> rescueDefaultPartition(final Connection c, final PartitionPolicy policy, final Instant now)
            throws SQLException {
        return rescueDefaultPartition(c, policy, now, true);
    }

    /** As above; with {@code apply = false} nothing is moved and the partitions that would be created are returned. */
    public List<Partition> rescueDefaultPartition(final Connection c, final PartitionPolicy policy, final Instant now,
                                                  final boolean apply) throws SQLException {
        return rescueDefaultPartition(c, policy, now, apply, RETHROW);
    }

    /**
     * As above; a failure goes to {@code errors}. The step works from the data, oldest stray row first: it asks
     * for the earliest timestamp still in the default partition, computes the one partition cell holding it,
     * moves that cell's rows, then asks again. So the cost is proportional to the number of occupied cells (at
     * most {@link #MAX_RESCUES_PER_RUN} per run), never to how far away a bogus timestamp is.
     */
    public List<Partition> rescueDefaultPartition(final Connection c, final PartitionPolicy policy, final Instant now,
                                                  final boolean apply, final ErrorSink errors) throws SQLException {
        final List<Partition> all = listPartitions(c, false);
        final Partition def = all.stream().filter(Partition::isDefault).findFirst().orElse(null);
        if (def == null) {
            return Collections.emptyList();
        }
        final List<Partition> ranges = new ArrayList<>(rangePartitions(all));
        final String schema = parentSchema(c);
        final List<Partition> rescued = new ArrayList<>();
        Instant cursor = null;
        while (rescued.size() < MAX_RESCUES_PER_RUN) {
            final Instant t = earliestStrayRow(c, def, cursor);
            if (t == null) {
                break;
            }
            final Candidate part;
            try {
                // Anchor on the earliest finite lower bound (a hand-made MINVALUE partition has none).
                final Instant anchor = ranges.stream()
                        .map(Partition::lowerBound)
                        .filter(PartitionManager::isFinite)
                        .findFirst()
                        .orElseGet(() -> null);
                final Candidate cell = cellContaining(c, anchor != null ? anchor : bootstrapBase(c, policy, t),
                        policy.partitionDuration(), t);
                // After a width change the cell may be partly covered; rows PostgreSQL routed to the default
                // partition can only sit in an uncovered piece, so take the piece that holds this row.
                part = uncovered(ranges, cell).stream().filter(x -> x.contains(t)).findFirst().orElse(null);
                if (part == null) {
                    throw new SQLException("row at " + t + " in " + def.name() + " lies inside an existing partition");
                }
            } catch (SQLException e) {
                log.error("Could not determine the partition for stray rows at {} in {}", t, def.name(), e);
                errors.failed("rescue rows at " + t, e);
                break;
            }
            try {
                final Partition p = apply ? rescueRange(c, schema, def, part) : describe(schema, part);
                rescued.add(p);
                ranges.add(p);
                ranges.sort(Comparator.comparing(Partition::lowerBound));
            } catch (SQLException e) {
                log.error("Could not move rows from {} into new partition {}", def.name(), childName(part), e);
                errors.failed("rescue " + childName(part), e);
                break;
            }
            cursor = part.hi;
        }
        if (rescued.size() >= MAX_RESCUES_PER_RUN) {
            log.info("Rescued {} partitions' worth of rows from {}, the per-run cap; the rest follow on the next run.",
                    rescued.size(), def.name());
        }
        if (apply && !rescued.isEmpty()) {
            log.info("Moved stray rows out of {} into {} new partition(s): {}", def.name(), rescued.size(), names(rescued));
        }
        return rescued;
    }

    /** The earliest timestamp in the default partition at or after {@code cursor} (any, when cursor is null). */
    private static Instant earliestStrayRow(final Connection c, final Partition def, final Instant cursor) throws SQLException {
        try (PreparedStatement ps = c.prepareStatement("SELECT min(time) FROM ONLY " + def.qualifiedName()
                + " WHERE time >= COALESCE(?::timestamptz, '-infinity'::timestamptz)")) {
            if (cursor == null) {
                ps.setNull(1, Types.TIMESTAMP_WITH_TIMEZONE);
            } else {
                ps.setObject(1, cursor.atOffset(ZoneOffset.UTC));
            }
            try (ResultSet rs = ps.executeQuery()) {
                rs.next();
                return toInstant(rs.getObject(1, OffsetDateTime.class));
            }
        }
    }

    /**
     * The partition cell of width {@code duration}, on the lattice anchored at {@code anchor}, that contains
     * {@code t}. The step count is estimated (exactly, for month-based widths, via age(); by epoch otherwise,
     * where DST can drift it by an hour per year) and a small window of cells around the estimate is generated.
     */
    private Candidate cellContaining(final Connection c, final Instant anchor, final String duration, final Instant t)
            throws SQLException {
        final String sql = "WITH p AS (SELECT ?::timestamptz AS anchor, ?::interval AS dur, ?::timestamptz AS t), "
                + "k AS (SELECT CASE WHEN extract(year FROM p.dur) * 12 + extract(month FROM p.dur) > 0 "
                + "  THEN floor((extract(year FROM age(p.t, p.anchor)) * 12 + extract(month FROM age(p.t, p.anchor))) "
                + "            / (extract(year FROM p.dur) * 12 + extract(month FROM p.dur))) "
                + "  ELSE floor(extract(epoch FROM (p.t - p.anchor)) / extract(epoch FROM p.dur)) END AS k FROM p), "
                + "w AS (SELECT k.k::int AS k, (3 + ceil(abs(k.k) * 0.0003))::int AS w FROM k) "
                + "SELECT s, s + p.dur, p.dur < interval '1 day' "
                + "FROM p, w, generate_series(p.anchor + p.dur * (w.k - w.w), p.anchor + p.dur * (w.k + w.w), p.dur) AS s "
                + "WHERE s <= p.t AND p.t < s + p.dur ORDER BY s LIMIT 1";
        try (PreparedStatement ps = c.prepareStatement(sql)) {
            ps.setObject(1, anchor.atOffset(ZoneOffset.UTC));
            ps.setString(2, duration);
            ps.setObject(3, t.atOffset(ZoneOffset.UTC));
            try (ResultSet rs = ps.executeQuery()) {
                if (!rs.next()) {
                    throw new SQLException("no partition cell of width " + duration + " anchored at " + anchor + " contains " + t);
                }
                return new Candidate(rs.getObject(1, OffsetDateTime.class).toInstant(),
                        rs.getObject(2, OffsetDateTime.class).toInstant(), rs.getBoolean(3));
            }
        }
    }

    private Partition rescueRange(final Connection c, final String schema, final Partition def, final Candidate cand)
            throws SQLException {
        final String child = quoteIdent(schema) + "." + quoteIdent(childName(cand));
        assertNameFree(c, schema, childName(cand));
        return inTransaction(c, () -> {
            try (Statement st = c.createStatement()) {
                // Keep new rows out of the default partition for this range while we move the existing ones,
                // otherwise ATTACH would find them and fail.
                st.execute("LOCK TABLE " + def.qualifiedName() + " IN SHARE ROW EXCLUSIVE MODE");
                st.execute(createLikeParentSql(schema, childName(cand)));
            }
            final int moved;
            try (PreparedStatement ps = c.prepareStatement("INSERT INTO " + child + " SELECT * FROM ONLY "
                    + def.qualifiedName() + " WHERE time >= ? AND time < ?")) {
                ps.setObject(1, cand.lo.atOffset(ZoneOffset.UTC));
                ps.setObject(2, cand.hi.atOffset(ZoneOffset.UTC));
                moved = ps.executeUpdate();
            }
            try (PreparedStatement ps = c.prepareStatement("DELETE FROM ONLY " + def.qualifiedName()
                    + " WHERE time >= ? AND time < ?")) {
                ps.setObject(1, cand.lo.atOffset(ZoneOffset.UTC));
                ps.setObject(2, cand.hi.atOffset(ZoneOffset.UTC));
                ps.executeUpdate();
            }
            try (Statement st = c.createStatement()) {
                st.execute(attachSql(schema, childName(cand), cand));
            }
            log.debug("Moved {} row(s) from {} into new partition {}", moved, def.name(), childName(cand));
            return new Partition(schema, childName(cand), false, cand.lo, cand.hi, 0, 0, 0, null);
        });
    }

    // ------------------------------------------------------------------------
    // Step 3: retention
    // ------------------------------------------------------------------------

    /**
     * Detaches and drops every range partition whose upper bound is older than {@code now - retention}. The
     * last remaining range partition is never dropped. Unlike pg_partman's default behaviour the table is really
     * dropped, so disk is reclaimed.
     *
     * @return the partitions dropped, in ascending order
     */
    public List<Partition> applyRetention(final Connection c, final PartitionPolicy policy, final Instant now)
            throws SQLException {
        return applyRetention(c, policy, now, true);
    }

    /** As above; with {@code apply = false} nothing is dropped and the partitions that would be are returned. */
    public List<Partition> applyRetention(final Connection c, final PartitionPolicy policy, final Instant now,
                                          final boolean apply) throws SQLException {
        return applyRetention(c, policy, now, apply, RETHROW);
    }

    /** As above; a failure to drop one partition goes to {@code errors} and the others are still attempted. */
    public List<Partition> applyRetention(final Connection c, final PartitionPolicy policy, final Instant now,
                                          final boolean apply, final ErrorSink errors) throws SQLException {
        if (!policy.hasRetention()) {
            return Collections.emptyList();
        }
        final Instant cutoff = minusInterval(c, now, policy.retention());
        final List<Partition> ranges = rangePartitions(listPartitions(c, true)); // sizes: reported as reclaimed
        final List<Partition> dropped = new ArrayList<>();
        int remaining = ranges.size();
        for (final Partition p : ranges) {
            if (!p.upperBound().isBefore(cutoff)) {
                continue;
            }
            if (remaining <= 1) {
                log.warn("Retention would drop the last partition of {} ({}); keeping it. Is anything being written?",
                        parentTable, p.name());
                break;
            }
            if (apply) {
                try {
                    inTransaction(c, () -> {
                        try (Statement st = c.createStatement()) {
                            st.execute("ALTER TABLE " + quoteIdent(p.schema()) + "." + quoteIdent(parentTable)
                                    + " DETACH PARTITION " + p.qualifiedName());
                            st.execute("DROP TABLE " + p.qualifiedName());
                        }
                        return null;
                    });
                } catch (SQLException e) {
                    log.error("Could not drop expired partition {}", p.name(), e);
                    errors.failed("drop " + p.name(), e);
                    continue;
                }
            }
            dropped.add(p);
            remaining--;
        }
        if (apply && !dropped.isEmpty()) {
            final long bytes = dropped.stream().mapToLong(Partition::totalBytes).sum();
            log.info("Retention dropped {} partition(s) of {} ({} bytes): {}", dropped.size(), parentTable, bytes, names(dropped));
        }
        return dropped;
    }

    // ------------------------------------------------------------------------
    // Step 4: compression (optional, needs citus_columnar)
    // ------------------------------------------------------------------------

    /**
     * Converts every range partition whose upper bound is older than {@code now - compression} to the
     * {@code columnar} access method. This is the one remaining optional extension dependency: when
     * {@code citus_columnar} is not installed the step logs once and does nothing.
     *
     * @return the partitions converted, in ascending order
     */
    public List<Partition> applyCompression(final Connection c, final PartitionPolicy policy, final Instant now)
            throws SQLException {
        return applyCompression(c, policy, now, true);
    }

    /** As above; with {@code apply = false} nothing is converted and the partitions that would be are returned. */
    public List<Partition> applyCompression(final Connection c, final PartitionPolicy policy, final Instant now,
                                            final boolean apply) throws SQLException {
        return applyCompression(c, policy, now, apply, RETHROW);
    }

    /** As above; a failure to convert one partition goes to {@code errors} and the others are still attempted. */
    public List<Partition> applyCompression(final Connection c, final PartitionPolicy policy, final Instant now,
                                            final boolean apply, final ErrorSink errors) throws SQLException {
        if (!policy.hasCompression()) {
            return Collections.emptyList();
        }
        if (!extensionInstalled(c, "citus_columnar")) {
            if (!columnarMissingLogged) {
                log.info(NO_COLUMNAR_LOG_ONCE);
                columnarMissingLogged = true;
            }
            return Collections.emptyList();
        }
        columnarMissingLogged = false;
        final Instant cutoff = minusInterval(c, now, policy.compression());
        final List<Partition> compressed = new ArrayList<>();
        for (final Partition p : rangePartitions(listPartitions(c, false))) {
            if ("columnar".equals(p.accessMethod()) || !p.upperBound().isBefore(cutoff)) {
                continue;
            }
            if (apply) {
                try {
                    inTransaction(c, () -> {
                        try (Statement st = c.createStatement()) {
                            st.execute("ALTER TABLE " + p.qualifiedName() + " SET ACCESS METHOD columnar");
                        }
                        return null;
                    });
                } catch (SQLException e) {
                    log.error("Could not compress partition {}", p.name(), e);
                    errors.failed("compress " + p.name(), e);
                    continue;
                }
            }
            compressed.add(p);
        }
        if (apply && !compressed.isEmpty()) {
            log.info("Compressed {} partition(s) of {}: {}", compressed.size(), parentTable, names(compressed));
        }
        return compressed;
    }

    private static boolean extensionInstalled(final Connection c, final String name) throws SQLException {
        try (PreparedStatement ps = c.prepareStatement("SELECT EXISTS (SELECT 1 FROM pg_extension WHERE extname = ?)")) {
            ps.setString(1, name);
            try (ResultSet rs = ps.executeQuery()) {
                return rs.next() && rs.getBoolean(1);
            }
        }
    }

    // ------------------------------------------------------------------------
    // Lattice: candidate partition ranges aligned to an anchor
    // ------------------------------------------------------------------------

    /** A would-be partition: bounds plus whether the width is below one day (which selects the name format). */
    record Candidate(Instant lo, Instant hi, boolean subDay) {
        boolean contains(final Instant t) {
            return !t.isBefore(lo) && t.isBefore(hi);
        }
    }

    /**
     * The pieces of {@code cand} that no existing range partition covers, in ascending order. The whole candidate
     * when nothing overlaps it; nothing when it is fully covered.
     */
    static List<Candidate> uncovered(final List<Partition> ranges, final Candidate cand) {
        final List<Candidate> parts = new ArrayList<>();
        Instant cursor = cand.lo;
        for (final Partition p : ranges) { // sorted by lower bound
            if (!p.overlaps(cand.lo, cand.hi)) {
                continue;
            }
            if (p.lowerBound().isAfter(cursor)) {
                parts.add(new Candidate(cursor, p.lowerBound(), cand.subDay));
            }
            if (p.upperBound().isAfter(cursor)) {
                cursor = p.upperBound();
            }
        }
        if (cursor.isBefore(cand.hi)) {
            parts.add(new Candidate(cursor, cand.hi, cand.subDay));
        }
        return parts;
    }

    /**
     * Generates the partition ranges of width {@code duration} aligned so that one of them starts exactly at
     * {@code anchor}, covering every instant in [from, to]. The ranges and their upper bounds are computed by
     * PostgreSQL so interval arithmetic (calendar months, DST) behaves exactly as it did under pg_partman.
     *
     * @param extraWhere optional SQL fragment starting with WHERE that may reference {@code lat.lo} and {@code p.dur}
     */
    private List<Candidate> lattice(final Connection c, final Instant anchor, final Instant from, final Instant to,
                                    final String duration, final String extraWhere, final int limit)
            throws SQLException {
        final String sql = "WITH p AS (SELECT ?::timestamptz AS anchor, ?::timestamptz AS t_from, ?::timestamptz AS t_to, ?::interval AS dur), "
                + "lat AS (SELECT s AS lo FROM p, generate_series(p.anchor, p.t_to, p.dur) AS s "
                + "        UNION SELECT s FROM p, generate_series(p.anchor - p.dur, p.t_from - p.dur, -p.dur) AS s) "
                + "SELECT lat.lo, lat.lo + p.dur AS hi, p.dur < interval '1 day' AS sub_day "
                + "FROM lat, p " + (extraWhere == null ? "" : extraWhere) + " ORDER BY lat.lo LIMIT ?";
        final List<Candidate> result = new ArrayList<>();
        try (PreparedStatement ps = c.prepareStatement(sql)) {
            ps.setObject(1, anchor.atOffset(ZoneOffset.UTC));
            ps.setObject(2, from.atOffset(ZoneOffset.UTC));
            ps.setObject(3, to.atOffset(ZoneOffset.UTC));
            ps.setString(4, duration);
            ps.setInt(5, limit);
            try (ResultSet rs = ps.executeQuery()) {
                while (rs.next()) {
                    result.add(new Candidate(
                            rs.getObject(1, OffsetDateTime.class).toInstant(),
                            rs.getObject(2, OffsetDateTime.class).toInstant(),
                            rs.getBoolean(3)));
                }
            }
        }
        return result;
    }

    /** False for the -infinity / infinity bounds of MINVALUE / MAXVALUE partitions. */
    static boolean isFinite(final Instant t) {
        return Math.abs(t.getEpochSecond()) < 3_000_000_000_000L; // about 95,000 years either side of 1970
    }

    private static List<Partition> rangePartitions(final List<Partition> all) {
        return all.stream()
                .filter(Partition::isRange)
                .sorted(Comparator.comparing(Partition::lowerBound))
                .collect(Collectors.toList());
    }

    // ------------------------------------------------------------------------
    // Inspection helpers for the shell
    // ------------------------------------------------------------------------

    /** A table that is not a partition but carries a partition-style name: what pg_partman's keep-table retention leaves behind. */
    public record OrphanTable(String schema, String name, long totalBytes) {
        public String qualifiedName() {
            return quoteIdent(schema) + "." + quoteIdent(name);
        }
    }

    /** Standalone tables in the parent's schema named like {@code <parent>_p...} that are not attached partitions. */
    public List<OrphanTable> listOrphans(final Connection c) throws SQLException {
        final String schema = parentSchema(c);
        // Exactly the shape of a partition name (<parent>_pYYYYMMDD or <parent>_pYYYYMMDD_HHMISS), so that a
        // table such as <parent>_p20250101_backup is never mistaken for an orphan and dropped.
        final String sql = "SELECT c.relname, pg_total_relation_size(c.oid) FROM pg_class c "
                + "JOIN pg_namespace n ON n.oid = c.relnamespace "
                + "WHERE n.nspname = ? AND c.relkind = 'r' AND NOT c.relispartition AND c.relname ~ ? "
                + "ORDER BY c.relname";
        final List<OrphanTable> result = new ArrayList<>();
        try (PreparedStatement ps = c.prepareStatement(sql)) {
            ps.setString(1, schema);
            ps.setString(2, "^" + parentTable.replaceAll("[^A-Za-z0-9_]", "\\\\$0") + "_p[0-9]{8}(_[0-9]{6})?$");
            try (ResultSet rs = ps.executeQuery()) {
                while (rs.next()) {
                    result.add(new OrphanTable(schema, rs.getString(1), rs.getLong(2)));
                }
            }
        }
        return result;
    }

    /** Drops an orphan table. The caller has decided it is really unwanted. */
    public void dropOrphan(final Connection c, final OrphanTable orphan) throws SQLException {
        try (Statement st = c.createStatement()) {
            st.execute("DROP TABLE " + orphan.qualifiedName());
        }
    }

    /** Number of rows sitting in the default partition, i.e. rows no range partition accepted. */
    public long countDefaultPartitionRows(final Connection c) throws SQLException {
        for (final Partition p : listPartitions(c, false)) {
            if (p.isDefault()) {
                try (Statement st = c.createStatement();
                     ResultSet rs = st.executeQuery("SELECT count(*) FROM ONLY " + p.qualifiedName())) {
                    return rs.next() ? rs.getLong(1) : 0;
                }
            }
        }
        return 0;
    }

    private Partition describe(final String schema, final Candidate cand) {
        return new Partition(schema, childName(cand), false, cand.lo, cand.hi, 0, 0, 0, null);
    }

    // ------------------------------------------------------------------------
    // DDL helpers
    // ------------------------------------------------------------------------

    private String childName(final Candidate cand) {
        return parentTable + "_p" + (cand.subDay ? SUBDAY_SUFFIX : DAY_SUFFIX.withZone(ZoneId.systemDefault())).format(cand.lo);
    }

    /**
     * Creates and attaches an empty partition. Done as CREATE + ATTACH (as pg_partman did) rather than
     * CREATE TABLE ... PARTITION OF, because ATTACH only takes SHARE UPDATE EXCLUSIVE on the parent and so does
     * not block concurrent inserts.
     */
    private Partition createPartition(final Connection c, final String schema, final Candidate cand) throws SQLException {
        final String name = childName(cand);
        assertNameFree(c, schema, name);
        return inTransaction(c, () -> {
            try (Statement st = c.createStatement()) {
                st.execute(createLikeParentSql(schema, name));
                st.execute(attachSql(schema, name, cand));
            }
            return new Partition(schema, name, false, cand.lo, cand.hi, 0, 0, 0, null);
        });
    }

    private String createLikeParentSql(final String schema, final String name) {
        return "CREATE TABLE " + quoteIdent(schema) + "." + quoteIdent(name)
                + " (LIKE " + quoteIdent(schema) + "." + quoteIdent(parentTable)
                + " INCLUDING DEFAULTS INCLUDING CONSTRAINTS INCLUDING INDEXES)";
    }

    private String attachSql(final String schema, final String name, final Candidate cand) {
        return "ALTER TABLE " + quoteIdent(schema) + "." + quoteIdent(parentTable)
                + " ATTACH PARTITION " + quoteIdent(schema) + "." + quoteIdent(name)
                + " FOR VALUES FROM ('" + TS_LITERAL.format(cand.lo) + "') TO ('" + TS_LITERAL.format(cand.hi) + "')";
    }

    /**
     * A relation with the intended child name that is not one of our partitions (for example a partition
     * pg_partman detached under its keep-table retention mode) must not be silently reused or dropped.
     */
    private void assertNameFree(final Connection c, final String schema, final String name) throws SQLException {
        try (PreparedStatement ps = c.prepareStatement("SELECT to_regclass(?) IS NOT NULL")) {
            ps.setString(1, quoteIdent(schema) + "." + quoteIdent(name));
            try (ResultSet rs = ps.executeQuery()) {
                if (rs.next() && rs.getBoolean(1)) {
                    throw new SQLException("A relation named " + schema + "." + name + " already exists but is not an "
                            + "attached partition of " + parentTable + " (an orphan left by pg_partman?). Drop or rename it.");
                }
            }
        }
    }

    @FunctionalInterface
    private interface TxWork<T> {
        T run() throws SQLException;
    }

    private static <T> T inTransaction(final Connection c, final TxWork<T> work) throws SQLException {
        c.setAutoCommit(false);
        try {
            final T result = work.run();
            c.commit();
            return result;
        } catch (SQLException | RuntimeException e) {
            try {
                c.rollback();
            } catch (SQLException re) {
                log.warn("Rollback failed", re);
            }
            throw e;
        } finally {
            c.setAutoCommit(true);
        }
    }

    private Instant plusInterval(final Connection c, final Instant t, final String interval) throws SQLException {
        return shift(c, t, interval, "+");
    }

    private Instant minusInterval(final Connection c, final Instant t, final String interval) throws SQLException {
        return shift(c, t, interval, "-");
    }

    private static Instant shift(final Connection c, final Instant t, final String interval, final String op) throws SQLException {
        try (PreparedStatement ps = c.prepareStatement("SELECT ?::timestamptz " + op + " ?::interval")) {
            ps.setObject(1, t.atOffset(ZoneOffset.UTC));
            ps.setString(2, interval);
            try (ResultSet rs = ps.executeQuery()) {
                rs.next();
                return rs.getObject(1, OffsetDateTime.class).toInstant();
            }
        }
    }

    private boolean tryLock(final Connection c) throws SQLException {
        try (PreparedStatement ps = c.prepareStatement("SELECT pg_try_advisory_lock(?)")) {
            ps.setLong(1, ADVISORY_LOCK_KEY);
            try (ResultSet rs = ps.executeQuery()) {
                return rs.next() && rs.getBoolean(1);
            }
        }
    }

    private void unlock(final Connection c) {
        try (PreparedStatement ps = c.prepareStatement("SELECT pg_advisory_unlock(?)")) {
            ps.setLong(1, ADVISORY_LOCK_KEY);
            ps.execute();
        } catch (SQLException e) {
            log.debug("Could not release the maintenance advisory lock (the session is closing anyway)", e);
        }
    }

    /** Double-quotes an SQL identifier. */
    public static String quoteIdent(final String ident) {
        return "\"" + ident.replace("\"", "\"\"") + "\"";
    }
}
