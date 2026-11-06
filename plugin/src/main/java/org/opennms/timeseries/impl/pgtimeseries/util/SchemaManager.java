package org.opennms.timeseries.impl.pgtimeseries.util;

import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.time.Clock;

import javax.sql.DataSource;

import org.opennms.timeseries.impl.pgtimeseries.config.PGTimeseriesConfig;
import org.opennms.timeseries.impl.pgtimeseries.partition.MaintenanceReport;
import org.opennms.timeseries.impl.pgtimeseries.partition.PartitionManager;

import lombok.extern.slf4j.Slf4j;

/**
 * Creates and upgrades the plugin's database schema with plain SQL. No PostgreSQL extension and no superuser
 * is needed: everything is created as the connecting role, which therefore owns it.
 *
 * <p>The schema is the same one the 0.2.x plugin had pg_timeseries set up (three tables, the same indexes, the
 * same default partition and partition naming), plus one small policy table that replaces the extension's
 * {@code ts_config} and a lookup index on the tag table that the old versions lacked. Every statement is idempotent so this can run at every start-up and on top of a database
 * that was set up by an earlier version.</p>
 *
 * <p>Datasource selection (local vs. external) lives in {@link PGTimeseriesDatabaseHelpers}.</p>
 */
@Slf4j
public class SchemaManager {

    /** Index on {@code pgtimeseries_tag (key, value)}; added in 0.3.0 for {@code findMetrics}. */
    public static final String TAG_LOOKUP_INDEX = "pgtimeseries_tag_key_value_idx";

    public SchemaManager(final DataSource dataSource, final PGTimeseriesConfig config) {
        PGTimeseriesDatabaseHelpers.configure(dataSource, config);
    }

    // ------------------------------------------------------------------------
    // Inspection
    // ------------------------------------------------------------------------

    public static boolean doesTableExist(final String tableName) throws SQLException {
        final DBUtils db = new DBUtils();
        try {
            final Connection conn = PGTimeseriesDatabaseHelpers.getWhichDataSourceConnection();
            db.watch(conn);
            final DatabaseMetaData metaData = conn.getMetaData();
            final ResultSet tables = metaData.getTables(null, null, tableName, null);
            db.watch(tables);
            return tables.next();
        } finally {
            db.cleanUp();
        }
    }

    /** True when the three data tables and the policy table all exist. */
    public static boolean tablesExist() throws SQLException {
        return doesTableExist(TableNames.PGTIMESERIES_TIME_SERIES)
                && doesTableExist(TableNames.PGTIMESERIES_METRIC)
                && doesTableExist(TableNames.PGTIMESERIES_TAG)
                && doesTableExist(TableNames.PGTIMESERIES_CONFIG);
    }

    /** True when the pg_timeseries extension (from the 0.2.x plugin) is installed in the database. */
    public static boolean isLegacyExtensionPresent() throws SQLException {
        try (Connection conn = PGTimeseriesDatabaseHelpers.getWhichDataSourceConnection();
             Statement st = conn.createStatement();
             ResultSet rs = st.executeQuery("SELECT EXISTS (SELECT 1 FROM pg_extension WHERE extname = 'timeseries')")) {
            return rs.next() && rs.getBoolean(1);
        }
    }

    /**
     * True when pg_partman still has a configuration row for our table, which means its hourly pg_cron job (if
     * it runs at all) is also creating and detaching partitions.
     */
    public static boolean isLegacyPartmanManaged() throws SQLException {
        try (Connection conn = PGTimeseriesDatabaseHelpers.getWhichDataSourceConnection()) {
            return isLegacyPartmanManaged(conn);
        }
    }

    static boolean isLegacyPartmanManaged(final Connection conn) throws SQLException {
        final String schema = legacyPartmanSchema(conn);
        if (schema == null) {
            return false;
        }
        try (PreparedStatement ps = conn.prepareStatement("SELECT EXISTS (SELECT 1 FROM "
                + PartitionManager.quoteIdent(schema) + ".part_config WHERE parent_table LIKE ?)")) {
            ps.setString(1, "%." + TableNames.PGTIMESERIES_TIME_SERIES);
            try (ResultSet rs = ps.executeQuery()) {
                return rs.next() && rs.getBoolean(1);
            }
        } catch (SQLException e) {
            log.debug("Could not read {}.part_config; assuming the table is not partman-managed", schema, e);
            return false;
        }
    }

    /**
     * The schema pg_partman is installed in, or null when it is not installed. {@code CREATE EXTENSION timeseries
     * CASCADE} puts it in the same schema as pg_timeseries (normally {@code public}), not in {@code partman}.
     */
    public static String legacyPartmanSchema(final Connection conn) throws SQLException {
        try (Statement st = conn.createStatement();
             ResultSet rs = st.executeQuery("SELECT n.nspname FROM pg_extension e JOIN pg_namespace n ON n.oid = e.extnamespace "
                     + "WHERE e.extname = 'pg_partman'")) {
            return rs.next() ? rs.getString(1) : null;
        }
    }

    /** Like {@link #isLegacyPartmanManaged()} but never throws; for use from the maintenance thread. */
    public static boolean isLegacyPartmanManagedQuietly() {
        try {
            return isLegacyPartmanManaged();
        } catch (SQLException e) {
            log.debug("Could not check for legacy pg_partman management", e);
            return false;
        }
    }

    /** A partition manager bound to the configured datasource and the system clock, for shell commands and migration. */
    public static PartitionManager newPartitionManager() {
        final PGTimeseriesConfig config = PGTimeseriesDatabaseHelpers.getConfig();
        return new PartitionManager(PGTimeseriesDatabaseHelpers::getWhichDataSourceConnection,
                TableNames.PGTIMESERIES_TIME_SERIES, TableNames.PGTIMESERIES_CONFIG,
                config == null ? null : config.getbackfillStart(), Clock.systemUTC());
    }

    // ------------------------------------------------------------------------
    // Creation
    // ------------------------------------------------------------------------

    /** Creates (or completes) the schema using the system clock for the initial partition layout. */
    public static MaintenanceReport createSchema() throws SQLException {
        return createSchema(Clock.systemUTC());
    }

    /**
     * Creates every missing schema object, in dependency order, then runs one partition maintenance pass (under
     * the maintenance advisory lock) so the initial partitions exist. Safe to call repeatedly: DDL is only issued
     * for objects that are missing, so a role that merely owns an already complete schema (or has no CREATE
     * privilege on it) never runs any.
     *
     * @param clock the clock the initial partitions are laid out around (injectable for tests)
     * @return the maintenance report for the bootstrap pass, or null when {@code managePartitions} is false
     */
    public static MaintenanceReport createSchema(final Clock clock) throws SQLException {
        ensureObjects();
        return bootstrapPartitions(clock);
    }

    /** Creates whatever schema objects are missing (tables, sequence, indexes, policy row, default partition). */
    public static void ensureObjects() throws SQLException {
        final PGTimeseriesConfig config = PGTimeseriesDatabaseHelpers.getConfig();
        try (Connection conn = PGTimeseriesDatabaseHelpers.getWhichDataSourceConnection()) {
            conn.setAutoCommit(true);
            try (Statement stmt = conn.createStatement()) {
                // The partitioned time series table itself.
                createIfMissing(conn, stmt, TableNames.PGTIMESERIES_TIME_SERIES, "CREATE TABLE " + TableNames.PGTIMESERIES_TIME_SERIES
                        + "(keyid bigint, time TIMESTAMPTZ NOT NULL, value DOUBLE PRECISION NULL) PARTITION BY RANGE (time)");
                // Metric id sequence and the metric table. The PRIMARY KEY on "key" already provides a unique index.
                createIfMissing(conn, stmt, "pgtimeseries_metric_seq", "CREATE SEQUENCE pgtimeseries_metric_seq no cycle");
                createIfMissing(conn, stmt, TableNames.PGTIMESERIES_METRIC, "CREATE TABLE " + TableNames.PGTIMESERIES_METRIC
                        + "(keyid bigint DEFAULT nextval('pgtimeseries_metric_seq'), key TEXT NOT NULL PRIMARY KEY)");
                // Tag table. Lookups by keyid use the leading column of the UNIQUE constraint's index.
                createIfMissing(conn, stmt, TableNames.PGTIMESERIES_TAG, "CREATE TABLE " + TableNames.PGTIMESERIES_TAG
                        + "(keyid bigint, key TEXT, value TEXT NOT NULL, type TEXT NOT NULL, UNIQUE (keyid, key, value, type))");
                // findMetrics looks tags up by key and value. The UNIQUE index above leads with keyid, so without this
                // one every resource lookup scanned the whole tag table. Missing on 0.2.x databases, hence added on
                // start-up; a role that may not create it gets a warning and the statement to run, not a failed start.
                createIfMissingOrWarn(conn, stmt, TAG_LOOKUP_INDEX,
                        "CREATE INDEX " + TAG_LOOKUP_INDEX + " ON " + TableNames.PGTIMESERIES_TAG + "(key, value)");
                // Indexes on the parent propagate to every existing and future partition.
                // The fetch path always filters "WHERE keyid = ? AND time > ? AND time < ?".
                createIfMissing(conn, stmt, "pgtimeseries_time_series_keyid_time_idx",
                        "CREATE INDEX pgtimeseries_time_series_keyid_time_idx ON " + TableNames.PGTIMESERIES_TIME_SERIES + "(keyid, time DESC)");
                // A standalone time index supports maintenance that scans by time range (default partition rescue).
                createIfMissing(conn, stmt, "pgtimeseries_time_series_time_idx",
                        "CREATE INDEX pgtimeseries_time_series_time_idx ON " + TableNames.PGTIMESERIES_TIME_SERIES + "(time DESC)");
                // Partition policy; replaces the extension's ts_config table, same columns.
                createIfMissing(conn, stmt, TableNames.PGTIMESERIES_CONFIG, "CREATE TABLE " + TableNames.PGTIMESERIES_CONFIG + "("
                        + "table_name text PRIMARY KEY, "
                        + "partition_duration interval NOT NULL, "
                        + "partition_lead_time interval NOT NULL, "
                        + "retention_duration interval, "
                        + "compression_duration interval, "
                        + "updated_at timestamptz NOT NULL DEFAULT now())");
            }

            seedPolicy(conn, config);

            try (Statement stmt = conn.createStatement()) {
                // Out-of-range timestamps land here instead of failing a whole flush batch; maintenance moves them
                // into proper partitions later.
                createIfMissing(conn, stmt, TableNames.PGTIMESERIES_DEFAULT_PARTITION, "CREATE TABLE "
                        + TableNames.PGTIMESERIES_DEFAULT_PARTITION + " PARTITION OF " + TableNames.PGTIMESERIES_TIME_SERIES + " DEFAULT");
            }
        }
    }

    /**
     * One maintenance pass with the given clock: bootstraps the initial partitions on a fresh schema, and on an
     * existing one just makes sure the lead window is covered. Runs under the maintenance advisory lock, so it
     * cannot race the maintenance thread of this or another OpenNMS instance.
     */
    public static MaintenanceReport bootstrapPartitions(final Clock clock) {
        final PGTimeseriesConfig config = PGTimeseriesDatabaseHelpers.getConfig();
        if (!config.getManagePartitions()) {
            log.info("managePartitions is false; not creating any partitions of {}", TableNames.PGTIMESERIES_TIME_SERIES);
            return null;
        }
        final PartitionManager pm = new PartitionManager(PGTimeseriesDatabaseHelpers::getWhichDataSourceConnection,
                TableNames.PGTIMESERIES_TIME_SERIES, TableNames.PGTIMESERIES_CONFIG, config.getbackfillStart(), clock);
        final MaintenanceReport report = pm.runMaintenance();
        if (report.hasErrors()) {
            log.warn("Partition bootstrap finished with errors: {}", report.summary());
        } else {
            log.info("Partition bootstrap: {}", report.summary());
        }
        return report;
    }

    /** Like {@link #createIfMissing} for an object the plugin works without: a failure is logged, not thrown. */
    private static void createIfMissingOrWarn(final Connection conn, final Statement stmt, final String relation, final String ddl)
            throws SQLException {
        try {
            createIfMissing(conn, stmt, relation, ddl);
        } catch (SQLException e) {
            log.warn("Could not create {} ({}). The plugin works without it but metric lookups are slower; "
                    + "run this as the owner of the table: {}", relation, e.getMessage().trim(), ddl);
        }
    }

    /** Runs {@code ddl} only when no relation named {@code relation} is visible; avoids DDL (and its privilege checks) otherwise. */
    private static void createIfMissing(final Connection conn, final Statement stmt, final String relation, final String ddl)
            throws SQLException {
        try (PreparedStatement ps = conn.prepareStatement("SELECT to_regclass(?) IS NULL")) {
            ps.setString(1, relation);
            try (ResultSet rs = ps.executeQuery()) {
                if (!rs.next() || !rs.getBoolean(1)) {
                    return;
                }
            }
        }
        execute(stmt, ddl);
    }

    /**
     * Inserts the policy row if it is missing. A row left behind by the pg_timeseries extension (ts_config) is
     * imported in preference to the configuration file, so runtime changes an operator made with ts-config
     * survive the upgrade.
     */
    private static void seedPolicy(final Connection conn, final PGTimeseriesConfig config) throws SQLException {
        if (hasLegacyTsConfig(conn)) {
            final String importSql = "INSERT INTO " + TableNames.PGTIMESERIES_CONFIG
                    + " (table_name, partition_duration, partition_lead_time, retention_duration, compression_duration) "
                    + "SELECT ?, partition_duration, partition_lead_time, retention_duration, compression_duration "
                    + "FROM ts_config WHERE table_id = ?::regclass "
                    + "ON CONFLICT (table_name) DO NOTHING";
            try (PreparedStatement ps = conn.prepareStatement(importSql)) {
                ps.setString(1, TableNames.PGTIMESERIES_TIME_SERIES);
                ps.setString(2, TableNames.PGTIMESERIES_TIME_SERIES);
                if (ps.executeUpdate() > 0) {
                    log.info("Imported the partition policy from the pg_timeseries ts_config table");
                }
            } catch (SQLException e) {
                log.warn("Could not import the policy from the legacy ts_config table; using the configured values", e);
            }
        }
        final String seedSql = "INSERT INTO " + TableNames.PGTIMESERIES_CONFIG
                + " (table_name, partition_duration, partition_lead_time, retention_duration, compression_duration) "
                + "VALUES (?, ?::interval, ?::interval, ?::interval, ?::interval) ON CONFLICT (table_name) DO NOTHING";
        try (PreparedStatement ps = conn.prepareStatement(seedSql)) {
            ps.setString(1, TableNames.PGTIMESERIES_TIME_SERIES);
            ps.setString(2, config.getpartitionDuration());
            ps.setString(3, config.getPartitionLeadTime());
            ps.setString(4, blankToNull(config.getretentionPolicy()));
            ps.setString(5, blankToNull(config.getcompressionPolicy()));
            if (ps.executeUpdate() > 0) {
                log.info("Seeded the partition policy from configuration: duration={}, leadTime={}, retention={}, compression={}",
                        config.getpartitionDuration(), config.getPartitionLeadTime(),
                        config.getretentionPolicy(), config.getcompressionPolicy());
            }
        }
    }

    private static boolean hasLegacyTsConfig(final Connection conn) throws SQLException {
        try (Statement st = conn.createStatement();
             ResultSet rs = st.executeQuery("SELECT to_regclass('ts_config') IS NOT NULL")) {
            return rs.next() && rs.getBoolean(1);
        }
    }

    private static String blankToNull(final String s) {
        return (s == null || s.isBlank()) ? null : s;
    }

    private static void execute(final Statement stmt, final String sql) throws SQLException {
        log.debug(sql);
        stmt.execute(sql);
    }

    /** Called from the storage's init: creates or completes the schema unless {@code createTablesOnInstall} is off. */
    public void initializeIfNeeded() throws SQLException {
        final PGTimeseriesConfig config = PGTimeseriesDatabaseHelpers.getConfig();
        log.debug("Starting up pgtimeseries plugin with config: {}", config);
        if (!config.getCreateTablesOnInstall()) {
            log.info("Skipping schema creation as createTablesOnInstall is false");
            return;
        }
        if (tablesExist()) {
            log.info("pgtimeseries tables exist; verifying the schema is complete.");
        } else {
            log.info("pgtimeseries tables are missing or incomplete; creating them now.");
        }
        ensureObjects();
        try {
            // Not fatal: the maintenance thread retries every interval, and the default partition accepts writes.
            bootstrapPartitions(Clock.systemUTC());
        } catch (RuntimeException e) {
            log.warn("Partition bootstrap failed; writes go to the default partition until maintenance succeeds", e);
        }
        if (isLegacyExtensionPresent()) {
            log.warn("The pg_timeseries extension is installed in this database. It is no longer required; "
                    + "run opennms-pgtimeseries:migrate-from-extension to hand partition management over to the plugin.");
        }
    }
}
