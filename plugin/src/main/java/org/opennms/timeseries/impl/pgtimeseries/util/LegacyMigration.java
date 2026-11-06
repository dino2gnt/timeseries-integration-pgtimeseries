package org.opennms.timeseries.impl.pgtimeseries.util;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import org.opennms.timeseries.impl.pgtimeseries.partition.PartitionManager;
import org.opennms.timeseries.impl.pgtimeseries.partition.PartitionManager.OrphanTable;

import lombok.extern.slf4j.Slf4j;

/**
 * One-off hand-over from the pg_timeseries extension (0.2.x plugin) to the plugin's own partition management.
 *
 * <p>Steps, each independent and each reported: complete the schema (which imports the policy from
 * {@code ts_config}); unschedule the two pg_cron jobs the extension installed; remove the table's pg_partman
 * configuration and template table; drop the {@code timeseries} extension; list (and optionally drop) orphan
 * tables that pg_partman's keep-table retention left behind. pg_cron and pg_partman themselves are left
 * installed; dropping them is a DBA decision.</p>
 *
 * <p>Clean-up statements run over the {@code adminDatasourceURL} connection when one is configured (the
 * extension is normally owned by a superuser), otherwise over the regular connection. Anything that fails is
 * reported together with the exact SQL so a DBA can run it by hand.</p>
 */
@Slf4j
public final class LegacyMigration {

    /** The pg_cron jobs pg_timeseries 0.1.x/0.2.x registers at CREATE EXTENSION time. */
    static final List<String> CRON_JOBS = List.of("partman-maintenance", "timeseries-compression");

    public record Options(boolean dryRun, boolean dropOrphans) {
    }

    /** What happened, as lines for the operator, plus any SQL that must be run by hand. */
    public static final class Result {
        private final List<String> lines = new ArrayList<>();
        private final List<String> manualSql = new ArrayList<>();
        private final List<OrphanTable> orphansRemaining = new ArrayList<>();

        public List<String> getLines() {
            return Collections.unmodifiableList(lines);
        }

        public List<String> getManualSql() {
            return Collections.unmodifiableList(manualSql);
        }

        public List<OrphanTable> getOrphansRemaining() {
            return Collections.unmodifiableList(orphansRemaining);
        }

        public boolean isComplete() {
            return manualSql.isEmpty();
        }

        void line(final String s) {
            lines.add(s);
        }
    }

    private LegacyMigration() {
    }

    public static Result run(final Options options) throws SQLException {
        final Result r = new Result();
        final PartitionManager pm = SchemaManager.newPartitionManager();

        final boolean extension = SchemaManager.isLegacyExtensionPresent();
        final boolean partman = SchemaManager.isLegacyPartmanManaged();
        final List<OrphanTable> orphans;
        try (Connection c = PGTimeseriesDatabaseHelpers.getWhichDataSourceConnection()) {
            orphans = pm.listOrphans(c);
        }
        if (!extension && !partman && orphans.isEmpty()) {
            r.line("Nothing to migrate: the pg_timeseries extension is not installed, pg_partman does not manage "
                    + TableNames.PGTIMESERIES_TIME_SERIES + ", and there are no orphan partition tables.");
            return r;
        }

        // 1. Schema (imports ts_config into pgtimeseries_config while ts_config still exists).
        if (options.dryRun()) {
            r.line("Would verify the plugin schema and import the partition policy from ts_config.");
        } else {
            SchemaManager.createSchema();
            r.line("Verified the plugin schema; the partition policy is in " + TableNames.PGTIMESERIES_CONFIG + ".");
        }

        final boolean admin = PGTimeseriesDatabaseHelpers.isAdminDatasourceURLAvailable();
        try (Connection c = admin ? PGTimeseriesDatabaseHelpers.getAdminConnection()
                                  : PGTimeseriesDatabaseHelpers.getWhichDataSourceConnection()) {
            c.setAutoCommit(true);
            r.line("Using the " + (admin ? "adminDatasourceURL" : "regular") + " connection for the extension clean-up.");

            // 2. pg_cron jobs.
            if (relationExists(c, "cron.job")) {
                for (final String job : CRON_JOBS) {
                    if (cronJobExists(c, job)) {
                        execute(c, r, options, "unschedule pg_cron job '" + job + "'",
                                "SELECT cron.unschedule(" + literal(job) + ")");
                    } else {
                        r.line("pg_cron job '" + job + "' is not scheduled; nothing to do.");
                    }
                }
            } else {
                r.line("pg_cron is not installed in this database; no jobs to unschedule.");
            }

            // 3. pg_partman configuration and template table.
            final String partmanSchema = SchemaManager.legacyPartmanSchema(c);
            if (partmanSchema != null) {
                final String template = partmanTemplateTable(c, partmanSchema);
                if (template != null || partman) {
                    execute(c, r, options, "remove the pg_partman configuration for " + TableNames.PGTIMESERIES_TIME_SERIES,
                            "DELETE FROM " + PartitionManager.quoteIdent(partmanSchema) + ".part_config WHERE parent_table LIKE "
                                    + literal("%." + TableNames.PGTIMESERIES_TIME_SERIES));
                }
                if (template != null) {
                    execute(c, r, options, "drop the pg_partman template table " + template,
                            "DROP TABLE IF EXISTS " + qualify(template));
                }
            }

            // 4. The extension itself (takes ts_config, the ts_* views and functions with it).
            if (extension) {
                execute(c, r, options, "drop the pg_timeseries extension", "DROP EXTENSION timeseries");
            } else {
                r.line("The pg_timeseries extension is not installed; nothing to drop.");
            }
        }

        // 5. Orphans left by pg_partman's keep-table retention.
        if (orphans.isEmpty()) {
            r.line("No orphan partition tables found.");
        } else if (options.dropOrphans() && !options.dryRun()) {
            try (Connection c = PGTimeseriesDatabaseHelpers.getWhichDataSourceConnection()) {
                for (final OrphanTable o : orphans) {
                    try {
                        pm.dropOrphan(c, o);
                        r.line("Dropped orphan table " + o.name() + " (" + o.totalBytes() + " bytes).");
                    } catch (SQLException e) {
                        r.line("FAILED to drop orphan table " + o.name() + ": " + e.getMessage());
                        r.manualSql.add("DROP TABLE " + o.qualifiedName() + ";");
                        r.orphansRemaining.add(o);
                    }
                }
            }
        } else {
            long bytes = 0;
            for (final OrphanTable o : orphans) {
                bytes += o.totalBytes();
                r.orphansRemaining.add(o);
            }
            r.line((options.dryRun() && options.dropOrphans() ? "Would drop " : "Found ") + orphans.size()
                    + " orphan partition table(s) holding " + bytes + " bytes, detached by pg_partman's retention "
                    + "but never dropped" + (options.dropOrphans() ? "." : "; re-run with --drop-orphans to remove them."));
            for (final OrphanTable o : orphans) {
                r.line("  " + o.name() + " (" + o.totalBytes() + " bytes)");
            }
        }

        if (!r.manualSql.isEmpty()) {
            r.line("Some steps failed, most likely for lack of privileges. Run the following as a superuser:");
            for (final String sql : r.manualSql) {
                r.line("  " + sql);
            }
        }
        return r;
    }

    private static void execute(final Connection c, final Result r, final Options options, final String what, final String sql) {
        if (options.dryRun()) {
            r.line("Would " + what + ": " + sql);
            return;
        }
        try (Statement st = c.createStatement()) {
            st.execute(sql);
            r.line("Done: " + what + ".");
        } catch (SQLException e) {
            log.warn("Legacy migration step failed: {} ({})", what, sql, e);
            r.line("FAILED to " + what + ": " + e.getMessage().trim());
            r.manualSql.add(sql + ";");
        }
    }

    private static boolean relationExists(final Connection c, final String qualified) throws SQLException {
        try (PreparedStatement ps = c.prepareStatement("SELECT to_regclass(?) IS NOT NULL")) {
            ps.setString(1, qualified);
            try (ResultSet rs = ps.executeQuery()) {
                return rs.next() && rs.getBoolean(1);
            }
        }
    }

    private static boolean cronJobExists(final Connection c, final String job) throws SQLException {
        try (PreparedStatement ps = c.prepareStatement("SELECT EXISTS (SELECT 1 FROM cron.job WHERE jobname = ?)")) {
            ps.setString(1, job);
            try (ResultSet rs = ps.executeQuery()) {
                return rs.next() && rs.getBoolean(1);
            }
        }
    }

    /** The template table pg_partman recorded for our parent, as "schema.table", or null if not configured. */
    private static String partmanTemplateTable(final Connection c, final String partmanSchema) {
        final String sql = "SELECT template_table FROM " + PartitionManager.quoteIdent(partmanSchema)
                + ".part_config WHERE parent_table LIKE ?";
        try (PreparedStatement ps = c.prepareStatement(sql)) {
            ps.setString(1, "%." + TableNames.PGTIMESERIES_TIME_SERIES);
            try (ResultSet rs = ps.executeQuery()) {
                return rs.next() ? rs.getString(1) : null;
            }
        } catch (SQLException e) {
            log.debug("Could not read the pg_partman template table name", e);
            return null;
        }
    }

    private static String qualify(final String schemaDotTable) {
        final int dot = schemaDotTable.indexOf('.');
        if (dot < 0) {
            return PartitionManager.quoteIdent(schemaDotTable);
        }
        return PartitionManager.quoteIdent(schemaDotTable.substring(0, dot)) + "."
                + PartitionManager.quoteIdent(schemaDotTable.substring(dot + 1));
    }

    private static String literal(final String s) {
        return "'" + s.replace("'", "''") + "'";
    }
}
