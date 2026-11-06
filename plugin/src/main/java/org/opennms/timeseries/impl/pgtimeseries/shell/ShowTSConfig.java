package org.opennms.timeseries.impl.pgtimeseries.shell;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;

import javax.sql.DataSource;

import org.apache.karaf.shell.api.action.Action;
import org.apache.karaf.shell.api.action.Command;
import org.apache.karaf.shell.api.action.Option;
import org.apache.karaf.shell.api.action.lifecycle.Reference;
import org.apache.karaf.shell.api.action.lifecycle.Service;
import org.apache.karaf.shell.support.table.ShellTable;
import org.opennms.timeseries.impl.pgtimeseries.PGTimeseriesStorage;
import org.opennms.timeseries.impl.pgtimeseries.partition.MaintenanceReport;
import org.opennms.timeseries.impl.pgtimeseries.partition.MaintenanceService;
import org.opennms.timeseries.impl.pgtimeseries.util.PGTimeseriesDatabaseHelpers;
import org.opennms.timeseries.impl.pgtimeseries.util.TableNames;

@Command(scope = "opennms-pgtimeseries", name = "ts-config", description = "Display or change the partition policy of the time series table (stored in pgtimeseries_config).")
@Service
public class ShowTSConfig implements Action {

    /** Value that clears an optional policy. */
    private static final String NONE = "none";

    @Option(name = "-r", aliases = "--retention", description = "Retention interval. Partitions older than this are dropped. A PostgreSQL interval such as '9 months' or '1 year', or 'none' to keep data forever.", required = false, multiValued = false)
    String retentionInterval;

    @Option(name = "-c", aliases = "--compression", description = "Compress partitions older than this (needs the citus_columnar extension). A PostgreSQL interval, or 'none' to disable.", required = false, multiValued = false)
    String compressionInterval;

    @Option(name = "-p", aliases = "--partition", description = "Width of each partition, e.g. '1 week' or '1 day'. Affects partitions created from now on; existing partitions keep their bounds.", required = false, multiValued = false)
    String partitionInterval;

    @Option(name = "-l", aliases = "--lead-time", description = "How far ahead of now partitions are kept pre-created, e.g. '1 month'.", required = false, multiValued = false)
    String leadTimeInterval;

    @Reference
    private DataSource dataSource;

    /** Optional so the command stays available when the storage failed to initialise, which is when it is needed. */
    @Reference(optional = true)
    private PGTimeseriesStorage storage;

    @Override
    public Object execute() throws SQLException {
        try (Connection conn = PGTimeseriesDatabaseHelpers.getWhichDataSourceConnection()) {
            // Width first, so that a retention given in the same call is checked against the new width.
            update(conn, "partition_duration", partitionInterval, false, "partition duration", "1 minute");
            update(conn, "partition_lead_time", leadTimeInterval, false, "partition lead time", "1 minute");
            final String width = currentWidth(conn);
            update(conn, "retention_duration", retentionInterval, true, "retention policy", width);
            update(conn, "compression_duration", compressionInterval, true, "compression policy", width);

            final ShellTable table = new ShellTable();
            table.column("Name");
            table.column("Partition Duration");
            table.column("Partition Lead Time");
            table.column("Retention");
            table.column("Compression After");
            table.column("Updated");
            final String sql = "SELECT table_name, partition_duration::text, partition_lead_time::text, "
                    + "retention_duration::text, compression_duration::text, updated_at FROM "
                    + TableNames.PGTIMESERIES_CONFIG + " ORDER BY table_name";
            try (PreparedStatement ps = conn.prepareStatement(sql); ResultSet rs = ps.executeQuery()) {
                while (rs.next()) {
                    table.addRow().addContent(rs.getString(1), rs.getString(2), rs.getString(3),
                            ShellSupport.nullToDash(rs.getString(4)), ShellSupport.nullToDash(rs.getString(5)),
                            ShellSupport.time(rs.getTimestamp(6) == null ? null : rs.getTimestamp(6).toInstant()));
                }
            }
            System.out.println();
            table.print(System.out);
            System.out.println();
        }

        final MaintenanceService ms = storage == null ? null : storage.getMaintenanceService();
        if (ms == null) {
            System.out.println("Partition maintenance is disabled (managePartitions=false); the policy is not applied by this plugin.");
        } else {
            final MaintenanceReport last = ms.getLastReport();
            System.out.println("Partition maintenance runs every " + ms.getIntervalMinutes() + " minute(s); last run: "
                    + (last == null ? "not yet" : ShellSupport.time(last.getStartedAt()) + " (" + last.summary() + ")"));
            System.out.println("Policy changes take effect on the next run; use opennms-pgtimeseries:maintenance run to apply them now.");
        }
        return null;
    }

    private static String currentWidth(final Connection conn) throws SQLException {
        try (PreparedStatement ps = conn.prepareStatement("SELECT partition_duration::text FROM "
                + TableNames.PGTIMESERIES_CONFIG + " WHERE table_name = ?")) {
            ps.setString(1, TableNames.PGTIMESERIES_TIME_SERIES);
            try (ResultSet rs = ps.executeQuery()) {
                return rs.next() ? rs.getString(1) : "1 minute";
            }
        }
    }

    /**
     * Rejects values that are almost certainly mistakes: a bare number (PostgreSQL would read "7" as seven
     * seconds, and a seven second retention drops all history on the next run), a non-positive interval, and an
     * interval shorter than {@code minimum} (the partition width for the policies, one minute for the width).
     * Returns null when the value is acceptable, otherwise the reason.
     */
    static String validate(final Connection conn, final String value, final String minimum) throws SQLException {
        if (value.matches("^-?\\d+(\\.\\d+)?$")) {
            return "'" + value + "' has no unit; PostgreSQL would read it as seconds. Use e.g. '" + value + " days'.";
        }
        try (PreparedStatement ps = conn.prepareStatement("SELECT ?::interval > interval '0', ?::interval >= ?::interval")) {
            ps.setString(1, value);
            ps.setString(2, value);
            ps.setString(3, minimum);
            try (ResultSet rs = ps.executeQuery()) {
                rs.next();
                if (!rs.getBoolean(1)) {
                    return "'" + value + "' is not a positive interval.";
                }
                if (!rs.getBoolean(2)) {
                    return "'" + value + "' is shorter than " + minimum + " (the partition width).";
                }
            }
        } catch (SQLException e) {
            return "'" + value + "' is not a valid PostgreSQL interval: " + e.getMessage().trim();
        }
        return null;
    }

    private static void update(final Connection conn, final String column, final String value, final boolean nullable,
                               final String label, final String minimum) throws SQLException {
        if (value == null || value.isBlank()) {
            return;
        }
        final boolean clear = NONE.equalsIgnoreCase(value.trim());
        if (clear && !nullable) {
            System.out.println("The " + label + " cannot be cleared; give an interval.");
            return;
        }
        if (!clear) {
            final String problem = validate(conn, value.trim(), minimum);
            if (problem != null) {
                System.out.println("Not changing the " + label + ": " + problem);
                return;
            }
        }
        final String sql = "UPDATE " + TableNames.PGTIMESERIES_CONFIG + " SET " + column + " = ?::interval, updated_at = now() "
                + "WHERE table_name = ?";
        try (PreparedStatement ps = conn.prepareStatement(sql)) {
            ps.setString(1, clear ? null : value.trim());
            ps.setString(2, TableNames.PGTIMESERIES_TIME_SERIES);
            if (ps.executeUpdate() == 0) {
                System.out.println("No policy row found for " + TableNames.PGTIMESERIES_TIME_SERIES
                        + "; run opennms-pgtimeseries:install first.");
            } else {
                System.out.println((clear ? "Cleared the " : "Set the ") + label + (clear ? "." : " to '" + value.trim() + "'."));
            }
        } catch (SQLException e) {
            System.out.println("Could not set the " + label + ": " + e.getMessage().trim());
        }
    }
}
