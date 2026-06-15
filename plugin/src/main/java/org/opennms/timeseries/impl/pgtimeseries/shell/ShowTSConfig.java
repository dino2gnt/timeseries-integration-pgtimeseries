package org.opennms.timeseries.impl.pgtimeseries.shell;

import org.apache.karaf.shell.api.action.Action;
import org.apache.karaf.shell.api.action.Command;
import org.apache.karaf.shell.api.action.Option;
import org.apache.karaf.shell.api.action.lifecycle.Reference;
import org.apache.karaf.shell.api.action.lifecycle.Service;
import org.apache.karaf.shell.support.table.ShellTable;
import org.opennms.timeseries.impl.pgtimeseries.util.DBUtils;
import org.opennms.timeseries.impl.pgtimeseries.util.PGTimeseriesDatabaseHelpers;

import javax.sql.DataSource;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;

@Command(scope = "opennms-pgtimeseries", name = "ts-config", description = "Display information about postgresql's time-series configuration")
@Service
public class ShowTSConfig implements Action {

    @Option(name = "-r", aliases = "--retention", description = "Retention interval. Partitions older than this are deleted. This is a standard PostgreSQL interval. Example: '9 months', '1 year`, etc", required = false, multiValued = false)
    String retentionInterval;

    @Option(name = "-c", aliases = "--compression", description = "Compress partitions after this interval. This is a standard PostgreSQL interval. Example: '3 months', '1 week`, etc", required = false, multiValued = false)
    String compressionInterval;

    @Option(name = "-p", aliases = "--partition", description = "Partition time interval.  Not currently implemented / not supported by timeseries extension.", required = false, multiValued = false)
    String partitionInterval;

    @Reference
    private DataSource dataSource;

    @Override
    public Object execute() throws SQLException {
        String sql;
        DBUtils db = new DBUtils();
        if (partitionInterval != null && !partitionInterval.isEmpty()) {
            System.out.println("Modifying partition width is not currently implemented.");
            return null;
        }
        try {
            Connection conn = PGTimeseriesDatabaseHelpers.getWhichDataSourceConnection();
            db.watch(conn);
            PreparedStatement statement = null;
            // configure retention
            if (retentionInterval != null && !retentionInterval.isEmpty()) {
                System.out.print("Setting new retention policy...");
                sql = "SELECT set_ts_retention_policy('pgtimeseries_time_series', cast(? as interval))";
                statement = conn.prepareStatement(sql);
                db.watch(statement);
                statement.setString(1, retentionInterval);
                statement.executeQuery();
                System.out.println("...Done!");
            }

            // configure compression
            if (compressionInterval != null && !compressionInterval.isEmpty()) {
                System.out.print("Setting new compression policy");
                sql = "SELECT set_ts_compression_policy('pgtimeseries_time_series', cast(? as interval))";
                statement = conn.prepareStatement(sql);
                db.watch(statement);
                statement.setString(1, compressionInterval);
                statement.executeQuery();
                System.out.println("...Done!");
            }
            // display what we've got.
            ShellTable table = new ShellTable();
            table.column("Name");
            table.column("Partition Duration");
            table.column("Partition Lead Time");
            table.column("Retention");
            table.column("Compression After");

            sql = "select table_id,partition_duration,partition_lead_time,retention_duration,compression_duration from ts_config";
            statement = conn.prepareStatement(sql);
            db.watch(statement);
            ResultSet rs = statement.executeQuery();
            while (rs.next()) {
                table.addRow().addContent(rs.getString(1), rs.getString(2), rs.getString(3), rs.getString(4), rs.getString(5));
            }
            rs.close();
            System.out.println();
            table.print(System.out);
            System.out.println();
        }
        catch (SQLException e) {
            throw new SQLException(e);
        } finally {
            db.cleanUp();
        }
        return null;
    }
}
