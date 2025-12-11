
package org.opennms.timeseries.impl.pgtimeseries.shell;

import org.apache.karaf.shell.api.action.Action;
import org.apache.karaf.shell.api.action.Command;
import org.apache.karaf.shell.api.action.lifecycle.Reference;
import org.apache.karaf.shell.api.action.lifecycle.Service;
import org.apache.karaf.shell.support.table.ShellTable;
import org.opennms.timeseries.impl.pgtimeseries.util.DBUtils;
import org.opennms.timeseries.impl.pgtimeseries.util.PGTimeseriesDatabaseInitializer;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;

import javax.sql.DataSource;

@Command(scope = "opennms-pgtimeseries", name = "show-partition-info", description = "Display information about postgresql's time-series table partitions.")
@Service
public class ShowPartitionInfo implements Action {

    @Reference
    private DataSource dataSource;

    @Override
    public Object execute() throws SQLException {
        String sql;
        DBUtils db = new DBUtils();
        ShellTable table = new ShellTable();
        table.column("Table Name");
        table.column("Partition ID");
        table.column("Time Range");
        table.column("Table Size");
        table.column("Index Size");
        table.column("Total Size");
        table.column("Access Method");
        try {
            Connection conn = PGTimeseriesDatabaseInitializer.getWhichDataSourceConnection();
            db.watch(conn);
            sql = "select table_id, part_id, part_range, pg_size_pretty(table_size_bytes), pg_size_pretty(index_size_bytes), pg_size_pretty(total_size_bytes), access_method FROM ts_part_info order by part_range desc";
            PreparedStatement statement = conn.prepareStatement(sql);
            db.watch(statement);
            ResultSet rs = statement.executeQuery();
            while (rs.next()) {
                table.addRow().addContent(rs.getString(1), rs.getString(2),rs.getString(3),rs.getString(4), rs.getString(5), rs.getString(6), rs.getString(7) );
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
