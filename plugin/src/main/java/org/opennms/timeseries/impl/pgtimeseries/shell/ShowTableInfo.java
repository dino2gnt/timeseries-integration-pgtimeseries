package org.opennms.timeseries.impl.pgtimeseries.shell;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.List;

import javax.sql.DataSource;

import org.apache.karaf.shell.api.action.Action;
import org.apache.karaf.shell.api.action.Command;
import org.apache.karaf.shell.api.action.lifecycle.Reference;
import org.apache.karaf.shell.api.action.lifecycle.Service;
import org.apache.karaf.shell.support.table.ShellTable;
import org.opennms.timeseries.impl.pgtimeseries.partition.Partition;
import org.opennms.timeseries.impl.pgtimeseries.util.PGTimeseriesDatabaseHelpers;
import org.opennms.timeseries.impl.pgtimeseries.util.SchemaManager;
import org.opennms.timeseries.impl.pgtimeseries.util.TableNames;

@Command(scope = "opennms-pgtimeseries", name = "show-table-info", description = "Display the size of the pgtimeseries tables (the time series table summed over its partitions).")
@Service
public class ShowTableInfo implements Action {

    @Reference
    private DataSource dataSource;

    @Override
    public Object execute() throws SQLException {
        final ShellTable table = new ShellTable();
        table.column("Name");
        table.column("Partitions");
        table.column("Table Size");
        table.column("Index Size");
        table.column("Total Size");

        final List<Partition> partitions = SchemaManager.newPartitionManager().listPartitions();
        long tableBytes = 0;
        long indexBytes = 0;
        long totalBytes = 0;
        for (final Partition p : partitions) {
            tableBytes += p.tableBytes();
            indexBytes += p.indexBytes();
            totalBytes += p.totalBytes();
        }
        table.addRow().addContent(TableNames.PGTIMESERIES_TIME_SERIES, partitions.size(),
                ShellSupport.prettyBytes(tableBytes), ShellSupport.prettyBytes(indexBytes), ShellSupport.prettyBytes(totalBytes));

        try (Connection c = PGTimeseriesDatabaseHelpers.getWhichDataSourceConnection();
             PreparedStatement ps = c.prepareStatement(
                     "SELECT pg_table_size(?::regclass), pg_indexes_size(?::regclass), pg_total_relation_size(?::regclass)")) {
            for (final String name : new String[] {TableNames.PGTIMESERIES_METRIC, TableNames.PGTIMESERIES_TAG}) {
                ps.setString(1, name);
                ps.setString(2, name);
                ps.setString(3, name);
                try (ResultSet rs = ps.executeQuery()) {
                    if (rs.next()) {
                        table.addRow().addContent(name, "-", ShellSupport.prettyBytes(rs.getLong(1)),
                                ShellSupport.prettyBytes(rs.getLong(2)), ShellSupport.prettyBytes(rs.getLong(3)));
                    }
                }
            }
        }
        System.out.println();
        table.print(System.out);
        System.out.println();
        return null;
    }
}
