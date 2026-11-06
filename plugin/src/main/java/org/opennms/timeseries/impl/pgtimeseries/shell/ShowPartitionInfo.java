package org.opennms.timeseries.impl.pgtimeseries.shell;

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;

import javax.sql.DataSource;

import org.apache.karaf.shell.api.action.Action;
import org.apache.karaf.shell.api.action.Command;
import org.apache.karaf.shell.api.action.lifecycle.Reference;
import org.apache.karaf.shell.api.action.lifecycle.Service;
import org.apache.karaf.shell.support.table.ShellTable;
import org.opennms.timeseries.impl.pgtimeseries.partition.Partition;
import org.opennms.timeseries.impl.pgtimeseries.util.SchemaManager;
import org.opennms.timeseries.impl.pgtimeseries.util.TableNames;

@Command(scope = "opennms-pgtimeseries", name = "show-partition-info", description = "Display the time series table's partitions, newest first, with their sizes.")
@Service
public class ShowPartitionInfo implements Action {

    @Reference
    private DataSource dataSource;

    @Override
    public Object execute() throws SQLException {
        final List<Partition> partitions = new ArrayList<>(SchemaManager.newPartitionManager().listPartitions());
        // newest range partition first, the default partition last
        partitions.sort(Comparator.comparing(Partition::isDefault)
                .thenComparing(Partition::lowerBound, Comparator.nullsLast(Comparator.reverseOrder())));

        final ShellTable table = new ShellTable();
        table.column("Table Name");
        table.column("Partition ID");
        table.column("Time Range");
        table.column("Table Size");
        table.column("Index Size");
        table.column("Total Size");
        table.column("Access Method");
        for (final Partition p : partitions) {
            table.addRow().addContent(
                    TableNames.PGTIMESERIES_TIME_SERIES,
                    p.name(),
                    p.isDefault() ? "DEFAULT" : ShellSupport.time(p.lowerBound()) + " to " + ShellSupport.time(p.upperBound()),
                    ShellSupport.prettyBytes(p.tableBytes()),
                    ShellSupport.prettyBytes(p.indexBytes()),
                    ShellSupport.prettyBytes(p.totalBytes()),
                    ShellSupport.nullToDash(p.accessMethod()));
        }
        System.out.println();
        table.print(System.out);
        System.out.println();
        return null;
    }
}
