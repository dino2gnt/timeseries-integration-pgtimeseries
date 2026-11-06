package org.opennms.timeseries.impl.pgtimeseries.shell;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.List;

import org.apache.karaf.shell.api.action.Action;
import org.apache.karaf.shell.api.action.Argument;
import org.apache.karaf.shell.api.action.Command;
import org.apache.karaf.shell.api.action.lifecycle.Reference;
import org.apache.karaf.shell.api.action.lifecycle.Service;
import org.opennms.timeseries.impl.pgtimeseries.PGTimeseriesStorage;
import org.opennms.timeseries.impl.pgtimeseries.partition.MaintenanceReport;
import org.opennms.timeseries.impl.pgtimeseries.partition.MaintenanceService;
import org.opennms.timeseries.impl.pgtimeseries.partition.PartitionManager;
import org.opennms.timeseries.impl.pgtimeseries.partition.PartitionManager.OrphanTable;
import org.opennms.timeseries.impl.pgtimeseries.util.PGTimeseriesDatabaseHelpers;
import org.opennms.timeseries.impl.pgtimeseries.util.SchemaManager;

@Command(scope = "opennms-pgtimeseries", name = "maintenance", description = "Show the state of partition maintenance, run it now, or preview what a run would do.")
@Service
public class Maintenance implements Action {

    @Argument(index = 0, name = "action", description = "status (default), run, or dry-run", required = false, multiValued = false)
    String action = "status";

    /** Optional so the command stays available when the storage failed to initialise, which is when it is needed. */
    @Reference(optional = true)
    private PGTimeseriesStorage storage;

    @Override
    public Object execute() throws SQLException {
        final MaintenanceService service = storage == null ? null : storage.getMaintenanceService();
        switch (action == null ? "status" : action.trim().toLowerCase()) {
            case "run":
                run(service);
                break;
            case "dry-run":
            case "dryrun":
            case "plan":
                print(SchemaManager.newPartitionManager().runMaintenance(true));
                break;
            case "status":
                status(service);
                break;
            default:
                System.out.println("Unknown action '" + action + "'. Use status, run or dry-run.");
        }
        return null;
    }

    private void run(final MaintenanceService service) {
        final MaintenanceReport report = service != null
                ? service.runNow()
                : SchemaManager.newPartitionManager().runMaintenance();
        print(report);
    }

    private void status(final MaintenanceService service) throws SQLException {
        if (service == null) {
            System.out.println("Scheduled maintenance: disabled (managePartitions=false or plugin not initialised).");
        } else {
            System.out.println("Scheduled maintenance: every " + service.getIntervalMinutes() + " minute(s), "
                    + (service.isRunning() ? "running" : "stopped"));
            final MaintenanceReport last = service.getLastReport();
            if (last == null) {
                System.out.println("Last run: none yet");
            } else {
                System.out.println("Last run: " + ShellSupport.time(last.getStartedAt()) + ", " + last.summary());
            }
        }

        final PartitionManager pm = SchemaManager.newPartitionManager();
        try (Connection c = PGTimeseriesDatabaseHelpers.getWhichDataSourceConnection()) {
            final long partitions = pm.listPartitions(c).stream().filter(p -> p.isRange()).count();
            System.out.println("Range partitions: " + partitions);
            final long stray = pm.countDefaultPartitionRows(c);
            System.out.println("Rows in the default partition: " + stray
                    + (stray > 0 ? " (will be moved into proper partitions on the next run)" : ""));
            final List<OrphanTable> orphans = pm.listOrphans(c);
            if (!orphans.isEmpty()) {
                long bytes = 0;
                for (final OrphanTable o : orphans) {
                    bytes += o.totalBytes();
                }
                System.out.println("Orphan partition tables (detached, never dropped): " + orphans.size() + " holding "
                        + ShellSupport.prettyBytes(bytes) + "; see opennms-pgtimeseries:migrate-from-extension --drop-orphans");
            }
        }
        if (SchemaManager.isLegacyExtensionPresent() || SchemaManager.isLegacyPartmanManaged()) {
            System.out.println("Legacy: the pg_timeseries extension is still present; run opennms-pgtimeseries:migrate-from-extension.");
        }
    }

    private static void print(final MaintenanceReport report) {
        System.out.println((report.isDryRun() ? "Dry run " : "Maintenance ") + ShellSupport.time(report.getStartedAt())
                + ": " + report.summary());
        list(report.isDryRun() ? "Would create" : "Created", report.getCreated());
        list(report.isDryRun() ? "Would rescue into" : "Rescued stray rows into", report.getRescued());
        list(report.isDryRun() ? "Would drop" : "Dropped", report.getDropped());
        list(report.isDryRun() ? "Would compress" : "Compressed", report.getCompressed());
        list("Errors", report.getErrors());
    }

    private static void list(final String label, final List<String> items) {
        if (items.isEmpty()) {
            return;
        }
        System.out.println(label + ":");
        for (final String item : items) {
            System.out.println("  " + item);
        }
    }
}
