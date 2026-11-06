package org.opennms.timeseries.impl.pgtimeseries.shell;

import java.sql.SQLException;

import javax.sql.DataSource;

import org.apache.karaf.shell.api.action.Action;
import org.apache.karaf.shell.api.action.Command;
import org.apache.karaf.shell.api.action.Option;
import org.apache.karaf.shell.api.action.lifecycle.Reference;
import org.apache.karaf.shell.api.action.lifecycle.Service;
import org.opennms.timeseries.impl.pgtimeseries.util.LegacyMigration;

@Command(scope = "opennms-pgtimeseries", name = "migrate-from-extension", description = "Hand partition management over from the pg_timeseries extension (0.2.x) to the plugin: import the policy, unschedule the pg_cron jobs, unregister the table from pg_partman and drop the extension.")
@Service
public class MigrateFromExtension implements Action {

    @Option(name = "--dry-run", description = "Only show what would be done.", required = false, multiValued = false)
    boolean dryRun;

    @Option(name = "--drop-orphans", description = "Also drop partition tables that pg_partman's retention detached but never dropped. Their data is gone for good.", required = false, multiValued = false)
    boolean dropOrphans;

    @Reference
    private DataSource dataSource;

    @Override
    public Object execute() throws SQLException {
        final LegacyMigration.Result result = LegacyMigration.run(new LegacyMigration.Options(dryRun, dropOrphans));
        System.out.println();
        for (final String line : result.getLines()) {
            System.out.println(line);
        }
        System.out.println();
        if (!dryRun) {
            System.out.println(result.isComplete()
                    ? "Migration complete. pg_cron and pg_partman were left installed; drop them if nothing else uses them."
                    : "Migration incomplete; run the SQL above as a superuser (or set adminDatasourceURL and re-run).");
        }
        return null;
    }
}
