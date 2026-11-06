package org.opennms.timeseries.impl.pgtimeseries.shell;

import org.apache.karaf.shell.api.action.Action;
import org.apache.karaf.shell.api.action.Command;
import org.apache.karaf.shell.api.action.lifecycle.Reference;
import org.apache.karaf.shell.api.action.lifecycle.Service;
import org.opennms.timeseries.impl.pgtimeseries.util.SchemaManager;

import javax.sql.DataSource;
import java.sql.SQLException;

@Command(scope = "opennms-pgtimeseries", name = "install", description = "Create the database tables and partitions for the pgtimeseries plugin if they do not exist.")
@Service
public class Install implements Action {
    @Reference
    private DataSource dataSource;

    @Override
    public Object execute() throws SQLException {
        if (SchemaManager.tablesExist()) {
            System.out.println("pgtimeseries tables exist; verifying the schema is complete...");
        } else {
            System.out.println("pgtimeseries tables are missing or incomplete; creating them now...");
        }
        final var report = SchemaManager.createSchema();
        System.out.println("Schema complete. No PostgreSQL extension is required.");
        if (report != null) {
            System.out.println("Partitions: " + report.summary());
        }
        if (SchemaManager.isLegacyExtensionPresent()) {
            System.out.println("Note: the pg_timeseries extension is installed in this database but is no longer used. "
                    + "Run opennms-pgtimeseries:migrate-from-extension to hand partition management over to the plugin.");
        }
        return null;
    }
}
