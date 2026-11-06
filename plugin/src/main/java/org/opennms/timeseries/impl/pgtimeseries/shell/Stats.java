
package org.opennms.timeseries.impl.pgtimeseries.shell;

import java.util.concurrent.TimeUnit;

import org.apache.karaf.shell.api.action.Action;
import org.apache.karaf.shell.api.action.Command;
import org.apache.karaf.shell.api.action.lifecycle.Reference;
import org.apache.karaf.shell.api.action.lifecycle.Service;
import org.opennms.timeseries.impl.pgtimeseries.PGTimeseriesStorage;

import com.codahale.metrics.ConsoleReporter;


@Command(scope = "opennms-pgtimeseries", name = "stats", description = "Display read, write, lost-sample and partition maintenance metrics from the pgtimeseries plugin.")
@Service
public class Stats implements Action {

    @Reference
    private PGTimeseriesStorage pgts;

    @Override
    public Object execute() {
        ConsoleReporter reporter = ConsoleReporter.forRegistry(pgts.getMetrics())
                .convertRatesTo(TimeUnit.SECONDS)
                .convertDurationsTo(TimeUnit.MILLISECONDS)
                .build();
        reporter.report();
        return null;
    }

}
