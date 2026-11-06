package org.opennms.timeseries.impl.pgtimeseries.partition;

/**
 * The partition management policy for the time series table, as stored in the plugin's config table.
 * All values are PostgreSQL interval strings; {@code retention} and {@code compression} may be null, which
 * disables the corresponding step.
 */
public record PartitionPolicy(
        String partitionDuration,
        String partitionLeadTime,
        String retention,
        String compression) {

    public boolean hasRetention() {
        return retention != null && !retention.isBlank();
    }

    public boolean hasCompression() {
        return compression != null && !compression.isBlank();
    }
}
