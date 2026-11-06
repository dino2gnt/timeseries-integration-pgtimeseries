package org.opennms.timeseries.impl.pgtimeseries.partition;

import java.time.Instant;

/**
 * One child partition of the time series table, as read from the PostgreSQL catalog.
 *
 * <p>Range partitions carry their bounds (lower inclusive, upper exclusive). The default partition has
 * {@code isDefault = true} and null bounds. Sizes are in bytes, as reported by {@code pg_table_size()},
 * {@code pg_indexes_size()} and {@code pg_total_relation_size()}.</p>
 */
public record Partition(
        String schema,
        String name,
        boolean isDefault,
        Instant lowerBound,
        Instant upperBound,
        long tableBytes,
        long indexBytes,
        long totalBytes,
        String accessMethod) {

    /** True for a bounded range partition (i.e. anything other than the default partition). */
    public boolean isRange() {
        return !isDefault && lowerBound != null && upperBound != null;
    }

    /** True if this range partition's [lower, upper) interval intersects the supplied [lo, hi) interval. */
    public boolean overlaps(final Instant lo, final Instant hi) {
        return isRange() && lowerBound.isBefore(hi) && upperBound.isAfter(lo);
    }

    /** True if the supplied instant falls inside this range partition. */
    public boolean contains(final Instant t) {
        return isRange() && !t.isBefore(lowerBound) && t.isBefore(upperBound);
    }

    /** Schema-qualified, identifier-quoted name suitable for use in DDL. */
    public String qualifiedName() {
        return PartitionManager.quoteIdent(schema) + "." + PartitionManager.quoteIdent(name);
    }

    /** The bounds as pg_get_expr() renders them, for display. */
    public String rangeDescription() {
        return isDefault ? "DEFAULT" : "FROM " + lowerBound + " TO " + upperBound;
    }
}
