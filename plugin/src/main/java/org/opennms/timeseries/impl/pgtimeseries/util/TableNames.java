package org.opennms.timeseries.impl.pgtimeseries.util;

public interface TableNames {
    String PGTIMESERIES_TIME_SERIES = "pgtimeseries_time_series";
    String PGTIMESERIES_METRIC = "pgtimeseries_metric";
    String PGTIMESERIES_TAG = "pgtimeseries_tag";
    /** Partition policy table; replaces the pg_timeseries extension's ts_config. */
    String PGTIMESERIES_CONFIG = "pgtimeseries_config";
    /** The default partition, named as pg_partman named it. */
    String PGTIMESERIES_DEFAULT_PARTITION = PGTIMESERIES_TIME_SERIES + "_default";
}
