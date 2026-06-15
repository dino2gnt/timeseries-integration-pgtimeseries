package org.opennms.timeseries.impl.pgtimeseries.config;

import java.util.StringJoiner;

public class PGTimeseriesConfig {

    private final String externalDatasourceURL;
    private final String adminDatasourceURL;
    private final String partitionDuration;
    private final String retentionPolicy;
    private final String compressionPolicy;
    private final String backfillStart;
    private final boolean createTablesOnInstall;
    private final int maxBatchSize;
    private final int connectionPoolSize;
    private final int flushMaxSamples;
    private final long flushMaxIntervalMs;

    public PGTimeseriesConfig() {
        this(builder());
    }

    public PGTimeseriesConfig(Builder builder) {
        this.externalDatasourceURL = builder.externalDatasourceURL;
        this.adminDatasourceURL = builder.adminDatasourceURL;
        this.partitionDuration = builder.partitionDuration;
        this.retentionPolicy = builder.retentionPolicy;
        this.compressionPolicy = builder.compressionPolicy;
        this.backfillStart = builder.backfillStart;
        this.createTablesOnInstall = builder.createTablesOnInstall;
        this.maxBatchSize = builder.maxBatchSize;
        this.connectionPoolSize = builder.connectionPoolSize;
        this.flushMaxSamples = builder.flushMaxSamples;
        this.flushMaxIntervalMs = builder.flushMaxIntervalMs;
    }

    /** Will be called via blueprint. The builder can be called when not running as Osgi plugin. */
    public PGTimeseriesConfig(
            final String externalDatasourceURL,
            final String adminDatasourceURL,
            final String partitionDuration,
            final String retentionPolicy,
            final String compressionPolicy,
            final String backfillStart,
            final boolean createTablesOnInstall,
            final int maxBatchSize,
            final int connectionPoolSize,
            final int flushMaxSamples,
            final long flushMaxIntervalMs) {
        this(builder()
                .externalDatasourceURL(externalDatasourceURL)
                .adminDatasourceURL(adminDatasourceURL)
                .partitionDuration(partitionDuration)
                .retentionPolicy(retentionPolicy)
                .compressionPolicy(compressionPolicy)
                .backfillStart(backfillStart)
                .createTablesOnInstall(createTablesOnInstall)
                .maxBatchSize(maxBatchSize)
                .connectionPoolSize(connectionPoolSize)
                .flushMaxSamples(flushMaxSamples)
                .flushMaxIntervalMs(flushMaxIntervalMs));
    }

    public String getExternalDatasourceURL() {
        return externalDatasourceURL;
    }

    public String getAdminDatasourceURL() {
        return adminDatasourceURL;
    }

    public String getpartitionDuration() {
        return partitionDuration;
    }

    public String getretentionPolicy() {
        return retentionPolicy;
    }

    public String getcompressionPolicy() {
        return compressionPolicy;
    }

    public String getbackfillStart() {
        return backfillStart;
    }

    public boolean getCreateTablesOnInstall() {
        return createTablesOnInstall;
    }

    public int getMaxBatchSize() {
        return maxBatchSize;
    }

    public int getConnectionPoolSize() {
        return connectionPoolSize;
    }

    /** Maximum number of buffered samples that will accumulate across {@code store()} calls before a flush is triggered. */
    public int getFlushMaxSamples() {
        return flushMaxSamples;
    }

    /** Upper bound, in milliseconds, on how long a buffered sample may wait before it is flushed regardless of count. */
    public long getFlushMaxIntervalMs() {
        return flushMaxIntervalMs;
    }

    public static Builder builder() {
        return new Builder();
    }

    public final static class Builder {
        private String externalDatasourceURL = "";
        private String adminDatasourceURL = "";
        private String partitionDuration = "1 week";
        private String retentionPolicy = "1 year";
        private String compressionPolicy = "3 months";
        private String backfillStart = null;
        private boolean createTablesOnInstall = true;
        private int maxBatchSize = 100;
        private int connectionPoolSize = 100;
        private int flushMaxSamples = 1000;
        private long flushMaxIntervalMs = 5000;

        public Builder externalDatasourceURL(final String externalDatasourceURL) {
            this.externalDatasourceURL = externalDatasourceURL;
            return this;
        }

        public Builder adminDatasourceURL(final String adminDatasourceURL) {
            this.adminDatasourceURL = adminDatasourceURL;
            return this;
        }

        public Builder partitionDuration(final String partitionDuration) {
            this.partitionDuration = partitionDuration;
            return this;
        }

        public Builder retentionPolicy(final String retentionPolicy) {
            this.retentionPolicy = retentionPolicy;
            return this;
        }

        public Builder compressionPolicy(final String compressionPolicy) {
            this.compressionPolicy = compressionPolicy;
            return this;
        }

        public Builder backfillStart(final String backfillStart) {
            this.backfillStart = backfillStart;
            return this;
        }

        public Builder createTablesOnInstall(final boolean createTablesOnInstall) {
            this.createTablesOnInstall = createTablesOnInstall;
            return this;
        }

        public Builder maxBatchSize(final int maxBatchSize) {
            this.maxBatchSize = maxBatchSize;
            return this;
        }

        public Builder connectionPoolSize(final int connectionPoolSize) {
            this.connectionPoolSize = connectionPoolSize;
            return this;
        }

        public Builder flushMaxSamples(final int flushMaxSamples) {
            this.flushMaxSamples = flushMaxSamples;
            return this;
        }

        public Builder flushMaxIntervalMs(final long flushMaxIntervalMs) {
            this.flushMaxIntervalMs = flushMaxIntervalMs;
            return this;
        }

        public PGTimeseriesConfig build() {
            return new PGTimeseriesConfig(this);
        }
    }

    @Override
    public String toString() {
        return new StringJoiner(", ", PGTimeseriesConfig.class.getSimpleName() + "[", "]")
                .add("externalDatasourceURL='" + externalDatasourceURL + "'")
                .add("adminDatasourceURL='" + adminDatasourceURL + "'")
                .add("partitionDuration='" + partitionDuration + "'")
                .add("retentionPolicy='" + retentionPolicy + "'")
                .add("compressionPolicy='" + compressionPolicy + "'")
                .add("backfillStart='" + backfillStart + "'")
                .add("createTablesOnInstall=" + createTablesOnInstall)
                .add("maxBatchSize='" + maxBatchSize + "'")
                .add("connectionPoolSize='" + connectionPoolSize + "'" )
                .add("flushMaxSamples='" + flushMaxSamples + "'")
                .add("flushMaxIntervalMs='" + flushMaxIntervalMs + "'")
                .toString();
    }
}
