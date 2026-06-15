package org.opennms.timeseries.impl.pgtimeseries.config;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import org.junit.Test;

/**
 * Unit tests for {@link PGTimeseriesConfig}. Pure value-object behaviour; no database required.
 */
public class PGTimeseriesConfigTest {

    @Test
    public void buildsWithDocumentedDefaults() {
        PGTimeseriesConfig config = PGTimeseriesConfig.builder().build();

        assertEquals("", config.getExternalDatasourceURL());
        assertEquals("", config.getAdminDatasourceURL());
        assertEquals("1 week", config.getpartitionDuration());
        assertEquals("1 year", config.getretentionPolicy());
        assertEquals("3 months", config.getcompressionPolicy());
        assertEquals(null, config.getbackfillStart());
        assertTrue(config.getCreateTablesOnInstall());
        assertEquals(100, config.getMaxBatchSize());
        assertEquals(100, config.getConnectionPoolSize());
        // new write-behind buffer settings
        assertEquals(1000, config.getFlushMaxSamples());
        assertEquals(5000L, config.getFlushMaxIntervalMs());
    }

    @Test
    public void builderRoundTripsAllValues() {
        PGTimeseriesConfig config = PGTimeseriesConfig.builder()
                .externalDatasourceURL("jdbc:postgresql://ext/db")
                .adminDatasourceURL("jdbc:postgresql://admin/db")
                .partitionDuration("1 day")
                .retentionPolicy("6 months")
                .compressionPolicy("1 month")
                .backfillStart("2020-01-01")
                .createTablesOnInstall(false)
                .maxBatchSize(250)
                .connectionPoolSize(20)
                .flushMaxSamples(5000)
                .flushMaxIntervalMs(2500L)
                .build();

        assertEquals("jdbc:postgresql://ext/db", config.getExternalDatasourceURL());
        assertEquals("jdbc:postgresql://admin/db", config.getAdminDatasourceURL());
        assertEquals("1 day", config.getpartitionDuration());
        assertEquals("6 months", config.getretentionPolicy());
        assertEquals("1 month", config.getcompressionPolicy());
        assertEquals("2020-01-01", config.getbackfillStart());
        assertEquals(false, config.getCreateTablesOnInstall());
        assertEquals(250, config.getMaxBatchSize());
        assertEquals(20, config.getConnectionPoolSize());
        assertEquals(5000, config.getFlushMaxSamples());
        assertEquals(2500L, config.getFlushMaxIntervalMs());
    }

    @Test
    public void allArgsConstructorMatchesBlueprintArgumentOrder() {
        // This mirrors the <argument> order in blueprint.xml; a mismatch here would mean the OSGi wiring
        // assigns values to the wrong fields.
        PGTimeseriesConfig config = new PGTimeseriesConfig(
                "jdbc:ext", "jdbc:admin", "2 weeks", "2 years", "4 months",
                "2021-06-01", true, 300, 30, 7000, 9000L);

        assertEquals("jdbc:ext", config.getExternalDatasourceURL());
        assertEquals("jdbc:admin", config.getAdminDatasourceURL());
        assertEquals("2 weeks", config.getpartitionDuration());
        assertEquals("2 years", config.getretentionPolicy());
        assertEquals("4 months", config.getcompressionPolicy());
        assertEquals("2021-06-01", config.getbackfillStart());
        assertTrue(config.getCreateTablesOnInstall());
        assertEquals(300, config.getMaxBatchSize());
        assertEquals(30, config.getConnectionPoolSize());
        assertEquals(7000, config.getFlushMaxSamples());
        assertEquals(9000L, config.getFlushMaxIntervalMs());
    }

    @Test
    public void toStringIncludesFlushSettings() {
        String s = PGTimeseriesConfig.builder()
                .flushMaxSamples(1234)
                .flushMaxIntervalMs(4321L)
                .build()
                .toString();
        assertTrue(s.contains("flushMaxSamples='1234'"));
        assertTrue(s.contains("flushMaxIntervalMs='4321'"));
    }
}
