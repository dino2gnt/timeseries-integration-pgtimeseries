package org.opennms.timeseries.impl.pgtimeseries;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.mock;

import java.sql.Timestamp;
import java.time.Instant;

import javax.sql.DataSource;

import org.junit.Before;
import org.junit.Test;
import org.opennms.integration.api.v1.timeseries.Aggregation;
import org.opennms.timeseries.impl.pgtimeseries.config.PGTimeseriesConfig;

/**
 * Unit tests for {@link PGTimeseriesStorage#buildFetchSql}. This covers the read-path query construction
 * (counter rate vs. gauge, aggregation function selection, step coercion, bucketing) without a database.
 */
public class FetchSqlTest {

    private PGTimeseriesStorage storage;
    private final Timestamp start = Timestamp.from(Instant.parse("2024-01-01T00:00:00Z"));
    private final Timestamp end = Timestamp.from(Instant.parse("2024-01-01T01:00:00Z"));

    @Before
    public void setUp() {
        storage = new PGTimeseriesStorage(PGTimeseriesConfig.builder().build(), mock(DataSource.class));
    }

    private static int countPlaceholders(String sql) {
        int c = 0;
        for (int i = 0; i < sql.length(); i++) {
            if (sql.charAt(i) == '?') {
                c++;
            }
        }
        return c;
    }

    private void assertCommonShape(String sql) {
        // bucketed equi-join, not the old buckets x samples range join
        assertTrue("should bucket with date_bin", sql.contains("date_bin("));
        assertTrue("should build the time axis with generate_series", sql.contains("generate_series("));
        assertTrue("empty buckets should be filled with NaN", sql.contains("'NaN'"));
        assertFalse("should not use the old interval range-join", sql.contains(">= f.start_time"));
        // exactly one bind parameter: the keyid
        assertEquals(1, countPlaceholders(sql));
    }

    @Test
    public void counterUsesRateWithSumForAggregationNone() {
        String sql = storage.buildFetchSql("counter", Aggregation.NONE, start, end, 30);
        assertCommonShape(sql);
        assertTrue("counters compute a delta via lag()", sql.contains("lag(value)"));
        assertTrue("NONE on a counter sums the deltas", sql.contains("sum(d.deltaval)"));
        assertTrue("rate divides the delta by the step", sql.contains("/ 30"));
    }

    @Test
    public void countTypeIsTreatedAsCounter() {
        String sql = storage.buildFetchSql("count", Aggregation.NONE, start, end, 30);
        assertTrue(sql.contains("lag(value)"));
        assertTrue(sql.contains("sum(d.deltaval)"));
    }

    @Test
    public void counterWithExplicitAggregationUsesThatFunction() {
        assertTrue(storage.buildFetchSql("counter", Aggregation.MAX, start, end, 30).contains("max(d.deltaval) / 30"));
        assertTrue(storage.buildFetchSql("counter", Aggregation.MIN, start, end, 30).contains("min(d.deltaval) / 30"));
        assertTrue(storage.buildFetchSql("counter", Aggregation.AVERAGE, start, end, 30).contains("avg(d.deltaval) / 30"));
    }

    @Test
    public void gaugeNoneReturnsLatestRawValuePerBucket() {
        String sql = storage.buildFetchSql("gauge", Aggregation.NONE, start, end, 30);
        assertCommonShape(sql);
        assertFalse("gauges do not compute a delta", sql.contains("lag(value)"));
        assertTrue("NONE on a gauge picks the latest value per bucket", sql.contains("DISTINCT ON (bucket)"));
        assertTrue(sql.contains("COALESCE(d.raw_value, 'NaN')"));
    }

    @Test
    public void gaugeWithAggregationAppliesTheFunctionToRawValues() {
        assertTrue(storage.buildFetchSql("gauge", Aggregation.AVERAGE, start, end, 30).contains("avg(d.raw_value)"));
        assertTrue(storage.buildFetchSql("gauge", Aggregation.MAX, start, end, 30).contains("max(d.raw_value)"));
        assertTrue(storage.buildFetchSql("gauge", Aggregation.MIN, start, end, 30).contains("min(d.raw_value)"));
        // gauge with an aggregation must not fall into the DISTINCT ON (NONE) branch
        assertFalse(storage.buildFetchSql("gauge", Aggregation.AVERAGE, start, end, 30).contains("DISTINCT ON"));
    }

    @Test
    public void nonPositiveStepIsCoercedToOneSecond() {
        String zero = storage.buildFetchSql("gauge", Aggregation.NONE, start, end, 0);
        String negative = storage.buildFetchSql("gauge", Aggregation.NONE, start, end, -5);
        assertTrue(zero.contains("'1 seconds'::interval"));
        assertTrue(negative.contains("'1 seconds'::interval"));
    }

    @Test
    public void stepValueIsRenderedIntoTheQuery() {
        String sql = storage.buildFetchSql("gauge", Aggregation.AVERAGE, start, end, 300);
        assertTrue(sql.contains("'300 seconds'::interval"));
    }
}
