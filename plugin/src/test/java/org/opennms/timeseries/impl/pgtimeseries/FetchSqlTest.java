package org.opennms.timeseries.impl.pgtimeseries;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.mock;

import java.time.Instant;
import java.time.OffsetDateTime;
import java.time.ZoneOffset;
import java.util.List;
import java.util.TimeZone;

import javax.sql.DataSource;

import org.junit.Before;
import org.junit.Test;
import org.opennms.integration.api.v1.timeseries.Aggregation;
import org.opennms.timeseries.impl.pgtimeseries.PGTimeseriesStorage.FetchQuery;
import org.opennms.timeseries.impl.pgtimeseries.config.PGTimeseriesConfig;

/**
 * Unit tests for {@link PGTimeseriesStorage#buildFetchQuery}. This covers the read-path query construction
 * (counter rate vs. gauge, aggregation function selection, step coercion, bucketing, bind parameters) without a
 * database.
 */
public class FetchSqlTest {

    private PGTimeseriesStorage storage;
    private final Instant start = Instant.parse("2024-01-01T00:00:00Z");
    private final Instant end = Instant.parse("2024-01-01T01:00:00Z");
    private static final int KEYID = 42;

    @Before
    public void setUp() {
        storage = new PGTimeseriesStorage(PGTimeseriesConfig.builder().build(), mock(DataSource.class));
    }

    private FetchQuery query(String mtype, Aggregation aggregation, long step) {
        return storage.buildFetchQuery(mtype, aggregation, start, end, step, KEYID);
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

    private void assertCommonShape(FetchQuery q, long step) {
        final String sql = q.sql();
        // bucketed equi-join, not the old buckets x samples range join
        assertTrue("should bucket with date_bin", sql.contains("date_bin("));
        assertTrue("should build the time axis with generate_series", sql.contains("generate_series("));
        assertTrue("empty buckets should be filled with NaN", sql.contains("'NaN'"));
        assertFalse("should not use the old interval range-join", sql.contains(">= f.start_time"));
        // every value is bound, nothing is spliced into the text
        assertFalse("no timestamp literal may appear in the SQL", sql.contains("2024-01-01"));
        assertEquals("one bind value per placeholder", countPlaceholders(sql), q.params().size());
        final List<Object> p = q.params();
        final OffsetDateTime from = start.atOffset(ZoneOffset.UTC);
        final OffsetDateTime to = end.atOffset(ZoneOffset.UTC);
        assertEquals(List.of(from, to, step, step, from, KEYID, from, to), p.subList(0, 8));
    }

    @Test
    public void counterUsesRateWithSumForAggregationNone() {
        FetchQuery q = query("counter", Aggregation.NONE, 30);
        assertCommonShape(q, 30);
        String sql = q.sql();
        assertTrue("counters compute a delta via lag()", sql.contains("lag(value)"));
        assertTrue("NONE on a counter sums the deltas", sql.contains("sum(d.deltaval)"));
        assertTrue("rate divides the delta by the step", sql.contains("/ ?::double precision"));
        assertEquals("the divisor is the last bind value", 30L, q.params().get(q.params().size() - 1));
        // RRDtool-style counter-wrap correction: 32-bit (2^32) then 64-bit (2^64) on a negative delta,
        // and NULL (-> NaN) if it is still negative after correction (e.g. a counter reset).
        assertTrue("32-bit wrap correction", sql.contains("delta + 4294967296::double precision >= 0"));
        assertTrue("64-bit wrap correction", sql.contains("delta + 18446744073709551616::double precision >= 0"));
        assertTrue("still-negative -> NaN", sql.contains("ELSE NULL"));
    }

    @Test
    public void countTypeIsTreatedAsCounter() {
        String sql = query("count", Aggregation.NONE, 30).sql();
        assertTrue(sql.contains("lag(value)"));
        assertTrue(sql.contains("sum(d.deltaval)"));
    }

    @Test
    public void counterWithExplicitAggregationUsesThatFunction() {
        assertTrue(query("counter", Aggregation.MAX, 30).sql().contains("max(d.deltaval) / ?"));
        assertTrue(query("counter", Aggregation.MIN, 30).sql().contains("min(d.deltaval) / ?"));
        assertTrue(query("counter", Aggregation.AVERAGE, 30).sql().contains("avg(d.deltaval) / ?"));
    }

    @Test
    public void gaugeNoneReturnsLatestRawValuePerBucket() {
        FetchQuery q = query("gauge", Aggregation.NONE, 30);
        assertCommonShape(q, 30);
        String sql = q.sql();
        assertFalse("gauges do not compute a delta", sql.contains("lag(value)"));
        assertTrue("NONE on a gauge picks the latest value per bucket", sql.contains("DISTINCT ON (bucket)"));
        assertTrue(sql.contains("COALESCE(d.raw_value, 'NaN')"));
        assertEquals("gauges have no rate divisor", 8, q.params().size());
    }

    @Test
    public void gaugeWithAggregationAppliesTheFunctionToRawValues() {
        assertCommonShape(query("gauge", Aggregation.AVERAGE, 30), 30);
        assertTrue(query("gauge", Aggregation.AVERAGE, 30).sql().contains("avg(d.raw_value)"));
        assertTrue(query("gauge", Aggregation.MAX, 30).sql().contains("max(d.raw_value)"));
        assertTrue(query("gauge", Aggregation.MIN, 30).sql().contains("min(d.raw_value)"));
        // gauge with an aggregation must not fall into the DISTINCT ON (NONE) branch
        assertFalse(query("gauge", Aggregation.AVERAGE, 30).sql().contains("DISTINCT ON"));
    }

    @Test
    public void nonPositiveStepIsCoercedToOneSecond() {
        assertEquals(1L, query("gauge", Aggregation.NONE, 0).params().get(2));
        assertEquals(1L, query("gauge", Aggregation.NONE, -5).params().get(2));
    }

    @Test
    public void stepIsBoundNotRendered() {
        FetchQuery q = query("gauge", Aggregation.AVERAGE, 300);
        assertTrue(q.sql().contains("? * interval '1 second'"));
        assertFalse(q.sql().contains("300"));
        assertEquals(300L, q.params().get(2));
        assertEquals(300L, q.params().get(3));
    }

    @Test
    public void timestampsCarryAnExplicitOffsetWhateverTheJvmTimeZone() {
        // The old query rendered java.sql.Timestamp.toString(), a local time without an offset that PostgreSQL
        // then read in the session's zone: ambiguous for the repeated hour when DST ends. Bound timestamptz values
        // name their instant exactly, regardless of the default zone.
        final TimeZone original = TimeZone.getDefault();
        try {
            TimeZone.setDefault(TimeZone.getTimeZone("America/New_York"));
            final Instant ambiguous = Instant.parse("2024-11-03T05:30:00Z"); // 01:30 EDT, repeated an hour later as EST
            FetchQuery q = storage.buildFetchQuery("gauge", Aggregation.NONE, ambiguous, ambiguous.plusSeconds(3600), 60, KEYID);
            OffsetDateTime bound = (OffsetDateTime) q.params().get(0);
            assertEquals(ZoneOffset.UTC, bound.getOffset());
            assertEquals(ambiguous, bound.toInstant());
        } finally {
            TimeZone.setDefault(original);
        }
    }
}
