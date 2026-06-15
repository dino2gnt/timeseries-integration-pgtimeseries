package org.opennms.timeseries.impl.pgtimeseries;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.mock;

import java.util.Arrays;
import java.util.Collections;

import javax.sql.DataSource;

import org.junit.Before;
import org.junit.Test;
import org.opennms.integration.api.v1.timeseries.TagMatcher;
import org.opennms.integration.api.v1.timeseries.immutables.ImmutableTagMatcher;
import org.opennms.timeseries.impl.pgtimeseries.config.PGTimeseriesConfig;

/**
 * Unit tests for {@link PGTimeseriesStorage#createMetricsSQL}. This is pure SQL-string generation and needs
 * no database, so it validates both the operator mapping and the parameterization (injection) fix directly.
 */
public class CreateMetricsSQLTest {

    private PGTimeseriesStorage storage;

    @Before
    public void setUp() {
        storage = new PGTimeseriesStorage(PGTimeseriesConfig.builder().build(), mock(DataSource.class));
    }

    private static TagMatcher matcher(final TagMatcher.Type type, final String key, final String value) {
        return new ImmutableTagMatcher(type, key, value);
    }

    @Test
    public void singleEqualsMatcherProducesOneParameterizedTerm() {
        String sql = storage.createMetricsSQL(
                Collections.singletonList(matcher(TagMatcher.Type.EQUALS, "name", "ifInOctets")));
        assertEquals("SELECT keyid FROM pgtimeseries_tag WHERE key = ? AND value = ?", sql);
        // exactly two bind placeholders (key, value); no INTERSECT for a single matcher
        assertEquals(2, countPlaceholders(sql));
        assertFalse(sql.contains("INTERSECT"));
    }

    @Test
    public void multipleMatchersAreCombinedWithIntersect() {
        String sql = storage.createMetricsSQL(Arrays.asList(
                matcher(TagMatcher.Type.EQUALS, "name", "ifInOctets"),
                matcher(TagMatcher.Type.EQUALS, "resourceId", "node[1]")));
        assertEquals(
                "SELECT keyid FROM pgtimeseries_tag WHERE key = ? AND value = ?"
                        + " INTERSECT "
                        + "SELECT keyid FROM pgtimeseries_tag WHERE key = ? AND value = ?",
                sql);
        // one INTERSECT for two matchers, four placeholders total
        assertEquals(1, countOccurrences(sql, "INTERSECT"));
        assertEquals(4, countPlaceholders(sql));
    }

    @Test
    public void operatorsAreMappedForEachMatcherType() {
        assertTrue(sqlFor(TagMatcher.Type.EQUALS).endsWith("value = ?"));
        assertTrue(sqlFor(TagMatcher.Type.NOT_EQUALS).endsWith("value != ?"));
        assertTrue(sqlFor(TagMatcher.Type.EQUALS_REGEX).endsWith("value ~ ?"));
        assertTrue(sqlFor(TagMatcher.Type.NOT_EQUALS_REGEX).endsWith("value !~ ?"));
    }

    @Test
    public void valuesAreNeverInlinedSoInjectionIsNotPossible() {
        String malicious = "x'; DROP TABLE pgtimeseries_time_series; --";
        String sql = storage.createMetricsSQL(
                Collections.singletonList(matcher(TagMatcher.Type.EQUALS, "name", malicious)));
        // the value (and its DROP TABLE payload) must be carried as a bind parameter, not concatenated
        assertFalse(sql.contains("DROP TABLE"));
        assertFalse(sql.contains(malicious));
        assertEquals(2, countPlaceholders(sql));
    }

    private String sqlFor(final TagMatcher.Type type) {
        return storage.createMetricsSQL(Collections.singletonList(matcher(type, "k", "v")));
    }

    private static int countPlaceholders(final String sql) {
        return countOccurrences(sql, "?");
    }

    private static int countOccurrences(final String haystack, final String needle) {
        int count = 0;
        int idx = 0;
        while ((idx = haystack.indexOf(needle, idx)) >= 0) {
            count++;
            idx += needle.length();
        }
        return count;
    }
}
