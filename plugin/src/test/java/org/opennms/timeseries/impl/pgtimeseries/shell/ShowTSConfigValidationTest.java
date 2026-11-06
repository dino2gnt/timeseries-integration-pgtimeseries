package org.opennms.timeseries.impl.pgtimeseries.shell;

import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

import java.sql.Connection;
import java.sql.DriverManager;

import org.junit.AfterClass;
import org.junit.Assume;
import org.junit.BeforeClass;
import org.junit.Test;
import org.opennms.timeseries.impl.pgtimeseries.util.ContainerizedDatabase;

/** The interval checks ts-config applies before touching the policy row. */
public class ShowTSConfigValidationTest {

    private static ContainerizedDatabase database;
    private static Connection connection;

    @BeforeClass
    public static void setUp() throws Exception {
        Assume.assumeTrue(ContainerizedDatabase.isRuntimeAvailable());
        try {
            database = ContainerizedDatabase.start();
        } catch (RuntimeException e) {
            Assume.assumeNoException(e);
        }
        connection = DriverManager.getConnection(database.jdbcUrl(), database.username(), database.password());
    }

    @AfterClass
    public static void tearDown() throws Exception {
        if (connection != null) {
            connection.close();
        }
        if (database != null) {
            database.close();
        }
    }

    @Test
    public void acceptsSensibleIntervals() throws Exception {
        assertNull(ShowTSConfig.validate(connection, "2 weeks", "7 days"));
        assertNull(ShowTSConfig.validate(connection, "7 days", "7 days"));
        assertNull(ShowTSConfig.validate(connection, "1 year", "1 month"));
        assertNull(ShowTSConfig.validate(connection, "1 hour", "1 minute"));
    }

    @Test
    public void rejectsBareNumbersNonPositiveAndTooShortIntervals() throws Exception {
        assertTrue(ShowTSConfig.validate(connection, "7", "7 days").contains("no unit"));
        assertTrue(ShowTSConfig.validate(connection, "-1 year", "7 days").contains("not a positive"));
        assertTrue(ShowTSConfig.validate(connection, "0", "1 minute").contains("no unit"));
        assertTrue(ShowTSConfig.validate(connection, "0 days", "1 minute").contains("not a positive"));
        assertTrue(ShowTSConfig.validate(connection, "3 days", "7 days").contains("shorter than 7 days"));
        assertTrue(ShowTSConfig.validate(connection, "soon", "7 days").contains("not a valid"));
    }
}
