package org.opennms.timeseries.impl.pgtimeseries.shell;

import java.time.Instant;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;

/** Small formatting helpers shared by the shell commands. */
final class ShellSupport {

    private static final DateTimeFormatter LOCAL = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss z")
            .withZone(ZoneId.systemDefault());

    private ShellSupport() {
    }

    /** Formats a byte count the way PostgreSQL's pg_size_pretty() does. */
    static String prettyBytes(final long bytes) {
        final String[] units = {"bytes", "kB", "MB", "GB", "TB", "PB"};
        double value = bytes;
        int unit = 0;
        // pg_size_pretty switches units once the value reaches 10 x 1024 of the current unit
        while (unit < units.length - 1 && Math.abs(value) >= 10 * 1024) {
            value /= 1024;
            unit++;
        }
        return unit == 0 ? bytes + " " + units[0] : Math.round(value) + " " + units[unit];
    }

    /** Renders an instant in the local timezone, or a dash for null. */
    static String time(final Instant t) {
        return t == null ? "-" : LOCAL.format(t);
    }

    static String nullToDash(final String s) {
        return s == null ? "-" : s;
    }
}
