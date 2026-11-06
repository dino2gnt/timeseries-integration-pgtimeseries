package org.opennms.timeseries.impl.pgtimeseries.util;

import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.time.ZoneId;
import java.time.ZoneOffset;
import java.util.concurrent.atomic.AtomicReference;

/** A clock the tests can move by hand so partition maintenance can be exercised without waiting for real time. */
public final class MutableClock extends Clock {

    private final AtomicReference<Instant> now;
    private final ZoneId zone;

    public MutableClock(final Instant start) {
        this(start, ZoneOffset.UTC);
    }

    private MutableClock(final Instant start, final ZoneId zone) {
        this.now = new AtomicReference<>(start);
        this.zone = zone;
    }

    public void set(final Instant instant) {
        now.set(instant);
    }

    public Instant plus(final Duration d) {
        return now.updateAndGet(i -> i.plus(d));
    }

    @Override
    public ZoneId getZone() {
        return zone;
    }

    @Override
    public Clock withZone(final ZoneId zone) {
        return new MutableClock(now.get(), zone);
    }

    @Override
    public Instant instant() {
        return now.get();
    }
}
