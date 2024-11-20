/*
 * Copyright 2010 Proofpoint, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.airlift.units;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonValue;

import java.util.Locale;
import java.util.concurrent.TimeUnit;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import static io.airlift.units.Preconditions.checkArgument;
import static java.lang.Double.isFinite;
import static java.lang.Math.floor;
import static java.lang.String.format;
import static java.util.Objects.requireNonNull;
import static java.util.concurrent.TimeUnit.DAYS;
import static java.util.concurrent.TimeUnit.HOURS;
import static java.util.concurrent.TimeUnit.MICROSECONDS;
import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static java.util.concurrent.TimeUnit.MINUTES;
import static java.util.concurrent.TimeUnit.NANOSECONDS;
import static java.util.concurrent.TimeUnit.SECONDS;

public final class Duration
        implements Comparable<Duration>
{
    private static final Pattern PATTERN = Pattern.compile("^\\s*(\\d+(?:\\.\\d+)?)\\s*([a-zA-Z]+)\\s*$");

    // We iterate over the TIME_UNITS constant in convertToMostSuccinctTimeUnit()
    // instead of TimeUnit.values() as the latter results in non-trivial amount of memory
    // allocation when that method is called in a tight loop. The reason is that the values()
    // call allocates a new array at each call.
    private static final TimeUnit[] TIME_UNITS = TimeUnit.values();

    public static final Duration ZERO = new Duration(0, SECONDS);

    public static Duration nanosSince(long start)
    {
        return succinctNanos(System.nanoTime() - start);
    }

    public static Duration succinctNanos(long nanos)
    {
        return succinctDuration(nanos, NANOSECONDS);
    }

    public static Duration succinctDuration(double value, TimeUnit unit)
    {
        if (value == 0) {
            return ZERO;
        }
        return new Duration(value, unit).convertToMostSuccinctTimeUnit();
    }

    private final double value;
    private final TimeUnit unit;

    public Duration(double value, TimeUnit unit)
    {
        if (Double.isInfinite(value)) {
            throw new IllegalArgumentException("value is infinite: " + value);
        }
        checkArgument(!Double.isNaN(value), "value is not a number");
        if (value < 0) {
            throw new IllegalArgumentException("value is negative: " + value);
        }
        requireNonNull(unit, "unit is null");

        this.value = value;
        this.unit = unit;
    }

    public long toMillis()
    {
        return roundTo(MILLISECONDS);
    }

    public double getValue()
    {
        return value;
    }

    public TimeUnit getUnit()
    {
        return unit;
    }

    public double getValue(TimeUnit timeUnit)
    {
        requireNonNull(timeUnit, "timeUnit is null");
        switch (unit) {
            case DAYS:
                switch (timeUnit) {
                    case DAYS:
                        return value;
                    case HOURS:
                        return value * 24;
                    case MINUTES:
                        return value * (24 * 60);
                    case SECONDS:
                        return value * (24 * 60 * 60);
                    case MILLISECONDS:
                        return value * (24 * 60 * 60 * 1000);
                    case MICROSECONDS:
                        return value * (24 * 60 * 60 * 1000 * 1000L);
                    case NANOSECONDS:
                        return value * (24 * 60 * 60 * 1000 * 1000L * 1000);
                    default:
                        throw new IllegalArgumentException("Unsupported time unit " + timeUnit);
                }

            case HOURS:
                switch (timeUnit) {
                    case DAYS:
                        return value / 24;
                    case HOURS:
                        return value;
                    case MINUTES:
                        return value * 60;
                    case SECONDS:
                        return value * (60 * 60);
                    case MILLISECONDS:
                        return value * (60 * 60 * 1000);
                    case MICROSECONDS:
                        return value * (60 * 60 * 1000 * 1000L);
                    case NANOSECONDS:
                        return value * (60 * 60 * 1000 * 1000L * 1000);
                    default:
                        throw new IllegalArgumentException("Unsupported time unit " + timeUnit);
                }

            case MINUTES:
                switch (timeUnit) {
                    case DAYS:
                        return value / (24 * 60);
                    case HOURS:
                        return value / 60;
                    case MINUTES:
                        return value;
                    case SECONDS:
                        return value * 60;
                    case MILLISECONDS:
                        return value * (60 * 1000);
                    case MICROSECONDS:
                        return value * (60 * 1000 * 1000);
                    case NANOSECONDS:
                        return value * (60 * 1000 * 1000 * 1000L);
                    default:
                        throw new IllegalArgumentException("Unsupported time unit " + timeUnit);
                }

            case SECONDS:
                switch (timeUnit) {
                    case DAYS:
                        return value / (24 * 60 * 60);
                    case HOURS:
                        return value / (60 * 60);
                    case MINUTES:
                        return value / 60;
                    case SECONDS:
                        return value;
                    case MILLISECONDS:
                        return value * (1000);
                    case MICROSECONDS:
                        return value * (1000 * 1000);
                    case NANOSECONDS:
                        return value * (1000 * 1000 * 1000);
                    default:
                        throw new IllegalArgumentException("Unsupported time unit " + timeUnit);
                }

            case MILLISECONDS:
                switch (timeUnit) {
                    case DAYS:
                        return value / (24 * 60 * 60 * 1000);
                    case HOURS:
                        return value / (60 * 60 * 1000);
                    case MINUTES:
                        return value / (60 * 1000);
                    case SECONDS:
                        return value / 1000;
                    case MILLISECONDS:
                        return value;
                    case MICROSECONDS:
                        return value * 1000;
                    case NANOSECONDS:
                        return value * (1000 * 1000);
                    default:
                        throw new IllegalArgumentException("Unsupported time unit " + timeUnit);
                }

            case MICROSECONDS:
                switch (timeUnit) {
                    case DAYS:
                        return value / (24 * 60 * 60 * 1000 * 1000L);
                    case HOURS:
                        return value / (60 * 60 * 1000 * 1000L);
                    case MINUTES:
                        return value / (60 * 1000 * 1000);
                    case SECONDS:
                        return value / (1000 * 1000);
                    case MILLISECONDS:
                        return value / 1000;
                    case MICROSECONDS:
                        return value;
                    case NANOSECONDS:
                        return value * 1000;
                    default:
                        throw new IllegalArgumentException("Unsupported time unit " + timeUnit);
                }

            case NANOSECONDS:
                switch (timeUnit) {
                    case DAYS:
                        return value / (24 * 60 * 60 * 1000 * 1000L * 1000);
                    case HOURS:
                        return value / (60 * 60 * 1000 * 1000L * 1000);
                    case MINUTES:
                        return value / (60 * 1000 * 1000 * 1000L);
                    case SECONDS:
                        return value / (1000 * 1000 * 1000);
                    case MILLISECONDS:
                        return value / (1000 * 1000);
                    case MICROSECONDS:
                        return value / 1000;
                    case NANOSECONDS:
                        return value;
                    default:
                        throw new IllegalArgumentException("Unsupported time unit " + timeUnit);
                }

            default:
                throw new IllegalArgumentException("Unsupported time unit " + unit);
        }
    }

    public long roundTo(TimeUnit timeUnit)
    {
        requireNonNull(timeUnit, "timeUnit is null");
        double rounded = Math.floor(getValue(timeUnit) + 0.5d);
        if (rounded > Long.MAX_VALUE) {
            throw new IllegalArgumentException(format("value %s %s is too large to be represented in requested unit %s as a long", value, unit, timeUnit));
        }
        return (long) rounded;
    }

    public Duration convertTo(TimeUnit timeUnit)
    {
        requireNonNull(timeUnit, "timeUnit is null");
        return new Duration(getValue(timeUnit), timeUnit);
    }

    public Duration convertToMostSuccinctTimeUnit()
    {
        TimeUnit unitToUse = NANOSECONDS;
        for (TimeUnit unitToTest : TIME_UNITS) {
            // since time units are powers of ten, we can get rounding errors here, so fuzzy match
            if (getValue(unitToTest) > 0.9999) {
                unitToUse = unitToTest;
            }
            else {
                break;
            }
        }
        return convertTo(unitToUse);
    }

    public java.time.Duration toJavaTime()
    {
        long seconds;
        long nanoAdjustment;
        long secondsPerUnit = SECONDS.convert(1, unit);
        long nanosPerUnit = NANOSECONDS.convert(1, unit);
        if (secondsPerUnit > 1) {
            seconds = (long) floor(value * secondsPerUnit);
            nanoAdjustment = (long) floor((value - (double) seconds / secondsPerUnit) * nanosPerUnit);
        }
        else {
            long unitsPerSecond = unit.convert(1, SECONDS);
            seconds = (long) floor(value / unitsPerSecond);
            nanoAdjustment = (long) floor((value - (double) seconds * unitsPerSecond) * nanosPerUnit);
        }

        if (seconds == Long.MAX_VALUE) {
            nanoAdjustment = 0;
        }
        return java.time.Duration.ofSeconds(seconds, nanoAdjustment);
    }

    @JsonValue
    @Override
    public String toString()
    {
        return toString(unit);
    }

    public String toString(TimeUnit timeUnit)
    {
        requireNonNull(timeUnit, "timeUnit is null");
        double magnitude = getValue(timeUnit);
        String timeUnitAbbreviation = timeUnitToString(timeUnit);
        return format(Locale.ENGLISH, "%.2f%s", magnitude, timeUnitAbbreviation);
    }

    @JsonCreator
    public static Duration valueOf(String duration)
            throws IllegalArgumentException
    {
        requireNonNull(duration, "duration is null");
        checkArgument(!duration.isEmpty(), "duration is empty");

        Matcher matcher = PATTERN.matcher(duration);
        if (!matcher.matches()) {
            throw new IllegalArgumentException("duration is not a valid data duration string: " + duration);
        }

        double value = Double.parseDouble(matcher.group(1));
        String unitString = matcher.group(2);

        TimeUnit timeUnit = valueOfTimeUnit(unitString);
        return new Duration(value, timeUnit);
    }

    @Override
    public int compareTo(Duration that)
    {
        double thisValueMillis = this.getValue(MILLISECONDS);
        double thatValueMillis = that.getValue(MILLISECONDS);
        int compare = Double.compare(thisValueMillis, thatValueMillis);
        if (compare == 0) {
            double thisValueNanos = this.getValue(NANOSECONDS);
            double thatValueNanos = that.getValue(NANOSECONDS);
            if (isFinite(thisValueNanos) && isFinite(thatValueNanos)) {
                compare = Double.compare(thisValueNanos, thatValueNanos);
            }
        }
        return compare;
    }

    public boolean isZero()
    {
        return equals(ZERO);
    }

    @Override
    public boolean equals(Object o)
    {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }

        Duration duration = (Duration) o;

        return compareTo(duration) == 0;
    }

    @Override
    public int hashCode()
    {
        double value = getValue(MILLISECONDS);
        return Double.hashCode(value);
    }

    public static TimeUnit valueOfTimeUnit(String timeUnitString)
    {
        requireNonNull(timeUnitString, "timeUnitString is null");
        switch (timeUnitString) {
            case "ns":
                return NANOSECONDS;
            case "us":
                return MICROSECONDS;
            case "ms":
                return MILLISECONDS;
            case "s":
                return SECONDS;
            case "m":
                return MINUTES;
            case "h":
                return HOURS;
            case "d":
                return DAYS;
            default:
                throw new IllegalArgumentException("Unknown time unit: " + timeUnitString);
        }
    }

    public static String timeUnitToString(TimeUnit timeUnit)
    {
        requireNonNull(timeUnit, "timeUnit is null");
        switch (timeUnit) {
            case NANOSECONDS:
                return "ns";
            case MICROSECONDS:
                return "us";
            case MILLISECONDS:
                return "ms";
            case SECONDS:
                return "s";
            case MINUTES:
                return "m";
            case HOURS:
                return "h";
            case DAYS:
                return "d";
            default:
                throw new IllegalArgumentException("Unsupported time unit " + timeUnit);
        }
    }
}
