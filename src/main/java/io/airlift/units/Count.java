/*
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

import java.util.regex.Matcher;
import java.util.regex.Pattern;

import static io.airlift.units.Preconditions.checkArgument;
import static java.lang.Long.parseLong;
import static java.lang.Math.multiplyExact;
import static java.lang.String.format;
import static java.util.Objects.requireNonNull;

public final class Count
        implements Comparable<Count>
{
    private static final Pattern PATTERN = Pattern.compile("^\\s*(\\d+)\\s*([a-zA-Z]?)\\s*$");

    // We iterate over the MAGNITUDES constant in convertToMostSuccinctRounded()
    // instead of Magnitude.values() as the latter results in non-trivial amount of memory
    // allocation when that method is called in a tight loop. The reason is that the values()
    // call allocates a new array at each call.
    private static final Magnitude[] MAGNITUDES = Magnitude.values();

    /**
     * @return count with no bigger value than 1000 in succinct magnitude, fractional part is rounded
     */
    public static Count succinctRounded(long count)
    {
        return succinctRounded(count, Magnitude.SINGLE);
    }

    /**
     * @return count with no bigger value than 1000 in succinct magnitude, fractional part is rounded
     */
    public static Count succinctRounded(long count, Magnitude magnitude)
    {
        return new Count(count, magnitude).convertToMostSuccinctRounded();
    }

    private final long value;
    private final Magnitude magnitude;

    public Count(long count, Magnitude magnitude)
    {
        checkArgument(count >= 0, "count is negative");
        requireNonNull(magnitude, "magnitude is null");

        this.value = count;
        this.magnitude = magnitude;
    }

    public long getValue()
    {
        return value;
    }

    public Magnitude getMagnitude()
    {
        return magnitude;
    }

    public long getValue(Magnitude magnitude)
    {
        requireNonNull(magnitude, "magnitude is null");

        if (value == 0L) {
            return 0L;
        }

        long scale = this.magnitude.getFactor() / magnitude.getFactor();
        if (scale * magnitude.getFactor() != this.magnitude.getFactor()) {
            throw new IllegalArgumentException(format("Unable to represent %s in %s, conversion would cause a precision loss", this, magnitude));
        }
        try {
            return multiplyExact(value, scale);
        }
        catch (ArithmeticException e) {
            throw new IllegalArgumentException(format("Unable to represent %s in %s due the Long value overflow", this, magnitude));
        }
    }

    public Count convertTo(Magnitude magnitude)
    {
        return new Count(getValue(magnitude), magnitude);
    }

    /**
     * @return converted count with no bigger value than 1000 in succinct magnitude, fractional part is rounded
     */
    public Count convertToMostSuccinctRounded()
    {
        for (Magnitude magnitude : MAGNITUDES) {
            double converted = (double) value * this.magnitude.getFactor() / magnitude.getFactor();
            if (converted < 1000) {
                return new Count(Math.round(converted), magnitude);
            }
        }
        throw new IllegalStateException();
    }

    @JsonValue
    @Override
    public String toString()
    {
        return value + magnitude.getMagnitudeString();
    }

    @JsonCreator
    public static Count valueOf(String count)
            throws IllegalArgumentException
    {
        requireNonNull(count, "count is null");
        checkArgument(!count.isEmpty(), "count is empty");

        Matcher matcher = PATTERN.matcher(count);
        if (!matcher.matches()) {
            throw new IllegalArgumentException("Not a valid count string: " + count);
        }

        long value = parseLong(matcher.group(1));
        String magnitutdeString = matcher.group(2);

        for (Magnitude magnitude : Magnitude.values()) {
            if (magnitude.getMagnitudeString().equals(magnitutdeString)) {
                return new Count(value, magnitude);
            }
        }

        throw new IllegalArgumentException("Unknown magnitude: " + magnitutdeString);
    }

    @Override
    public int compareTo(Count o)
    {
        return Long.compare(getValue(Magnitude.SINGLE), o.getValue(Magnitude.SINGLE));
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

        Count count = (Count) o;

        return compareTo(count) == 0;
    }

    @Override
    public int hashCode()
    {
        return Long.hashCode(getValue(Magnitude.SINGLE));
    }

    public enum Magnitude
    {
        // must be in increasing magnitude order
        SINGLE(1L, ""),
        THOUSAND(1000L, "K"),
        MILLION(1000_000L, "M"),
        BILLION(1000_000_000L, "B"),
        TRILION(1000_000_000_000L, "T"),
        QUADRILLION(1000_000_000_000_000L, "P");

        private final long factor;
        private final String magnitudeString;

        Magnitude(long factor, String magnitudeString)
        {
            this.factor = factor;
            this.magnitudeString = requireNonNull(magnitudeString, "magnitudeString is null");
        }

        long getFactor()
        {
            return factor;
        }

        public String getMagnitudeString()
        {
            return magnitudeString;
        }
    }
}
