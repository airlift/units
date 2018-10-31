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

import java.util.Locale;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import static io.airlift.units.Preconditions.checkArgument;
import static java.lang.Long.parseLong;
import static java.lang.Math.floor;
import static java.lang.String.format;
import static java.util.Objects.requireNonNull;

public class Quantity
        implements Comparable<Quantity>
{
    private static final Pattern PATTERN = Pattern.compile("^\\s*(\\d+)\\s*([a-zA-Z]?)\\s*$");

    // We iterate over the QUANTITY_UNITS constant in convertToMostSuccinctDataSize()
    // instead of Unit.values() as the latter results in non-trivial amount of memory
    // allocation when that method is called in a tight loop. The reason is that the values()
    // call allocates a new array at each call.
    private static final Magnitude[] QUANTITY_MAGNITUDES = Magnitude.values();

    /**
     * @return quantity with no bigger value than 1000 in succinct unit, fractional part is rounded
     */
    public static Quantity succinctRounded(long quantity)
    {
        return succinctRounded(quantity, Magnitude.SINGLE);
    }

    /**
     * @return quantity with no bigger value than 1000 in succinct unit, fractional part is rounded
     */
    public static Quantity succinctRounded(long quantity, Magnitude magnitude)
    {
        return new Quantity(quantity, magnitude).convertToMostSuccinctRounded();
    }

    private final long value;
    private final Magnitude magnitude;

    public Quantity(long quantity, Magnitude magnitude)
    {
        checkArgument(!Double.isInfinite(quantity), "quantity is infinite");
        checkArgument(!Double.isNaN(quantity), "quantity is not a number");
        checkArgument(quantity >= 0, "quantity is negative");
        requireNonNull(magnitude, "unit is null");

        this.value = quantity;
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
        if (value == 0L) {
            return 0L;
        }

        long scale = this.magnitude.getFactor() / magnitude.getFactor();
        if (scale == 0) {
            throw new IllegalArgumentException(format("Unable to return value %s in unit %s due precision loss", this, magnitude));
        }
        return value * scale;
    }

    public Quantity convertTo(Magnitude magnitude)
    {
        requireNonNull(magnitude, "unit is null");
        return new Quantity(getValue(magnitude), magnitude);
    }

    /**
     * @return converted quantity with no bigger value than 1000 in succinct unit, fractional part is rounded
     */
    public Quantity convertToMostSuccinctRounded()
    {
        for (Magnitude magnitude : QUANTITY_MAGNITUDES) {
            double converted = (double) value * this.magnitude.getFactor() / magnitude.getFactor();
            if (converted < 1000) {
                return new Quantity(Math.round(converted), magnitude);
            }
        }
        throw new IllegalStateException();
    }

    @JsonValue
    @Override
    public String toString()
    {
        //noinspection FloatingPointEquality
        if (floor(value) == value) {
            return (long) (floor(value)) + magnitude.getUnitString();
        }

        return format(Locale.ENGLISH, "%.2f%s", value, magnitude.getUnitString());
    }

    @JsonCreator
    public static Quantity valueOf(String quantity)
            throws IllegalArgumentException
    {
        requireNonNull(quantity, "quantity is null");
        checkArgument(!quantity.isEmpty(), "quantity is empty");

        Matcher matcher = PATTERN.matcher(quantity);
        if (!matcher.matches()) {
            throw new IllegalArgumentException("Not a valid quantity string: " + quantity);
        }

        long value = parseLong(matcher.group(1));
        String unitString = matcher.group(2);

        for (Magnitude magnitude : Magnitude.values()) {
            if (magnitude.getUnitString().equals(unitString)) {
                return new Quantity(value, magnitude);
            }
        }

        throw new IllegalArgumentException("Unknown unit: " + unitString);
    }

    @Override
    public int compareTo(Quantity quantity)
    {
        requireNonNull(quantity, "quantity is null");
        return Double.compare(getValue(Magnitude.SINGLE), quantity.getValue(Magnitude.SINGLE));
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

        Quantity quantity = (Quantity) o;

        return compareTo(quantity) == 0;
    }

    @Override
    public int hashCode()
    {
        return Double.hashCode(getValue(Magnitude.SINGLE));
    }

    public enum Magnitude
    {
        //This order is important, it should be in increasing magnitude.
        SINGLE(1L, ""),
        THOUSAND(1000L, "K"),
        MILLION(1000_000L, "M"),
        BILLION(1000_000_000L, "B"),
        TRILION(1000_000_000_000L, "T"),
        QUADRILLION(1000_000_000_000_000L, "P");

        private final long factor;
        private final String unitString;

        Magnitude(long factor, String unitString)
        {
            this.factor = factor;
            this.unitString = unitString;
        }

        long getFactor()
        {
            return factor;
        }

        public String getUnitString()
        {
            return unitString;
        }
    }
}
