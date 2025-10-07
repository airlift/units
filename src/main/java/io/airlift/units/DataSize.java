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
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import static io.airlift.units.Preconditions.checkArgument;
import static java.lang.Math.floor;
import static java.lang.Math.multiplyExact;
import static java.lang.String.format;
import static java.util.Objects.requireNonNull;

public final class DataSize
        implements Comparable<DataSize>
{
    public static final DataSize ZERO = new DataSize(0, Unit.BYTE);

    private static final Pattern DECIMAL_WITH_UNIT_PATTERN = Pattern.compile("^\\s*(\\d+(?:\\.\\d+)?)\\s*([a-zA-Z]+)\\s*$");

    // We iterate over the DATASIZE_UNITS constant in convertToMostSuccinctDataSize()
    // instead of Unit.values() as the latter results in non-trivial amount of memory
    // allocation when that method is called in a tight loop. The reason is that the values()
    // call allocates a new array at each call.
    private static final Unit[] DATASIZE_UNITS = Unit.values();

    /**
     * Creates a {@link DataSize} instance with the provided quantity of the provided {@link Unit}. This
     * value is immediately converted to bytes which might overflow.
     *
     * @param size The quantity of the supplied unit
     * @param unit The unit to use as the default unit for the constructed instance and to convert the size to bytes
     * @throws IllegalArgumentException If the provided size would overflow a long value when converting to bytes
     */
    public static DataSize of(long size, Unit unit)
            throws IllegalArgumentException
    {
        if (size == 0) {
            return ZERO;
        }
        requireNonNull(unit, "unit is null");
        checkArgument(size >= 0, "size is negative: %s", size);
        if (unit == Unit.BYTE) {
            return new DataSize(size, unit);
        }
        try {
            return new DataSize(multiplyExact(size, unit.inBytes()), unit);
        }
        catch (ArithmeticException e) {
            throw new IllegalArgumentException(format("size is too large to be represented in bytes: %s%s", size, unit.getUnitString()));
        }
    }

    public static DataSize ofBytes(long bytes)
    {
        if (bytes == 0) {
            return ZERO;
        }
        return new DataSize(bytes, Unit.BYTE);
    }

    /**
     * Prefer {@link DataSize#ofBytes(long)} when conversion to the most 'succinct' unit is not necessary or desirable
     */
    public static DataSize succinctBytes(long bytes)
    {
        if (bytes == 0) {
            return ZERO;
        }
        return new DataSize(bytes, succinctUnit(bytes));
    }

    /**
     * Prefer {@link DataSize#of(long, Unit)} when conversion to the most 'succinct' unit is not necessary or desirable.
     * Otherwise, use {@link DataSize#succinctBytes(long)} since it will not incur rounding and loss of precision.
     *
     * @deprecated use {@link DataSize#succinctBytes(long)} instead, double values are imprecise
     */
    @Deprecated
    public static DataSize succinctDataSize(double size, Unit unit)
    {
        if (size == 0.0) {
            return ZERO;
        }
        long roundedSize = roundDoubleSizeInUnitToLongBytes(size, unit);
        return new DataSize(roundedSize, succinctUnit(roundedSize));
    }

    private final long bytes;
    private final Unit unit;

    /**
     * Private constructor to avoid confusing usage sites with having to pass a number of bytes
     * alongside non-bytes unit
     *
     * @param bytes The number of bytes, regardless of unit
     * @param unit The preferred display unit of this value
     */
    private DataSize(long bytes, Unit unit)
    {
        this.unit = requireNonNull(unit, "unit is null");
        checkArgument(bytes >= 0, "bytes is negative");
        this.bytes = bytes;
    }

    /**
     * @deprecated Use {@link DataSize#of(long, Unit)} instead. The imprecise nature of using doubles for DataSize is deprecated for removal
     */
    @Deprecated
    public DataSize(double size, Unit unit)
    {
        this.unit = requireNonNull(unit, "unit is null");
        this.bytes = roundDoubleSizeInUnitToLongBytes(size, unit);
    }

    public long toBytes()
    {
        return bytes;
    }

    /**
     * @deprecated Use {@link DataSize#toBytes()} instead to avoid floating point precision semantics
     */
    @Deprecated
    public double getValue()
    {
        return getValue(this.unit);
    }

    public Unit getUnit()
    {
        return unit;
    }

    /**
     * @deprecated Use {@link DataSize#toBytes()} instead to avoid floating point precision semantics
     */
    @Deprecated
    public double getValue(Unit unit)
    {
        requireNonNull(unit, "unit is null");
        if (unit == Unit.BYTE) {
            return (double) bytes;
        }
        return bytes * (1.0d / unit.inBytes());
    }

    /**
     * @deprecated Use {@link DataSize#toBytes()} instead. This method uses floating point semantics to compute the
     * rounded value which can yield to unexpected loss of precision beyond the intended rounding
     */
    @Deprecated
    public long roundTo(Unit unit)
    {
        requireNonNull(unit, "unit is null");
        if (unit == Unit.BYTE) {
            return bytes;
        }
        double rounded = floor(getValue(unit) + 0.5d);
        checkArgument(rounded <= Long.MAX_VALUE,
                "size is too large to be represented in requested unit as a long: %s%s", rounded, unit.getUnitString());
        return (long) rounded;
    }

    private static Unit succinctUnit(long bytes)
    {
        Unit unitToUse = Unit.BYTE;
        for (Unit unitToTest : DATASIZE_UNITS) {
            if (unitToTest.bytes <= bytes) {
                unitToUse = unitToTest;
            }
            else {
                break;
            }
        }
        return unitToUse;
    }

    /**
     * @deprecated Use {@link DataSize#to(Unit)} instead. No conversion occurs when calling this method, only the unit
     * used for the default string representation is changed. This has no effect on the unit used during JSON serialization
     */
    @Deprecated
    public DataSize convertTo(Unit unit)
    {
        return to(unit);
    }

    public DataSize to(Unit unit)
    {
        if (unit == this.unit) {
            return this;
        }
        return new DataSize(bytes, unit);
    }

    public DataSize succinct()
    {
        return to(succinctUnit(bytes));
    }

    /**
     * @deprecated Use {@link DataSize#succinct()} instead. No conversion occurs when calling this method, only the unit
     * used for the default string representation is changed. This has no effect on the unit used during JSON serialization
     */
    @Deprecated
    public DataSize convertToMostSuccinctDataSize()
    {
        return succinct();
    }

    @JsonValue
    public String toBytesValueString()
    {
        return bytes + Unit.BYTE.getUnitString();
    }

    @Override
    public String toString()
    {
        // Fast-path for exact byte values
        if (this.unit == Unit.BYTE) {
            return toBytesValueString();
        }
        double unitValue = getValue();
        //noinspection FloatingPointEquality
        if (floor(unitValue) == unitValue) {
            return ((long) unitValue) + unit.getUnitString();
        }
        return format(Locale.ENGLISH, "%.2f%s", unitValue, unit.getUnitString());
    }

    @JsonCreator
    public static DataSize valueOf(String size)
            throws IllegalArgumentException
    {
        requireNonNull(size, "size is null");
        checkArgument(!size.isEmpty(), "size is empty");

        // Attempt fast path parsing of JSON values without regex validation
        int stringLength = size.length();
        if (stringLength > 1 && stringLength <= 20 && size.charAt(0) != '+' && size.charAt(stringLength - 1) == 'B') {
            // must have at least 1 numeric char, less than Long.MAX_VALUE numeric chars, not start with a sign indicator, and be in unit BYTES
            try {
                return DataSize.ofBytes(Long.parseLong(size, 0, stringLength - 1, 10));
            }
            catch (Exception ignored) {
                // Ignored, slow path will either handle or produce the appropriate error from here
            }
        }

        Matcher longOrDouble = DECIMAL_WITH_UNIT_PATTERN.matcher(size);
        if (!longOrDouble.matches()) {
            throw new IllegalArgumentException("size is not a valid data size string: " + size);
        }
        Unit unit = Unit.fromUnitString(longOrDouble.group(2));
        String number = longOrDouble.group(1);
        if (number.indexOf('.') == -1) {
            // Strings without decimals can avoid precision loss by parsing as long
            return DataSize.of(Long.parseLong(number), unit);
        }
        return new DataSize(roundDoubleSizeInUnitToLongBytes(Double.parseDouble(number), unit), unit);
    }

    private static long roundDoubleSizeInUnitToLongBytes(double size, Unit unit)
    {
        checkArgument(!Double.isInfinite(size), "size is infinite");
        checkArgument(!Double.isNaN(size), "size is not a number");
        checkArgument(size >= 0, "size is negative: %s", size);
        requireNonNull(unit, "unit is null");
        double rounded = floor((size / (1.0d / unit.inBytes())) + 0.5d);
        checkArgument(rounded <= Long.MAX_VALUE,
                "size is too large to be represented in requested unit as a long: %s%s", size, unit.getUnitString());
        return (long) rounded;
    }

    @Override
    public int compareTo(DataSize o)
    {
        return Long.compare(bytes, o.bytes);
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
        return bytes == ((DataSize) o).bytes;
    }

    @Override
    public int hashCode()
    {
        return Long.hashCode(bytes);
    }

    public enum Unit
    {
        //This order is important, it should be in increasing magnitude.
        BYTE(1L, "B"),
        KILOBYTE(1L << 10, "kB"),
        MEGABYTE(1L << 20, "MB"),
        GIGABYTE(1L << 30, "GB"),
        TERABYTE(1L << 40, "TB"),
        PETABYTE(1L << 50, "PB"),
        EXABYTE(1L << 60, "EB");

        private final long bytes;
        private final String unitString;

        Unit(long bytes, String unitString)
        {
            this.bytes = bytes;
            this.unitString = unitString;
        }

        public long inBytes()
        {
            return bytes;
        }

        public String getUnitString()
        {
            return unitString;
        }

        private static Unit fromUnitString(String unitString)
        {
            for (Unit unit : DATASIZE_UNITS) {
                if (unit.unitString.equals(unitString)) {
                    return unit;
                }
            }
            throw new IllegalArgumentException("Unknown unit: " + unitString);
        }
    }
}
