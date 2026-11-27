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

import static java.lang.Long.numberOfLeadingZeros;
import static java.lang.Math.floor;
import static java.lang.Math.multiplyExact;
import static java.lang.Math.round;
import static java.lang.String.format;
import static java.util.Objects.requireNonNull;

public final class DataSize
        implements Comparable<DataSize>
{
    public static final DataSize ZERO = new DataSize(0, Unit.BYTE);

    private static final double[] POWERS_OF_TEN = {1e0, 1e1, 1e2, 1e3, 1e4, 1e5, 1e6, 1e7, 1e8, 1e9, 1e10, 1e11, 1e12, 1e13, 1e14};

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
        if (!(size >= 0)) {
            throw new IllegalArgumentException(format("size is negative: %s", size));
        }
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
        if (!(bytes >= 0)) {
            throw new IllegalArgumentException("bytes is negative");
        }
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
        if (!(rounded <= Long.MAX_VALUE)) {
            throw new IllegalArgumentException(format("size is too large to be represented in requested unit as a long: %s%s", rounded, unit.getUnitString()));
        }
        return (long) rounded;
    }

    private static Unit succinctUnit(long bytes)
    {
        if (bytes == 0) {
            return Unit.BYTE;
        }
        int index = (63 - numberOfLeadingZeros(bytes)) / 10;
        return DATASIZE_UNITS[index];
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
        return formatValue(unitValue) + unit.getUnitString();
    }

    @JsonCreator
    public static DataSize valueOf(String size)
            throws IllegalArgumentException
    {
        requireNonNull(size, "size is null");
        boolean expression = !size.isEmpty();
        if (!expression) {
            throw new IllegalArgumentException("size is empty");
        }

        // Allocation-free equivalent of matching ^\s*(\d+(?:\.\d+)?)\s*([a-zA-Z]+)\s*$
        int numberStart = skipWhitespace(size, 0);
        int numberEnd = skipNumber(size, numberStart);
        int unitStart = skipWhitespace(size, numberEnd);
        int unitEnd = skipLetters(size, unitStart);
        if (numberEnd == numberStart || unitEnd == unitStart || skipWhitespace(size, unitEnd) != size.length()) {
            return parseInvalid(size);
        }
        Unit unit = Unit.fromUnitString(size, unitStart, unitEnd);
        int dot = size.indexOf('.', numberStart);
        if (dot == -1 || dot >= numberEnd) {
            // Strings without decimals can avoid precision loss by parsing as long
            return DataSize.of(parseLong(size, numberStart, numberEnd), unit);
        }
        return new DataSize(roundDoubleSizeInUnitToLongBytes(parseDecimal(size, numberStart, numberEnd), unit), unit);
    }

    // The former Long.parseLong fast path also accepted byte counts such as "-0B" or non-ASCII digits
    private static DataSize parseInvalid(String size)
    {
        int length = size.length();
        if (length > 1 && length <= 20 && size.charAt(0) != '+' && size.charAt(length - 1) == 'B') {
            try {
                return ofBytes(Long.parseLong(size, 0, length - 1, 10));
            }
            catch (RuntimeException ignored) {
                // not a valid byte count either
            }
        }
        throw new IllegalArgumentException("size is not a valid data size string: " + size);
    }

    // Same character set as the regex \s
    private static int skipWhitespace(String value, int index)
    {
        while (index < value.length()) {
            char c = value.charAt(index);
            if (c != ' ' && (c < '\t' || c > '\r')) {
                break;
            }
            index++;
        }
        return index;
    }

    private static int skipLetters(String value, int index)
    {
        while (index < value.length()) {
            char c = value.charAt(index);
            if ((c < 'a' || c > 'z') && (c < 'A' || c > 'Z')) {
                break;
            }
            index++;
        }
        return index;
    }

    private static int skipDigits(String value, int index)
    {
        while (index < value.length()) {
            char c = value.charAt(index);
            if (c < '0' || c > '9') {
                break;
            }
            index++;
        }
        return index;
    }

    // Returns the end of \d+(?:\.\d+)? starting at index, or index if there is no match
    private static int skipNumber(String value, int index)
    {
        int end = skipDigits(value, index);
        if (end == index || end == value.length() || value.charAt(end) != '.') {
            return end;
        }
        int fractionEnd = skipDigits(value, end + 1);
        return fractionEnd == end + 1 ? end : fractionEnd;
    }

    private static long parseLong(String digits, int start, int end)
    {
        // At most 18 digits cannot overflow
        if (end - start > 18) {
            return Long.parseLong(digits.substring(start, end));
        }
        long result = 0;
        for (int i = start; i < end; i++) {
            result = result * 10 + (digits.charAt(i) - '0');
        }
        return result;
    }

    // Same result as Double.parseDouble for a match of skipNumber
    private static double parseDecimal(String number, int start, int end)
    {
        // At most 15 digits: mantissa and power of ten are exact doubles, so the single division is correctly rounded
        if (end - start <= 15) {
            long mantissa = 0;
            int scale = 0;
            boolean fraction = false;
            for (int i = start; i < end; i++) {
                char c = number.charAt(i);
                if (c == '.') {
                    fraction = true;
                    continue;
                }
                mantissa = mantissa * 10 + (c - '0');
                if (fraction) {
                    scale++;
                }
            }
            return mantissa / POWERS_OF_TEN[scale];
        }
        return Double.parseDouble(number.substring(start, end));
    }

    private static long roundDoubleSizeInUnitToLongBytes(double size, Unit unit)
    {
        boolean expression1 = !Double.isInfinite(size);
        if (!expression1) {
            throw new IllegalArgumentException("size is infinite");
        }
        boolean expression = !Double.isNaN(size);
        if (!expression) {
            throw new IllegalArgumentException("size is not a number");
        }
        if (!(size >= 0)) {
            throw new IllegalArgumentException(format("size is negative: %s", size));
        }
        requireNonNull(unit, "unit is null");
        double rounded = floor((size / (1.0d / unit.inBytes())) + 0.5d);
        if (!(rounded <= Long.MAX_VALUE)) {
            throw new IllegalArgumentException(format("size is too large to be represented in requested unit as a long: %s%s", size, unit.getUnitString()));
        }
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

        private static Unit fromUnitString(String value, int start, int end)
        {
            int length = end - start;
            if (length == 1 && value.charAt(start) == 'B') {
                return BYTE;
            }
            if (length == 2 && value.charAt(start + 1) == 'B') {
                Unit unit = switch (value.charAt(start)) {
                    case 'k' -> KILOBYTE;
                    case 'M' -> MEGABYTE;
                    case 'G' -> GIGABYTE;
                    case 'T' -> TERABYTE;
                    case 'P' -> PETABYTE;
                    case 'E' -> EXABYTE;
                    default -> null;
                };
                if (unit != null) {
                    return unit;
                }
            }
            throw new IllegalArgumentException("Unknown unit: " + value.substring(start, end));
        }
    }

    private static String formatValue(double value)
    {
        long scaled = round(value * 100);
        long integerPart = scaled / 100;
        long fractionalPart = scaled % 100;

        return integerPart + "." + (fractionalPart < 10 ? "0" : "") + fractionalPart;
    }
}
