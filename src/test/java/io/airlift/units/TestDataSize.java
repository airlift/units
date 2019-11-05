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

import com.google.common.collect.ImmutableList;
import io.airlift.json.JsonCodec;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import java.util.Locale;

import static io.airlift.testing.EquivalenceTester.comparisonTester;
import static io.airlift.units.DataSize.Unit.BYTE;
import static io.airlift.units.DataSize.Unit.GIGABYTE;
import static io.airlift.units.DataSize.Unit.KILOBYTE;
import static io.airlift.units.DataSize.Unit.MEGABYTE;
import static io.airlift.units.DataSize.Unit.PETABYTE;
import static io.airlift.units.DataSize.Unit.TERABYTE;
import static io.airlift.units.DataSize.succinctBytes;
import static io.airlift.units.DataSize.succinctDataSize;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.within;
import static org.testng.Assert.assertEquals;

public class TestDataSize
{
    @Test
    public void testSuccinctFactories()
    {
        assertEquals(succinctBytes(123), DataSize.ofBytes(123));
        assertEquals(succinctBytes(123).getUnit(), BYTE);

        DataSize fiveMiBi = DataSize.ofBytes(5 * 1024 * 1024);
        assertEquals(succinctBytes(fiveMiBi.toBytes()), fiveMiBi);
        assertEquals(succinctBytes(fiveMiBi.toBytes()).getUnit(), MEGABYTE);
    }

    @Test
    public void testDeprecatedSuccinctFactories()
    {
        assertEquals(succinctDataSize(123, BYTE), DataSize.ofBytes(123));
        assertEquals(succinctDataSize(123, BYTE).getUnit(), BYTE);

        assertEquals(succinctDataSize((long) (5.5 * 1024), BYTE), new DataSize(5.5, KILOBYTE));
        assertEquals(succinctDataSize((long) (5.5 * 1024), BYTE).getUnit(), KILOBYTE);

        assertEquals(succinctDataSize(5 * 1024, KILOBYTE), DataSize.of(5, MEGABYTE));
        assertEquals(succinctDataSize(5 * 1024, KILOBYTE).getUnit(), MEGABYTE);
    }

    @Test
    public void testToBytesValueString()
    {
        DataSize oneByte = DataSize.ofBytes(1);
        assertEquals(oneByte.toString(), oneByte.toBytesValueString(), "exact values match toString()");
        assertEquals(oneByte.toString(), "1B");

        for (DataSize.Unit unit : DataSize.Unit.values()) {
            DataSize oneInUnit = DataSize.of(1, unit);
            assertEquals(oneInUnit.toBytesValueString(), unit.getFactor() + "B");
            assertEquals(DataSize.valueOf(oneInUnit.toBytesValueString()), oneInUnit);
        }

        assertEquals(DataSize.of(1, KILOBYTE).toBytesValueString(), "1024B");
        assertEquals(DataSize.of(2, MEGABYTE).toBytesValueString(), "2097152B");
        assertEquals(DataSize.of(3, GIGABYTE).toBytesValueString(), "3221225472B");
        assertEquals(DataSize.of(4, TERABYTE).toBytesValueString(), "4398046511104B");
        assertEquals(DataSize.of(5, PETABYTE).toBytesValueString(), "5629499534213120B");
    }

    @Test(dataProvider = "conversions")
    public void testConversions(DataSize.Unit unit, DataSize.Unit toUnit, double factor)
    {
        DataSize size = DataSize.of(1, unit).to(toUnit);
        assertEquals(size.getUnit(), toUnit);
        assertEquals(size.getValue(), factor);

        assertEquals(size.getValue(toUnit), factor);
    }

    @Test(dataProvider = "conversions")
    public void testConvertToMostSuccinctDataSize(DataSize.Unit unit, DataSize.Unit toUnit, double factor)
    {
        DataSize size = new DataSize(factor, toUnit);
        DataSize actual = size.succinct();
        assertThat(actual).isEqualTo(DataSize.of(1, unit));
        assertThat(actual.getValue(unit)).isCloseTo(1.0, within(0.001));
        assertThat(actual.getUnit()).isEqualTo(unit);
    }

    @Test
    public void testValueEquivalence()
    {
        comparisonTester()
                .addLesserGroup(group(0))
                .addGreaterGroup(group(1))
                .addGreaterGroup(group(123352))
                .addGreaterGroup(group(Long.MAX_VALUE))
                .check();
    }

    private static Iterable<DataSize> group(long bytes)
    {
        return ImmutableList.of(
                DataSize.ofBytes(bytes),
                DataSize.ofBytes(bytes).convertTo(KILOBYTE),
                DataSize.ofBytes(bytes).convertTo(MEGABYTE),
                DataSize.ofBytes(bytes).convertTo(GIGABYTE),
                DataSize.ofBytes(bytes).convertTo(TERABYTE),
                DataSize.ofBytes(bytes).convertTo(PETABYTE)
        );
    }

    @Test
    public void testDeprecatedDoubleValueEquivalence()
    {
        comparisonTester()
                .addLesserGroup(deprecatedDoubleValueGroup(0))
                .addGreaterGroup(deprecatedDoubleValueGroup(1))
                .addGreaterGroup(deprecatedDoubleValueGroup(123352))
                .addGreaterGroup(deprecatedDoubleValueGroup(Long.MAX_VALUE))
                .check();
    }

    private static Iterable<DataSize> deprecatedDoubleValueGroup(double bytes)
    {
        return ImmutableList.of(
                new DataSize(bytes, BYTE),
                new DataSize(bytes / 1024, KILOBYTE),
                new DataSize(bytes / 1024 / 1024, MEGABYTE),
                new DataSize(bytes / 1024 / 1024 / 1024, GIGABYTE),
                new DataSize(bytes / 1024 / 1024 / 1024 / 1024, TERABYTE),
                new DataSize(bytes / 1024 / 1024 / 1024 / 1024 / 1024, PETABYTE)
        );
    }

    @Test(dataProvider = "printedValues")
    public void testToString(String expectedString, double value, DataSize.Unit unit)
    {
        assertEquals(new DataSize(value, unit).toString(), expectedString);
    }

    @Test(dataProvider = "printedValues")
    public void testNonEnglishLocale(String expectedString, double value, DataSize.Unit unit)
    {
        synchronized (Locale.class) {
            Locale previous = Locale.getDefault();
            Locale.setDefault(Locale.GERMAN);
            try {
                assertEquals(new DataSize(value, unit).toString(), expectedString);
            }
            finally {
                Locale.setDefault(previous);
            }
        }
    }

    @Test(dataProvider = "parseableValues")
    public void testValueOf(String string, double expectedValue, DataSize.Unit expectedUnit)
    {
        DataSize size = DataSize.valueOf(string);

        assertEquals(size.getUnit(), expectedUnit);
        assertThat(size.getValue()).isCloseTo(expectedValue, within(0.001));
    }

    @Test
    public void testValueOfDecimalAndLongHandling()
    {
        //  If parsed as double, this results in only 2^53 not (2^53) + 1
        DataSize tooLargeAsDouble = DataSize.ofBytes((1L << 53) + 1);
        assertEquals(DataSize.valueOf(Long.toString(tooLargeAsDouble.toBytes()) + "B"), tooLargeAsDouble, "should parse as long and not double");
        assertEquals(DataSize.valueOf(Long.toString(tooLargeAsDouble.toBytes()) + ".0B"), DataSize.ofBytes(1L << 53), "should parse as double");
    }

    @Test(expectedExceptions = NullPointerException.class, expectedExceptionsMessageRegExp = "size is null")
    public void testValueOfRejectsNull()
    {
        DataSize.valueOf(null);
    }

    @Test(expectedExceptions = IllegalArgumentException.class, expectedExceptionsMessageRegExp = "size is empty")
    public void testValueOfRejectsEmptyString()
    {
        DataSize.valueOf("");
    }

    @Test(expectedExceptions = IllegalArgumentException.class, expectedExceptionsMessageRegExp = "Unknown unit: kg")
    public void testValueOfRejectsInvalidUnit()
    {
        DataSize.valueOf("1.234 kg");
    }

    @Test(expectedExceptions = IllegalArgumentException.class, expectedExceptionsMessageRegExp = "size is not a valid.*")
    public void testValueOfRejectsInvalidNumber()
    {
        DataSize.valueOf("1.2x4 B");
    }

    @SuppressWarnings("ResultOfObjectAllocationIgnored")
    @Test(expectedExceptions = IllegalArgumentException.class, expectedExceptionsMessageRegExp = "size is negative: -1.0")
    public void testDeprecatedConstructorRejectsNegativeSize()
    {
        new DataSize(-1.0, BYTE);
    }

    @SuppressWarnings("ResultOfObjectAllocationIgnored")
    @Test(expectedExceptions = IllegalArgumentException.class, expectedExceptionsMessageRegExp = "size is infinite")
    public void testDeprecatedConstructorRejectsInfiniteSize()
    {
        new DataSize(Double.POSITIVE_INFINITY, BYTE);
    }

    @SuppressWarnings("ResultOfObjectAllocationIgnored")
    @Test(expectedExceptions = IllegalArgumentException.class, expectedExceptionsMessageRegExp = "size is infinite")
    public void testDeprecatedConstructorRejectsInfiniteSize2()
    {
        new DataSize(Double.NEGATIVE_INFINITY, BYTE);
    }

    @SuppressWarnings("ResultOfObjectAllocationIgnored")
    @Test(expectedExceptions = IllegalArgumentException.class, expectedExceptionsMessageRegExp = "size is not a number")
    public void testDeprecatedConstructorRejectsNaN()
    {
        new DataSize(Double.NaN, BYTE);
    }

    @SuppressWarnings("ResultOfObjectAllocationIgnored")
    @Test(expectedExceptions = NullPointerException.class, expectedExceptionsMessageRegExp = "unit is null")
    public void testConstructorRejectsNullUnit()
    {
        new DataSize(1, null);
    }

    @Test(expectedExceptions = IllegalArgumentException.class, expectedExceptionsMessageRegExp = "size is negative: -11")
    public void testOfRejectsNegativeSize()
    {
        DataSize.of(-11, MEGABYTE);
    }

    @Test(expectedExceptions = IllegalArgumentException.class, expectedExceptionsMessageRegExp = "size is too large to be represented in bytes: 9223372036854775807MB")
    public void testOfDetectsOverflow()
    {
        DataSize.of(Long.MAX_VALUE, MEGABYTE);
    }

    @Test(expectedExceptions = IllegalArgumentException.class, expectedExceptionsMessageRegExp = "bytes is negative")
    public void testOfBytesRejectsNegativeSize()
    {
        DataSize.ofBytes(-1);
    }

    @Test(expectedExceptions = NullPointerException.class, expectedExceptionsMessageRegExp = "unit is null")
    public void testOfRejectsNullUnit()
    {
        DataSize.of(1, null);
    }

    @Test
    public void testToBytes()
    {
        assertEquals(DataSize.of(0, BYTE).toBytes(), 0);
        assertEquals(DataSize.of(0, MEGABYTE).toBytes(), 0);
        assertEquals(DataSize.of(1, BYTE).toBytes(), 1);
        assertEquals(DataSize.of(1, KILOBYTE).toBytes(), 1024);
        assertEquals(DataSize.of(42, MEGABYTE).toBytes(), 42L * 1024 * 1024);
        assertEquals(DataSize.of(1, TERABYTE).toBytes(), 1024L * 1024 * 1024 * 1024);
        assertEquals(DataSize.of(1, PETABYTE).toBytes(), 1024L * 1024 * 1024 * 1024 * 1024);
        assertEquals(DataSize.of(1024, PETABYTE).toBytes(), 1024L * 1024 * 1024 * 1024 * 1024 * 1024);
        assertEquals(DataSize.of(8191, PETABYTE).toBytes(), 8191L * 1024 * 1024 * 1024 * 1024 * 1024);
        assertEquals(DataSize.of(Long.MAX_VALUE, BYTE).toBytes(), Long.MAX_VALUE);
    }

    @Test
    public void testDeprecatedToBytes()
    {
        assertEquals(new DataSize(37.0 / 1024, KILOBYTE).toBytes(), 37);
        assertEquals(new DataSize(Long.MAX_VALUE / 1024.0, KILOBYTE).toBytes(), Long.MAX_VALUE);
    }

    @Test
    public void testRoundTo()
    {
        assertEquals(DataSize.ofBytes(0).roundTo(BYTE), 0);
        assertEquals(new DataSize(0.5, BYTE).roundTo(BYTE), 1);
        assertEquals(new DataSize((42 * 1024) + 511, BYTE).roundTo(KILOBYTE), 42);
        assertEquals(new DataSize((42 * 1024) + 512, BYTE).roundTo(KILOBYTE), 43);
        assertEquals(new DataSize(513, TERABYTE).roundTo(PETABYTE), 1);
    }

    @Test(expectedExceptions = IllegalArgumentException.class, expectedExceptionsMessageRegExp = "size is too large .*")
    public void testSizeTooLarge()
    {
        new DataSize(Long.MAX_VALUE + 1024.0001, BYTE);
    }

    @Test(expectedExceptions = IllegalArgumentException.class, expectedExceptionsMessageRegExp = "size is too large .*")
    public void testSizeTooLargeInStaticOfMethod()
    {
        DataSize.of(9000, PETABYTE);
    }

    @Test(expectedExceptions = IllegalArgumentException.class, expectedExceptionsMessageRegExp = "size is too large .*")
    public void testRoundToTooLarge3()
    {
        new DataSize(9000 * 1024, PETABYTE).roundTo(KILOBYTE);
    }

    @Test
    public void testJsonRoundTrip()
    {
        assertJsonRoundTrip(DataSize.ofBytes(1L));
        assertJsonRoundTrip(new DataSize(1.234, KILOBYTE));
        assertJsonRoundTrip(new DataSize(1.234, MEGABYTE));
        assertJsonRoundTrip(new DataSize(1.234, GIGABYTE));
        assertJsonRoundTrip(new DataSize(1.234, TERABYTE));
        assertJsonRoundTrip(new DataSize(1.234, PETABYTE));

        assertJsonRoundTrip(DataSize.ofBytes(Long.MAX_VALUE));

        // Arbitrary assortment of values other than 1, primes and multiples of 2 and 10
        long[] sizes = new long[] { 1, 2, 3, 5, 7, 8, 11, 16, 100 };

        for (DataSize.Unit unit : DataSize.Unit.values()) {
            for (long size : sizes) {
                assertJsonRoundTrip(DataSize.ofBytes(size).convertTo(unit));
                assertJsonRoundTrip(DataSize.of(size, unit));
            }
        }
    }

    private static void assertJsonRoundTrip(DataSize dataSize)
    {
        JsonCodec<DataSize> dataSizeCodec = JsonCodec.jsonCodec(DataSize.class);
        String json = dataSizeCodec.toJson(dataSize);
        DataSize dataSizeCopy = dataSizeCodec.fromJson(json);

        assertThat(dataSizeCopy.getValue(BYTE))
                .isCloseTo(dataSize.getValue(BYTE), within(0.001));

        assertEquals(dataSizeCopy.getUnit(), BYTE, "JSON serialization should always be in bytes");
        assertEquals(dataSize.toBytes(), dataSizeCopy.toBytes(), "byte value equivalence");
        assertEquals(dataSize, dataSizeCopy, "equals method equivalence");
    }

    @DataProvider(name = "parseableValues", parallel = true)
    private Object[][] parseableValues()
    {
        return new Object[][] {
                // spaces
                new Object[] {"1234 B", 1234, BYTE},
                new Object[] {"1234 kB", 1234, KILOBYTE},
                new Object[] {"1234 MB", 1234, MEGABYTE},
                new Object[] {"1234 GB", 1234, GIGABYTE},
                new Object[] {"1234 TB", 1234, TERABYTE},
                new Object[] {"1234 PB", 1234, PETABYTE},
                new Object[] {"1234.567 kB", 1234.567, KILOBYTE},
                new Object[] {"1234.567 MB", 1234.567, MEGABYTE},
                new Object[] {"1234.567 GB", 1234.567, GIGABYTE},
                new Object[] {"1234.567 TB", 1234.567, TERABYTE},
                new Object[] {"1234.567 PB", 1234.567, PETABYTE},
                // no spaces
                new Object[] {"1234B", 1234, BYTE},
                new Object[] {"1234kB", 1234, KILOBYTE},
                new Object[] {"1234MB", 1234, MEGABYTE},
                new Object[] {"1234GB", 1234, GIGABYTE},
                new Object[] {"1234TB", 1234, TERABYTE},
                new Object[] {"1234PB", 1234, PETABYTE},
                new Object[] {"1234.567kB", 1234.567, KILOBYTE},
                new Object[] {"1234.567MB", 1234.567, MEGABYTE},
                new Object[] {"1234.567GB", 1234.567, GIGABYTE},
                new Object[] {"1234.567TB", 1234.567, TERABYTE},
                new Object[] {"1234.567PB", 1234.567, PETABYTE}
        };
    }

    @DataProvider(name = "printedValues", parallel = true)
    private Object[][] printedValues()
    {
        return new Object[][] {
                new Object[] {"1234B", 1234, BYTE},
                new Object[] {"1234kB", 1234, KILOBYTE},
                new Object[] {"1234MB", 1234, MEGABYTE},
                new Object[] {"1234GB", 1234, GIGABYTE},
                new Object[] {"1234TB", 1234, TERABYTE},
                new Object[] {"1234PB", 1234, PETABYTE},
                new Object[] {"1234.57kB", 1234.567, KILOBYTE},
                new Object[] {"1234.57MB", 1234.567, MEGABYTE},
                new Object[] {"1234.57GB", 1234.567, GIGABYTE},
                new Object[] {"1234.57TB", 1234.567, TERABYTE},
                new Object[] {"1234.57PB", 1234.567, PETABYTE}
        };
    }

    @DataProvider(name = "conversions", parallel = true)
    private Object[][] conversions()
    {
        return new Object[][] {
                new Object[] {BYTE, BYTE, 1},
                new Object[] {BYTE, KILOBYTE, 1.0 / 1024},
                new Object[] {BYTE, MEGABYTE, 1.0 / 1024 / 1024},
                new Object[] {BYTE, GIGABYTE, 1.0 / 1024 / 1024 / 1024},
                new Object[] {BYTE, TERABYTE, 1.0 / 1024 / 1024 / 1024 / 1024},
                new Object[] {BYTE, PETABYTE, 1.0 / 1024 / 1024 / 1024 / 1024 / 1024},

                new Object[] {KILOBYTE, BYTE, 1024},
                new Object[] {KILOBYTE, KILOBYTE, 1},
                new Object[] {KILOBYTE, MEGABYTE, 1.0 / 1024},
                new Object[] {KILOBYTE, GIGABYTE, 1.0 / 1024 / 1024},
                new Object[] {KILOBYTE, TERABYTE, 1.0 / 1024 / 1024 / 1024},
                new Object[] {KILOBYTE, PETABYTE, 1.0 / 1024 / 1024 / 1024 / 1024},

                new Object[] {MEGABYTE, BYTE, 1024 * 1024},
                new Object[] {MEGABYTE, KILOBYTE, 1024},
                new Object[] {MEGABYTE, MEGABYTE, 1},
                new Object[] {MEGABYTE, GIGABYTE, 1.0 / 1024},
                new Object[] {MEGABYTE, TERABYTE, 1.0 / 1024 / 1024},
                new Object[] {MEGABYTE, PETABYTE, 1.0 / 1024 / 1024 / 1024},

                new Object[] {GIGABYTE, BYTE, 1024 * 1024 * 1024},
                new Object[] {GIGABYTE, KILOBYTE, 1024 * 1024},
                new Object[] {GIGABYTE, MEGABYTE, 1024},
                new Object[] {GIGABYTE, GIGABYTE, 1},
                new Object[] {GIGABYTE, TERABYTE, 1.0 / 1024},
                new Object[] {GIGABYTE, PETABYTE, 1.0 / 1024 / 1024},

                new Object[] {TERABYTE, BYTE, 1024L * 1024 * 1024 * 1024},
                new Object[] {TERABYTE, KILOBYTE, 1024 * 1024 * 1024},
                new Object[] {TERABYTE, MEGABYTE, 1024 * 1024},
                new Object[] {TERABYTE, GIGABYTE, 1024},
                new Object[] {TERABYTE, TERABYTE, 1},
                new Object[] {TERABYTE, PETABYTE, 1.0 / 1024},

                new Object[] {PETABYTE, BYTE, 1024L * 1024 * 1024 * 1024 * 1024},
                new Object[] {PETABYTE, KILOBYTE, 1024L * 1024 * 1024 * 1024},
                new Object[] {PETABYTE, MEGABYTE, 1024 * 1024 * 1024},
                new Object[] {PETABYTE, GIGABYTE, 1024 * 1024},
                new Object[] {PETABYTE, TERABYTE, 1024},
                new Object[] {PETABYTE, PETABYTE, 1},
                };
    }
}
