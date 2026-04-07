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

import com.google.common.collect.ImmutableList;
import io.airlift.json.JsonCodec;
import io.airlift.units.Count.Magnitude;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import java.util.Locale;

import static io.airlift.testing.EquivalenceTester.comparisonTester;
import static io.airlift.units.Count.Magnitude.BILLION;
import static io.airlift.units.Count.Magnitude.MILLION;
import static io.airlift.units.Count.Magnitude.QUADRILLION;
import static io.airlift.units.Count.Magnitude.SINGLE;
import static io.airlift.units.Count.Magnitude.THOUSAND;
import static io.airlift.units.Count.Magnitude.TRILION;
import static io.airlift.units.Count.succinctRounded;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.data.Percentage.withPercentage;
import static org.testng.Assert.assertEquals;

public class TestCount
{
    @Test
    public void testSuccinctFactories()
    {
        assertEquals(succinctRounded(123), new Count(123, SINGLE));
        assertEquals(succinctRounded(5 * 1000 * 1000), new Count(5, MILLION));

        assertEquals(succinctRounded(123, SINGLE), new Count(123, SINGLE));
        assertEquals(succinctRounded(5 * 1000, THOUSAND), new Count(5, MILLION));
    }

    @Test(dataProvider = "conversions")
    public void testConversions(Magnitude magnitude, Magnitude toMagnitude, long factor)
    {
        Count count = new Count(1, magnitude).convertTo(toMagnitude);
        assertEquals(count.getMagnitude(), toMagnitude);
        assertEquals(count.getValue(), factor);

        assertEquals(count.getValue(toMagnitude), factor);
    }

    @Test(dataProvider = "conversions")
    public void testConvertToMostSuccinctCountRounded(Magnitude magnitude, Magnitude toMagnitude, long factor)
    {
        Count count = new Count(factor, toMagnitude);
        Count actual = count.convertToMostSuccinctRounded();
        assertThat(actual.getValue()).isEqualTo(1);
        assertThat(actual.getMagnitude()).isEqualTo(magnitude);
        assertThat(actual.getValue(magnitude)).isEqualTo(1);
        assertThat(actual.getMagnitude()).isEqualTo(magnitude);
    }


    @Test
    public void testConvertToMostSuccinctRounded()
    {
        assertThat(new Count(1_499, SINGLE).convertToMostSuccinctRounded()).isEqualTo(new Count(1, THOUSAND));
        assertThat(new Count(1_500, SINGLE).convertToMostSuccinctRounded()).isEqualTo(new Count(2, THOUSAND));
    }

    @SuppressWarnings("ResultOfObjectAllocationIgnored")
    @Test(dataProvider = "precisionLossConversions", expectedExceptions = IllegalArgumentException.class, expectedExceptionsMessageRegExp = ".*precision loss")
    public void testPrecisionLossConversions(Magnitude magnitude, Magnitude toMagnitude)
    {
        new Count(1, magnitude).convertTo(toMagnitude);
    }

    @Test
    public void testEquivalence()
    {
        comparisonTester()
                .addLesserGroup(group(1_000_000))
                .addGreaterGroup(group(2_000_000))
                .addGreaterGroup(group(1_000_000_000))
                .check();
    }

    private static Iterable<Count> group(long count)
    {
        return ImmutableList.of(
                new Count(count, SINGLE),
                new Count(count / 1000, THOUSAND),
                new Count(count / 1000 / 1000, MILLION)
        );
    }

    @Test(dataProvider = "printedValues")
    public void testToString(String expectedString, long value, Magnitude magnitude)
    {
        assertEquals(new Count(value, magnitude).toString(), expectedString);
    }

    @Test(dataProvider = "printedValues")
    public void testNonEnglishLocale(String expectedString, long value, Magnitude magnitude)
    {
        synchronized (Locale.class) {
            Locale previous = Locale.getDefault();
            Locale.setDefault(Locale.GERMAN);
            try {
                assertEquals(new Count(value, magnitude).toString(), expectedString);
            }
            finally {
                Locale.setDefault(previous);
            }
        }
    }

    @Test(dataProvider = "parseableValues")
    public void testValueOf(String string, long expectedValue, Magnitude expectedMagnitude)
    {
        Count count = Count.valueOf(string);

        assertEquals(count.getMagnitude(), expectedMagnitude);
        assertEquals(count.getValue(), expectedValue);
    }

    @Test(expectedExceptions = NullPointerException.class, expectedExceptionsMessageRegExp = "count is null")
    public void testValueOfRejectsNull()
    {
        Count.valueOf(null);
    }

    @Test(expectedExceptions = IllegalArgumentException.class, expectedExceptionsMessageRegExp = "count is empty")
    public void testValueOfRejectsEmptyString()
    {
        Count.valueOf("");
    }

    @Test(expectedExceptions = IllegalArgumentException.class, expectedExceptionsMessageRegExp = "Unknown magnitude: x")
    public void testValueOfRejectsInvalidUnit()
    {
        Count.valueOf("1234 x");
    }

    @Test(expectedExceptions = IllegalArgumentException.class, expectedExceptionsMessageRegExp = "Not a valid count string.*")
    public void testValueOfRejectsInvalidNumber()
    {
        Count.valueOf("1.24 B");
    }

    @SuppressWarnings("ResultOfObjectAllocationIgnored")
    @Test(expectedExceptions = IllegalArgumentException.class, expectedExceptionsMessageRegExp = "count is negative")
    public void testConstructorRejectsNegativeSize()
    {
        new Count(-1, SINGLE);
    }

    @Test
    public void testMaxValue()
    {
        new Count(Long.MAX_VALUE, SINGLE);
    }

    @SuppressWarnings("ResultOfObjectAllocationIgnored")
    @Test(expectedExceptions = NullPointerException.class, expectedExceptionsMessageRegExp = "magnitude is null")
    public void testConstructorRejectsNullUnit()
    {
        new Count(1, null);
    }

    @Test
    public void testJsonRoundTrip()
    {
        assertJsonRoundTrip(new Count(1234, SINGLE));
        assertJsonRoundTrip(new Count(1234, THOUSAND));
        assertJsonRoundTrip(new Count(1234, MILLION));
        assertJsonRoundTrip(new Count(1234, BILLION));
        assertJsonRoundTrip(new Count(1234, TRILION));
        assertJsonRoundTrip(new Count(1234, QUADRILLION));
    }

    private static void assertJsonRoundTrip(Count count)
    {
        JsonCodec<Count> dataSizeCodec = JsonCodec.jsonCodec(Count.class);
        String json = dataSizeCodec.toJson(count);
        Count dataSizeCopy = dataSizeCodec.fromJson(json);

        assertThat(dataSizeCopy.getValue(SINGLE))
                .isCloseTo(count.getValue(SINGLE), withPercentage(1));
    }

    @DataProvider(name = "parseableValues", parallel = true)
    private Object[][] parseableValues()
    {
        return new Object[][] {
                // spaces
                new Object[] {"1000", 1000, SINGLE},
                new Object[] {"1000 K", 1000, THOUSAND},
                new Object[] {"1000 M", 1000, MILLION},
                new Object[] {"1000 B", 1000, BILLION},
                new Object[] {"1000 T", 1000, TRILION},
                new Object[] {"1000 P", 1000, QUADRILLION},
                // no spaces
                new Object[] {"1000", 1000, SINGLE},
                new Object[] {"1000K", 1000, THOUSAND},
                new Object[] {"1000M", 1000, MILLION},
                new Object[] {"1000B", 1000, BILLION},
                new Object[] {"1000T", 1000, TRILION},
                new Object[] {"1000P", 1000, QUADRILLION},
        };
    }

    @DataProvider(name = "printedValues", parallel = true)
    private Object[][] printedValues()
    {
        return new Object[][] {
                new Object[] {"1000", 1000, SINGLE},
                new Object[] {"1000K", 1000, THOUSAND},
                new Object[] {"1000M", 1000, MILLION},
                new Object[] {"1000B", 1000, BILLION},
                new Object[] {"1000T", 1000, TRILION},
                new Object[] {"1000P", 1000, QUADRILLION},
        };
    }

    @DataProvider(name = "conversions", parallel = true)
    private Object[][] conversions()
    {
        return new Object[][] {

                new Object[] {SINGLE, SINGLE, 1},

                new Object[] {THOUSAND, SINGLE, 1000},
                new Object[] {THOUSAND, THOUSAND, 1},

                new Object[] {MILLION, SINGLE, 1000_000},
                new Object[] {MILLION, THOUSAND, 1000},
                new Object[] {MILLION, MILLION, 1},

                new Object[] {BILLION, SINGLE, 1000_000_000},
                new Object[] {BILLION, THOUSAND, 1000_000},
                new Object[] {BILLION, MILLION, 1000},
                new Object[] {BILLION, BILLION, 1},

                new Object[] {TRILION, SINGLE, 1000_000_000_000L},
                new Object[] {TRILION, THOUSAND, 1000_000_000},
                new Object[] {TRILION, MILLION, 1000_000},
                new Object[] {TRILION, BILLION, 1000},
                new Object[] {TRILION, TRILION, 1},

                new Object[] {QUADRILLION, SINGLE, 1000_000_000_000_000L},
                new Object[] {QUADRILLION, THOUSAND, 1000_000_000_000L},
                new Object[] {QUADRILLION, MILLION, 1000_000_000},
                new Object[] {QUADRILLION, BILLION, 1000_000},
                new Object[] {QUADRILLION, TRILION, 1000},
                new Object[] {QUADRILLION, QUADRILLION, 1},
        };
    }

    @DataProvider(name = "precisionLossConversions", parallel = true)
    private Object[][] precisionLossConversions()
    {
        return new Object[][] {
                new Object[] {SINGLE, THOUSAND},
                new Object[] {SINGLE, MILLION},
                new Object[] {SINGLE, BILLION},
                new Object[] {SINGLE, TRILION},
                new Object[] {SINGLE, QUADRILLION},

                new Object[] {THOUSAND, MILLION},
                new Object[] {THOUSAND, BILLION},
                new Object[] {THOUSAND, TRILION},
                new Object[] {THOUSAND, QUADRILLION},

                new Object[] {MILLION, BILLION},
                new Object[] {MILLION, TRILION},
                new Object[] {MILLION, QUADRILLION},

                new Object[] {BILLION, TRILION},
                new Object[] {BILLION, QUADRILLION},

                new Object[] {TRILION, QUADRILLION},
        };
    }
}
