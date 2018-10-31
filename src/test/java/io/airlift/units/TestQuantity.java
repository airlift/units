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
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import java.util.Locale;

import static io.airlift.testing.EquivalenceTester.comparisonTester;
import static io.airlift.units.Quantity.Magnitude.BILLION;
import static io.airlift.units.Quantity.Magnitude.MILLION;
import static io.airlift.units.Quantity.Magnitude.QUADRILLION;
import static io.airlift.units.Quantity.Magnitude.SINGLE;
import static io.airlift.units.Quantity.Magnitude.THOUSAND;
import static io.airlift.units.Quantity.Magnitude.TRILION;
import static io.airlift.units.Quantity.succinctRounded;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.data.Percentage.withPercentage;
import static org.testng.Assert.assertEquals;

public class TestQuantity
{
    @Test
    public void testSuccinctFactories()
    {
        assertEquals(succinctRounded(123), new Quantity(123, SINGLE));
        assertEquals(succinctRounded(5 * 1000 * 1000), new Quantity(5, MILLION));

        assertEquals(succinctRounded(123, SINGLE), new Quantity(123, SINGLE));
        assertEquals(succinctRounded(5 * 1000, THOUSAND), new Quantity(5, MILLION));
    }

    @Test(dataProvider = "conversions")
    public void testConversions(Quantity.Magnitude magnitude, Quantity.Magnitude toMagnitude, long factor)
    {
        Quantity quantity = new Quantity(1, magnitude).convertTo(toMagnitude);
        assertEquals(quantity.getMagnitude(), toMagnitude);
        assertEquals(quantity.getValue(), factor);

        assertEquals(quantity.getValue(toMagnitude), factor);
    }

    @Test(dataProvider = "conversions")
    public void testConvertToMostSuccinctQuantityRounded(Quantity.Magnitude magnitude, Quantity.Magnitude toMagnitude, long factor)
    {
        Quantity quantity = new Quantity(factor, toMagnitude);
        Quantity actual = quantity.convertToMostSuccinctRounded();
        assertThat(actual.getValue()).isEqualTo(1);
        assertThat(actual.getMagnitude()).isEqualTo(magnitude);
        assertThat(actual.getValue(magnitude)).isEqualTo(1);
        assertThat(actual.getMagnitude()).isEqualTo(magnitude);
    }


    @Test
    public void testConvertToMostSuccinctRounded()
    {
        assertThat(new Quantity(1_499, SINGLE).convertToMostSuccinctRounded()).isEqualTo(new Quantity(1, THOUSAND));
        assertThat(new Quantity(1_500, SINGLE).convertToMostSuccinctRounded()).isEqualTo(new Quantity(2, THOUSAND));
    }

    @SuppressWarnings("ResultOfObjectAllocationIgnored")
    @Test(dataProvider = "precisionLossConversions", expectedExceptions = IllegalArgumentException.class, expectedExceptionsMessageRegExp = ".*precision loss")
    public void testPrecisionLossConversions(Quantity.Magnitude magnitude, Quantity.Magnitude toMagnitude)
    {
        new Quantity(1, magnitude).convertTo(toMagnitude);
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

    private static Iterable<Quantity> group(long quantity)
    {
        return ImmutableList.of(
                new Quantity(quantity, SINGLE),
                new Quantity(quantity / 1000, THOUSAND),
                new Quantity(quantity / 1000 / 1000, MILLION)
        );
    }

    @Test(dataProvider = "printedValues")
    public void testToString(String expectedString, long value, Quantity.Magnitude magnitude)
    {
        assertEquals(new Quantity(value, magnitude).toString(), expectedString);
    }

    @Test(dataProvider = "printedValues")
    public void testNonEnglishLocale(String expectedString, long value, Quantity.Magnitude magnitude)
    {
        synchronized (Locale.class) {
            Locale previous = Locale.getDefault();
            Locale.setDefault(Locale.GERMAN);
            try {
                assertEquals(new Quantity(value, magnitude).toString(), expectedString);
            }
            finally {
                Locale.setDefault(previous);
            }
        }
    }

    @Test(dataProvider = "parseableValues")
    public void testValueOf(String string, long expectedValue, Quantity.Magnitude expectedMagnitude)
    {
        Quantity quantity = Quantity.valueOf(string);

        assertEquals(quantity.getMagnitude(), expectedMagnitude);
        assertEquals(quantity.getValue(), expectedValue);
    }

    @Test(expectedExceptions = NullPointerException.class, expectedExceptionsMessageRegExp = "quantity is null")
    public void testValueOfRejectsNull()
    {
        Quantity.valueOf(null);
    }

    @Test(expectedExceptions = IllegalArgumentException.class, expectedExceptionsMessageRegExp = "quantity is empty")
    public void testValueOfRejectsEmptyString()
    {
        Quantity.valueOf("");
    }

    @Test(expectedExceptions = IllegalArgumentException.class, expectedExceptionsMessageRegExp = "Unknown unit: x")
    public void testValueOfRejectsInvalidUnit()
    {
        Quantity.valueOf("1234 x");
    }

    @Test(expectedExceptions = IllegalArgumentException.class, expectedExceptionsMessageRegExp = "Not a valid quantity string.*")
    public void testValueOfRejectsInvalidNumber()
    {
        Quantity.valueOf("1.24 B");
    }

    @SuppressWarnings("ResultOfObjectAllocationIgnored")
    @Test(expectedExceptions = IllegalArgumentException.class, expectedExceptionsMessageRegExp = "quantity is negative")
    public void testConstructorRejectsNegativeSize()
    {
        new Quantity(-1, SINGLE);
    }

    @Test
    public void testMaxValue()
    {
        new Quantity(Long.MAX_VALUE, SINGLE);
    }

    @SuppressWarnings("ResultOfObjectAllocationIgnored")
    @Test(expectedExceptions = NullPointerException.class, expectedExceptionsMessageRegExp = "unit is null")
    public void testConstructorRejectsNullUnit()
    {
        new Quantity(1, null);
    }

    @Test
    public void testJsonRoundTrip()
    {
        assertJsonRoundTrip(new Quantity(1234, SINGLE));
        assertJsonRoundTrip(new Quantity(1234, THOUSAND));
        assertJsonRoundTrip(new Quantity(1234, MILLION));
        assertJsonRoundTrip(new Quantity(1234, BILLION));
        assertJsonRoundTrip(new Quantity(1234, TRILION));
        assertJsonRoundTrip(new Quantity(1234, QUADRILLION));
    }

    private static void assertJsonRoundTrip(Quantity quantity)
    {
        JsonCodec<Quantity> quantityCodec = JsonCodec.jsonCodec(Quantity.class);
        String json = quantityCodec.toJson(quantity);
        Quantity quantityCopy = quantityCodec.fromJson(json);

        assertThat(quantityCopy.getValue(SINGLE))
                .isCloseTo(quantity.getValue(SINGLE), withPercentage(1));
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
