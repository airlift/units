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

import org.testng.annotations.Test;

import static io.airlift.units.ThreadCount.MachineInfo.getAvailablePhysicalProcessorCount;
import static java.lang.Math.round;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

public class TestThreadsCount
{
    public static final int AVAILABLE_PROCESSORS = getAvailablePhysicalProcessorCount();

    @Test
    public void ensureRequiredNumberOfProcessors()
    {
        // For tests to work properly we need at least two threads
        assertThat(AVAILABLE_PROCESSORS).isGreaterThan(1);
    }

    @Test
    public void testParsingIntegerValues()
    {
        assertThreadsCount("1", 1);
        assertThreadsCount("2", 2);
        assertThreadsCount("67", 67);
        assertThreadsCount("0", 0);
        assertThreadsCount(Integer.valueOf(Integer.MAX_VALUE).toString(), Integer.MAX_VALUE);
        assertInvalidValue("-1", "Thread count cannot be negative");
        assertInvalidValue("67.0", "Cannot parse value '67.0' as integer");
        assertInvalidValue(Long.valueOf(((long) Integer.MAX_VALUE) + 1).toString(), "Thread count is greater than 2^32 - 1");
    }

    @Test
    public void testParsingMultiplierPerCore()
    {
        assertThreadsCount("1C", AVAILABLE_PROCESSORS);
        assertThreadsCount("0.5 C", AVAILABLE_PROCESSORS / 2);
        assertThreadsCount("0.2 C", round(AVAILABLE_PROCESSORS / 5.0f));
        assertThreadsCount("1.5C", round(AVAILABLE_PROCESSORS * 1.5f));
        assertThreadsCount("2 C", AVAILABLE_PROCESSORS * 2);
        assertThreadsCount("0.0001 C", 0);
        assertInvalidValue("-0.0001 C", "Thread multiplier cannot be negative");
        assertInvalidValue(-1, "Thread count cannot be negative");
        assertInvalidValue("-1C", "Thread multiplier cannot be negative");
        assertInvalidValue("-1SC", "Cannot parse value '-1S' as float");
        assertInvalidValue("2147483647C", "Thread count is greater than 2^32 - 1");
        assertInvalidValue("3147483648C", "Thread count is greater than 2^32 - 1");
    }

    @Test
    public void testParsingBoundedValue()
    {
        assertBoundedThreadsCount("3", "1", "1", 1);
        assertBoundedThreadsCount("256C", "4", "16", 16);
        assertBoundedThreadsCount("3", "4", "16", 4);
    }

    private void assertThreadsCount(String value, int expected)
    {
        ThreadCount threadCount = ThreadCount.valueOf(value);
        assertThat(threadCount).isEqualTo(ThreadCount.exactValueOf(expected));
        assertThat(threadCount.getThreadCount()).isEqualTo(expected);
    }

    private void assertBoundedThreadsCount(String value, String min, String max, int expected)
    {
        ThreadCount threadCount = ThreadCount.boundedValueOf(value, min, max);
        assertThat(threadCount).isEqualTo(new ThreadCount(expected));
    }

    private void assertInvalidValue(String value, String expectedMessage)
    {
        assertThatThrownBy(() -> ThreadCount.valueOf(value))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessage(expectedMessage);
    }

    private void assertInvalidValue(int value, String expectedMessage)
    {
        assertThatThrownBy(() -> new ThreadCount(value))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessage(expectedMessage);
    }
}
