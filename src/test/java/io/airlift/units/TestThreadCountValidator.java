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

import jakarta.validation.ValidationException;
import org.testng.annotations.Test;

import java.util.Optional;

import static io.airlift.testing.ValidationAssertions.assertFailsValidation;
import static io.airlift.testing.ValidationAssertions.assertValidates;
import static io.airlift.units.ConstraintValidatorAssert.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

public class TestThreadCountValidator
{
    @Test
    public void testMaxThreadCountValidator()
    {
        MaxThreadCountValidator maxValidator = new MaxThreadCountValidator();
        maxValidator.initialize(new MockMaxThreadCount(new ThreadCount(8)));

        assertThat(maxValidator).isValidFor(new ThreadCount(0));
        assertThat(maxValidator).isValidFor(new ThreadCount(5));
        assertThat(maxValidator).isValidFor(new ThreadCount(8));
    }

    @Test
    public void testMinThreadCountValidator()
    {
        MinThreadCountValidator minValidator = new MinThreadCountValidator();
        minValidator.initialize(new MockMinThreadCount(new ThreadCount(4)));

        assertThat(minValidator).isValidFor(new ThreadCount(4));
        assertThat(minValidator).isValidFor(new ThreadCount(5));
    }

    @Test
    public void testAllowsNullMinAnnotation()
    {
        assertValidates(new TestThreadCountValidator.NullMinAnnotation());
    }

    @Test
    public void testAllowsNullMaxAnnotation()
    {
        assertValidates(new TestThreadCountValidator.NullMaxAnnotation());
    }

    @Test
    public void testDetectsBrokenMinAnnotation()
    {
        assertThatThrownBy(() -> assertValidates(new TestThreadCountValidator.BrokenMinAnnotation()))
                .isInstanceOf(ValidationException.class)
                .hasMessage("HV000032: Unable to initialize io.airlift.units.MinThreadCountValidator.")
                .hasRootCauseInstanceOf(NumberFormatException.class)
                .hasRootCauseMessage("For input string: \"broken\"");

        assertThatThrownBy(() -> assertValidates(new TestThreadCountValidator.MinAnnotationOnOptional()))
                .isInstanceOf(ValidationException.class)
                .hasMessageContaining("No validator could be found for constraint 'io.airlift.units.MinThreadCount' validating type 'java.util.Optional<io.airlift.units.ThreadCount>'. Check configuration for 'constrainedByMin'");

        assertThatThrownBy(() -> assertValidates(new TestThreadCountValidator.BrokenOptionalMinAnnotation()))
                .isInstanceOf(ValidationException.class)
                .hasMessage("HV000032: Unable to initialize io.airlift.units.MinThreadCountValidator.")
                .hasRootCauseInstanceOf(NumberFormatException.class)
                .hasRootCauseMessage("For input string: \"broken\"");
    }

    @Test
    public void testDetectsBrokenMaxAnnotation()
    {
        assertThatThrownBy(() -> assertValidates(new TestThreadCountValidator.BrokenMaxAnnotation()))
                .isInstanceOf(ValidationException.class)
                .hasMessage("HV000032: Unable to initialize io.airlift.units.MaxThreadCountValidator.")
                .hasRootCauseInstanceOf(NumberFormatException.class)
                .hasRootCauseMessage("For input string: \"broken\"");

        assertThatThrownBy(() -> assertValidates(new TestThreadCountValidator.MaxAnnotationOnOptional()))
                .isInstanceOf(ValidationException.class)
                .hasMessageContaining("No validator could be found for constraint 'io.airlift.units.MaxThreadCount' validating type 'java.util.Optional<io.airlift.units.ThreadCount>'. Check configuration for 'constrainedByMin'");

        assertThatThrownBy(() -> assertValidates(new TestThreadCountValidator.BrokenOptionalMaxAnnotation()))
                .isInstanceOf(ValidationException.class)
                .hasMessage("HV000032: Unable to initialize io.airlift.units.MaxThreadCountValidator.")
                .hasRootCauseInstanceOf(NumberFormatException.class)
                .hasRootCauseMessage("For input string: \"broken\"");
    }

    @Test
    public void testPassesValidation()
    {
        assertValidates(new TestThreadCountValidator.ConstrainedThreadCount(new ThreadCount(7)));
        assertValidates(new TestThreadCountValidator.ConstrainedOptionalThreadCount(Optional.of(new ThreadCount(7))));
        assertValidates(new TestThreadCountValidator.ConstrainedOptionalThreadCount(Optional.empty()));
        assertValidates(new TestThreadCountValidator.ConstrainedOptionalThreadCount(null));
    }

    @Test
    public void testFailsMaxThreadCountConstraint()
    {
        assertFailsValidation(new TestThreadCountValidator.ConstrainedThreadCount(new ThreadCount(11)), "constrainedByMinAndMax", "must be less than or equal to 10", MaxThreadCount.class);
        assertFailsValidation(new TestThreadCountValidator.ConstrainedThreadCount(new ThreadCount(11)), "constrainedByMax", "must be less than or equal to 10", MaxThreadCount.class);

        assertFailsValidation(new TestThreadCountValidator.ConstrainedOptionalThreadCount(Optional.of(new ThreadCount(11))), "constrainedByMinAndMax", "must be less than or equal to 10", MaxThreadCount.class);
        assertFailsValidation(new TestThreadCountValidator.ConstrainedOptionalThreadCount(Optional.of(new ThreadCount(11))), "constrainedByMax", "must be less than or equal to 10", MaxThreadCount.class);
    }

    @Test
    public void testFailsMinThreadCountConstraint()
    {
        assertFailsValidation(new TestThreadCountValidator.ConstrainedThreadCount(new ThreadCount(1)), "constrainedByMin", "must be greater than or equal to 5", MinThreadCount.class);
        assertFailsValidation(new TestThreadCountValidator.ConstrainedThreadCount(new ThreadCount(1)), "constrainedByMinAndMax", "must be greater than or equal to 5", MinThreadCount.class);

        assertFailsValidation(new TestThreadCountValidator.ConstrainedOptionalThreadCount(Optional.of(new ThreadCount(1))), "constrainedByMin", "must be greater than or equal to 5", MinThreadCount.class);
        assertFailsValidation(new TestThreadCountValidator.ConstrainedOptionalThreadCount(Optional.of(new ThreadCount(1))), "constrainedByMinAndMax", "must be greater than or equal to 5", MinThreadCount.class);
    }

    @SuppressWarnings("UnusedDeclaration")
    public static class ConstrainedThreadCount
    {
        private final ThreadCount threadCount;

        public ConstrainedThreadCount(ThreadCount threadCount)
        {
            this.threadCount = threadCount;
        }

        @MinThreadCount("5")
        public ThreadCount getConstrainedByMin()
        {
            return threadCount;
        }

        @MaxThreadCount("10")
        public ThreadCount getConstrainedByMax()
        {
            return threadCount;
        }

        @MinThreadCount("5")
        @MaxThreadCount("10")
        public ThreadCount getConstrainedByMinAndMax()
        {
            return threadCount;
        }
    }

    @SuppressWarnings("UnusedDeclaration")
    public static class ConstrainedOptionalThreadCount
    {
        private final Optional<ThreadCount> threadCount;

        public ConstrainedOptionalThreadCount(Optional<ThreadCount> threadCount)
        {
            this.threadCount = threadCount;
        }

        public Optional<@MinThreadCount("5") ThreadCount> getConstrainedByMin()
        {
            return threadCount;
        }

        public Optional<@MaxThreadCount("10") ThreadCount> getConstrainedByMax()
        {
            return threadCount;
        }

        public Optional<@MinThreadCount("5") @MaxThreadCount("10") ThreadCount> getConstrainedByMinAndMax()
        {
            return threadCount;
        }
    }

    public static class NullMinAnnotation
    {
        @SuppressWarnings("UnusedDeclaration")
        @MinThreadCount("16")
        public ThreadCount getConstrainedByMin()
        {
            return null;
        }
    }

    public static class NullMaxAnnotation
    {
        @SuppressWarnings("UnusedDeclaration")
        @MaxThreadCount("16")
        public ThreadCount getConstrainedByMin()
        {
            return null;
        }
    }

    public static class BrokenMinAnnotation
    {
        @SuppressWarnings("UnusedDeclaration")
        @MinThreadCount("broken")
        public ThreadCount getConstrainedByMin()
        {
            return new ThreadCount(32);
        }
    }

    public static class BrokenMaxAnnotation
    {
        @SuppressWarnings("UnusedDeclaration")
        @MaxThreadCount("broken")
        public ThreadCount getConstrainedByMin()
        {
            return new ThreadCount(32);
        }
    }

    public static class MinAnnotationOnOptional
    {
        @SuppressWarnings("UnusedDeclaration")
        @MinThreadCount("16")
        public Optional<ThreadCount> getConstrainedByMin()
        {
            return Optional.empty();
        }
    }

    public static class MaxAnnotationOnOptional
    {
        @SuppressWarnings("UnusedDeclaration")
        @MaxThreadCount("16")
        public Optional<ThreadCount> getConstrainedByMin()
        {
            return Optional.empty();
        }
    }

    public static class BrokenOptionalMinAnnotation
    {
        @SuppressWarnings("UnusedDeclaration")
        public Optional<@MinThreadCount("broken") ThreadCount> getConstrainedByMin()
        {
            return Optional.empty();
        }
    }

    public static class BrokenOptionalMaxAnnotation
    {
        @SuppressWarnings("UnusedDeclaration")
        public Optional<@MaxThreadCount("broken") ThreadCount> getConstrainedByMax()
        {
            return Optional.empty();
        }
    }
}
