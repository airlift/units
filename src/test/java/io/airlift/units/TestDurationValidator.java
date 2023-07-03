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

import jakarta.validation.ValidationException;
import org.testng.annotations.Test;

import java.util.Optional;
import java.util.concurrent.TimeUnit;

import static io.airlift.testing.ValidationAssertions.assertFailsValidation;
import static io.airlift.testing.ValidationAssertions.assertValidates;
import static io.airlift.units.ConstraintValidatorAssert.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

public class TestDurationValidator
{
    @Test
    public void testMaxDurationValidator()
    {
        MaxDurationValidator maxValidator = new MaxDurationValidator();
        maxValidator.initialize(new MockMaxDuration(new Duration(5, TimeUnit.SECONDS)));

        assertThat(maxValidator).isValidFor(new Duration(0, TimeUnit.SECONDS));
        assertThat(maxValidator).isValidFor(new Duration(5, TimeUnit.SECONDS));
        assertThat(maxValidator).isInvalidFor(new Duration(6, TimeUnit.SECONDS));
    }

    @Test
    public void testMinDurationValidator()
    {
        MinDurationValidator minValidator = new MinDurationValidator();
        minValidator.initialize(new MockMinDuration(new Duration(5, TimeUnit.SECONDS)));

        assertThat(minValidator).isValidFor(new Duration(5, TimeUnit.SECONDS));
        assertThat(minValidator).isValidFor(new Duration(6, TimeUnit.SECONDS));
        assertThat(minValidator).isInvalidFor(new Duration(0, TimeUnit.SECONDS));
    }

    @Test
    public void testAllowsNullMinAnnotation()
    {
        assertValidates(new NullMinAnnotation());
    }

    @Test
    public void testAllowsNullMaxAnnotation()
    {
        assertValidates(new NullMaxAnnotation());
    }

    @Test
    public void testDetectsBrokenMinAnnotation()
    {
        assertThatThrownBy(() -> assertValidates(new BrokenMinAnnotation()))
                .isInstanceOf(ValidationException.class)
                .hasMessage("HV000032: Unable to initialize io.airlift.units.MinDurationValidator.")
                .hasRootCauseInstanceOf(IllegalArgumentException.class)
                .hasRootCauseMessage("duration is not a valid data duration string: broken");

        assertThatThrownBy(() -> assertValidates(new MinAnnotationOnOptional()))
                .isInstanceOf(ValidationException.class)
                .hasMessageContaining("No validator could be found for constraint 'io.airlift.units.MinDuration' validating type 'java.util.Optional<io.airlift.units.Duration>'. Check configuration for 'constrainedByMin'");

        assertThatThrownBy(() -> assertValidates(new BrokenOptionalMinAnnotation()))
                .isInstanceOf(ValidationException.class)
                .hasMessage("HV000032: Unable to initialize io.airlift.units.MinDurationValidator.")
                .hasRootCauseInstanceOf(IllegalArgumentException.class)
                .hasRootCauseMessage("duration is not a valid data duration string: broken");
    }

    @Test
    public void testDetectsBrokenMaxAnnotation()
    {
        assertThatThrownBy(() -> assertValidates(new BrokenMaxAnnotation()))
                .isInstanceOf(ValidationException.class)
                .hasMessage("HV000032: Unable to initialize io.airlift.units.MinDurationValidator.")
                .hasRootCauseInstanceOf(IllegalArgumentException.class)
                .hasRootCauseMessage("duration is not a valid data duration string: broken");

        assertThatThrownBy(() -> assertValidates(new MaxAnnotationOnOptional()))
                .isInstanceOf(ValidationException.class)
                .hasMessageContaining("No validator could be found for constraint 'io.airlift.units.MaxDuration' validating type 'java.util.Optional<io.airlift.units.Duration>'. Check configuration for 'constrainedByMin'");

        assertThatThrownBy(() -> assertValidates(new BrokenOptionalMaxAnnotation()))
                .isInstanceOf(ValidationException.class)
                .hasMessage("HV000032: Unable to initialize io.airlift.units.MaxDurationValidator.")
                .hasRootCauseInstanceOf(IllegalArgumentException.class)
                .hasRootCauseMessage("duration is not a valid data duration string: broken");
    }

    @Test
    public void testPassesValidation()
    {
        assertValidates(new ConstrainedDuration(new Duration(7, TimeUnit.SECONDS)));
        assertValidates(new ConstrainedOptionalDuration(Optional.of(new Duration(7, TimeUnit.SECONDS))));
        assertValidates(new ConstrainedOptionalDuration(Optional.empty()));
        assertValidates(new ConstrainedOptionalDuration(null));
    }

    @Test
    public void testFailsMaxDurationConstraint()
    {
        assertFailsValidation(new ConstrainedDuration(new Duration(11, TimeUnit.SECONDS)), "constrainedByMax", "must be less than or equal to 10s", MaxDuration.class);
        assertFailsValidation(new ConstrainedDuration(new Duration(11, TimeUnit.SECONDS)), "constrainedByMinAndMax", "must be less than or equal to 10000ms", MaxDuration.class);

        assertFailsValidation(new ConstrainedOptionalDuration(Optional.of(new Duration(11, TimeUnit.SECONDS))), "constrainedByMax", "must be less than or equal to 10s", MaxDuration.class);
        assertFailsValidation(new ConstrainedOptionalDuration(Optional.of(new Duration(11, TimeUnit.SECONDS))), "constrainedByMinAndMax", "must be less than or equal to 10000ms", MaxDuration.class);
    }

    @Test
    public void testFailsMinDurationConstraint()
    {
        assertFailsValidation(new ConstrainedDuration(new Duration(1, TimeUnit.SECONDS)), "constrainedByMin", "must be greater than or equal to 5s", MinDuration.class);
        assertFailsValidation(new ConstrainedDuration(new Duration(1, TimeUnit.SECONDS)), "constrainedByMinAndMax", "must be greater than or equal to 5000ms", MinDuration.class);

        assertFailsValidation(new ConstrainedOptionalDuration(Optional.of(new Duration(1, TimeUnit.SECONDS))), "constrainedByMin", "must be greater than or equal to 5s", MinDuration.class);
        assertFailsValidation(new ConstrainedOptionalDuration(Optional.of(new Duration(1, TimeUnit.SECONDS))), "constrainedByMinAndMax", "must be greater than or equal to 5000ms", MinDuration.class);
    }

    @SuppressWarnings("UnusedDeclaration")
    public static class ConstrainedDuration
    {
        private final Duration duration;

        public ConstrainedDuration(Duration duration)
        {
            this.duration = duration;
        }

        @MinDuration("5s")
        public Duration getConstrainedByMin()
        {
            return duration;
        }

        @MaxDuration("10s")
        public Duration getConstrainedByMax()
        {
            return duration;
        }

        @MinDuration("5000ms")
        @MaxDuration("10000ms")
        public Duration getConstrainedByMinAndMax()
        {
            return duration;
        }
    }

    @SuppressWarnings("UnusedDeclaration")
    public static class ConstrainedOptionalDuration
    {
        private final Optional<Duration> duration;

        public ConstrainedOptionalDuration(Optional<Duration> duration)
        {
            this.duration = duration;
        }

        public Optional<@MinDuration("5s") Duration> getConstrainedByMin()
        {
            return duration;
        }

        public Optional<@MaxDuration("10s") Duration> getConstrainedByMax()
        {
            return duration;
        }

        public Optional<@MinDuration("5000ms") @MaxDuration("10000ms") Duration> getConstrainedByMinAndMax()
        {
            return duration;
        }
    }

    public static class NullMinAnnotation
    {
        @SuppressWarnings("UnusedDeclaration")
        @MinDuration("1s")
        public Duration getConstrainedByMin()
        {
            return null;
        }
    }

    public static class NullMaxAnnotation
    {
        @SuppressWarnings("UnusedDeclaration")
        @MaxDuration("1s")
        public Duration getConstrainedByMin()
        {
            return null;
        }
    }

    public static class BrokenMinAnnotation
    {
        @SuppressWarnings("UnusedDeclaration")
        @MinDuration("broken")
        public Duration getConstrainedByMin()
        {
            return new Duration(10, TimeUnit.SECONDS);
        }
    }

    public static class BrokenMaxAnnotation
    {
        @SuppressWarnings("UnusedDeclaration")
        @MinDuration("broken")
        public Duration getConstrainedByMin()
        {
            return new Duration(10, TimeUnit.SECONDS);
        }
    }

    public static class MinAnnotationOnOptional
    {
        @SuppressWarnings("UnusedDeclaration")
        @MinDuration("1s")
        public Optional<Duration> getConstrainedByMin()
        {
            return Optional.empty();
        }
    }

    public static class MaxAnnotationOnOptional
    {
        @SuppressWarnings("UnusedDeclaration")
        @MaxDuration("1s")
        public Optional<Duration> getConstrainedByMin()
        {
            return Optional.empty();
        }
    }

    public static class BrokenOptionalMinAnnotation
    {
        @SuppressWarnings("UnusedDeclaration")
        public Optional<@MinDuration("broken") Duration> getConstrainedByMin()
        {
            return Optional.empty();
        }
    }

    public static class BrokenOptionalMaxAnnotation
    {
        @SuppressWarnings("UnusedDeclaration")
        public Optional<@MaxDuration("broken") Duration> getConstrainedByMax()
        {
            return Optional.empty();
        }
    }
}
