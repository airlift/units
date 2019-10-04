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

import org.apache.bval.jsr.ApacheValidationProvider;
import org.testng.annotations.Test;

import javax.validation.ConstraintViolation;
import javax.validation.Validation;
import javax.validation.ValidationException;
import javax.validation.Validator;

import java.util.Set;
import java.util.concurrent.TimeUnit;

import static io.airlift.units.ConstraintValidatorAssert.assertThat;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

public class TestDurationValidator
{
    private static final Validator VALIDATOR = Validation.byProvider(ApacheValidationProvider.class).configure().buildValidatorFactory().getValidator();

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
        VALIDATOR.validate(new NullMinAnnotation());
    }

    @Test
    public void testAllowsNullMaxAnnotation()
    {
        VALIDATOR.validate(new NullMaxAnnotation());
    }

    @Test
    public void testDetectsBrokenMinAnnotation()
    {
        assertThatThrownBy(() -> VALIDATOR.validate(new BrokenMinAnnotation()))
                .isInstanceOf(ValidationException.class)
                .hasRootCauseInstanceOf(IllegalArgumentException.class)
                .hasMessage("java.lang.IllegalArgumentException: duration is not a valid data duration string: broken");
    }

    @Test
    public void testDetectsBrokenMaxAnnotation()
    {
        assertThatThrownBy(() -> VALIDATOR.validate(new BrokenMaxAnnotation()))
                .isInstanceOf(ValidationException.class)
                .hasRootCauseInstanceOf(IllegalArgumentException.class)
                .hasMessage("java.lang.IllegalArgumentException: duration is not a valid data duration string: broken");
    }

    @Test
    public void testPassesValidation()
    {
        assertThat(VALIDATOR.validate(new ConstrainedDuration(new Duration(7, TimeUnit.SECONDS))))
                .isEmpty();
    }

    @Test
    public void testFailsMaxDurationConstraint()
    {
        Set<? extends ConstraintViolation<?>> violations = VALIDATOR.validate(new ConstrainedDuration(new Duration(11, TimeUnit.SECONDS)));
        assertThat(violations).hasSize(2);
        assertThat(violations.stream().map(violation -> violation.getConstraintDescriptor().getAnnotation()))
                .allMatch(MaxDuration.class::isInstance);
        assertThat(violations.stream().map(violation -> violation.getPropertyPath().toString()))
                .containsOnly("constrainedByMax", "constrainedByMinAndMax");
    }

    @Test
    public void testFailsMinDurationConstraint()
    {
        Set<? extends ConstraintViolation<?>> violations = VALIDATOR.validate(new ConstrainedDuration(new Duration(1, TimeUnit.SECONDS)));
        assertThat(violations).hasSize(2);
        assertThat(violations.stream().map(violation -> violation.getConstraintDescriptor().getAnnotation()))
                .allMatch(MinDuration.class::isInstance);
        assertThat(violations.stream().map(violation -> violation.getPropertyPath().toString()))
                .containsOnly("constrainedByMin", "constrainedByMinAndMax");
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
}
