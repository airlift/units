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

import org.apache.bval.jsr.ApacheValidationProvider;
import org.testng.annotations.Test;

import javax.validation.ConstraintViolation;
import javax.validation.Validation;
import javax.validation.ValidationException;
import javax.validation.Validator;

import java.util.Optional;
import java.util.Set;

import static io.airlift.units.ConstraintValidatorAssert.assertThat;
import static io.airlift.units.DataSize.Unit.GIGABYTE;
import static io.airlift.units.DataSize.Unit.KILOBYTE;
import static io.airlift.units.DataSize.Unit.MEGABYTE;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.testng.Assert.assertTrue;

public class TestDataSizeValidator
{
    private static final Validator VALIDATOR = Validation.byProvider(ApacheValidationProvider.class).configure().buildValidatorFactory().getValidator();

    @Test
    public void testMaxDataSizeValidator()
    {
        MaxDataSizeValidator maxValidator = new MaxDataSizeValidator();
        maxValidator.initialize(new MockMaxDataSize(new DataSize(8, MEGABYTE)));

        assertThat(maxValidator).isValidFor(new DataSize(0, KILOBYTE));
        assertThat(maxValidator).isValidFor(new DataSize(5, KILOBYTE));
        assertThat(maxValidator).isValidFor(new DataSize(5005, KILOBYTE));
        assertThat(maxValidator).isValidFor(new DataSize(5, MEGABYTE));
        assertThat(maxValidator).isValidFor(new DataSize(8, MEGABYTE));
        assertThat(maxValidator).isValidFor(new DataSize(8192, KILOBYTE));
        assertThat(maxValidator).isInvalidFor(new DataSize(9, MEGABYTE));
        assertThat(maxValidator).isInvalidFor(new DataSize(1, GIGABYTE));
    }

    @Test
    public void testMinDataSizeValidator()
    {
        MinDataSizeValidator minValidator = new MinDataSizeValidator();
        minValidator.initialize(new MockMinDataSize(new DataSize(4, MEGABYTE)));

        assertThat(minValidator).isValidFor(new DataSize(4, MEGABYTE));
        assertThat(minValidator).isValidFor(new DataSize(4096, KILOBYTE));
        assertThat(minValidator).isValidFor(new DataSize(5, MEGABYTE));
        assertThat(minValidator).isInvalidFor(new DataSize(0, GIGABYTE));
        assertThat(minValidator).isInvalidFor(new DataSize(1, MEGABYTE));
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
                .hasMessage("java.lang.IllegalArgumentException: size is not a valid data size string: broken");

        assertThatThrownBy(() -> VALIDATOR.validate(new MinAnnotationOnOptional()))
                .isInstanceOf(ValidationException.class)
                .hasMessage("No compliant io.airlift.units.MinDataSize ConstraintValidator found for annotated element of type java.util.Optional<T>");

        assertThatThrownBy(() -> VALIDATOR.validate(new BrokenOptionalMinAnnotation()))
                .isInstanceOf(ValidationException.class)
                .hasRootCauseInstanceOf(IllegalArgumentException.class)
                .hasMessage("java.lang.IllegalArgumentException: size is not a valid data size string: broken");
    }

    @Test
    public void testDetectsBrokenMaxAnnotation()
    {
        assertThatThrownBy(() -> VALIDATOR.validate(new BrokenMaxAnnotation()))
                .isInstanceOf(ValidationException.class)
                .hasRootCauseInstanceOf(IllegalArgumentException.class)
                .hasMessage("java.lang.IllegalArgumentException: size is not a valid data size string: broken");

        assertThatThrownBy(() -> VALIDATOR.validate(new MaxAnnotationOnOptional()))
                .isInstanceOf(ValidationException.class)
                .hasMessage("No compliant io.airlift.units.MaxDataSize ConstraintValidator found for annotated element of type java.util.Optional<T>");

        assertThatThrownBy(() -> VALIDATOR.validate(new BrokenOptionalMaxAnnotation()))
                .isInstanceOf(ValidationException.class)
                .hasRootCauseInstanceOf(IllegalArgumentException.class)
                .hasMessage("java.lang.IllegalArgumentException: size is not a valid data size string: broken");
    }

    @Test
    public void testPassesValidation()
    {
        assertTrue(VALIDATOR.validate(new ConstrainedDataSize(new DataSize(7, MEGABYTE))).isEmpty());

        assertThat(VALIDATOR.validate(new ConstrainedOptionalDataSize(Optional.of(new DataSize(7, MEGABYTE))))).isEmpty();

        assertThat(VALIDATOR.validate(new ConstrainedOptionalDataSize(Optional.empty()))).isEmpty();

        assertThat(VALIDATOR.validate(new ConstrainedOptionalDataSize(null))).isEmpty();
    }

    @Test
    public void testFailsMaxDataSizeConstraint()
    {
        Set<? extends ConstraintViolation<?>> violations = VALIDATOR.validate(new ConstrainedDataSize(new DataSize(11, MEGABYTE)));
        assertThat(violations).hasSize(2);
        assertThat(violations)
                .extracting(violation -> violation.getConstraintDescriptor().getAnnotation())
                .allMatch(MaxDataSize.class::isInstance);
        assertThat(violations)
                .extracting(violation -> violation.getPropertyPath().toString())
                .containsOnly("constrainedByMax", "constrainedByMinAndMax");
        assertThat(violations)
                .extracting(ConstraintViolation::getMessage)
                .containsOnly("must be less than or equal to 10000kB", "must be less than or equal to 10MB");

        violations = VALIDATOR.validate(new ConstrainedOptionalDataSize(Optional.of(new DataSize(11, MEGABYTE))));
        assertThat(violations).hasSize(2);
        assertThat(violations)
                .extracting(violation -> violation.getConstraintDescriptor().getAnnotation())
                .allMatch(MaxDataSize.class::isInstance);
        assertThat(violations)
                .extracting(violation -> violation.getPropertyPath().toString())
                .containsOnly("constrainedByMax", "constrainedByMinAndMax");
        assertThat(violations)
                .extracting(ConstraintViolation::getMessage)
                .containsOnly("must be less than or equal to 10000kB", "must be less than or equal to 10MB");
    }

    @Test
    public void testFailsMinDataSizeConstraint()
    {
        Set<? extends ConstraintViolation<?>> violations = VALIDATOR.validate(new ConstrainedDataSize(new DataSize(1, MEGABYTE)));
        assertThat(violations).hasSize(2);
        assertThat(violations)
                .extracting(violation -> violation.getConstraintDescriptor().getAnnotation())
                .allMatch(MinDataSize.class::isInstance);
        assertThat(violations)
                .extracting(violation -> violation.getPropertyPath().toString())
                .containsOnly("constrainedByMin", "constrainedByMinAndMax");
        assertThat(violations)
                .extracting(ConstraintViolation::getMessage)
                .containsOnly("must be greater than or equal to 5MB", "must be greater than or equal to 5000kB");

        violations = VALIDATOR.validate(new ConstrainedOptionalDataSize(Optional.of(new DataSize(1, MEGABYTE))));
        assertThat(violations).hasSize(2);
        assertThat(violations)
                .extracting(violation -> violation.getConstraintDescriptor().getAnnotation())
                .allMatch(MinDataSize.class::isInstance);
        assertThat(violations)
                .extracting(violation -> violation.getPropertyPath().toString())
                .containsOnly("constrainedByMin", "constrainedByMinAndMax");
        assertThat(violations)
                .extracting(ConstraintViolation::getMessage)
                .containsOnly("must be greater than or equal to 5MB", "must be greater than or equal to 5000kB");
    }

    @SuppressWarnings("UnusedDeclaration")
    public static class ConstrainedDataSize
    {
        private final DataSize dataSize;

        public ConstrainedDataSize(DataSize dataSize)
        {
            this.dataSize = dataSize;
        }

        @MinDataSize("5MB")
        public DataSize getConstrainedByMin()
        {
            return dataSize;
        }

        @MaxDataSize("10MB")
        public DataSize getConstrainedByMax()
        {
            return dataSize;
        }

        @MinDataSize("5000kB")
        @MaxDataSize("10000kB")
        public DataSize getConstrainedByMinAndMax()
        {
            return dataSize;
        }
    }

    @SuppressWarnings("UnusedDeclaration")
    public static class ConstrainedOptionalDataSize
    {
        private final Optional<DataSize> dataSize;

        public ConstrainedOptionalDataSize(Optional<DataSize> dataSize)
        {
            this.dataSize = dataSize;
        }

        public Optional<@MinDataSize("5MB") DataSize> getConstrainedByMin()
        {
            return dataSize;
        }

        public Optional<@MaxDataSize("10MB") DataSize> getConstrainedByMax()
        {
            return dataSize;
        }

        public Optional<@MinDataSize("5000kB") @MaxDataSize("10000kB") DataSize> getConstrainedByMinAndMax()
        {
            return dataSize;
        }
    }

    public static class NullMinAnnotation
    {
        @SuppressWarnings("UnusedDeclaration")
        @MinDataSize("1MB")
        public DataSize getConstrainedByMin()
        {
            return null;
        }
    }

    public static class NullMaxAnnotation
    {
        @SuppressWarnings("UnusedDeclaration")
        @MaxDataSize("1MB")
        public DataSize getConstrainedByMin()
        {
            return null;
        }
    }

    public static class BrokenMinAnnotation
    {
        @SuppressWarnings("UnusedDeclaration")
        @MinDataSize("broken")
        public DataSize getConstrainedByMin()
        {
            return new DataSize(32, KILOBYTE);
        }
    }

    public static class BrokenMaxAnnotation
    {
        @SuppressWarnings("UnusedDeclaration")
        @MinDataSize("broken")
        public DataSize getConstrainedByMin()
        {
            return new DataSize(32, KILOBYTE);
        }
    }

    public static class MinAnnotationOnOptional
    {
        @SuppressWarnings("UnusedDeclaration")
        @MinDataSize("1MB")
        public Optional<DataSize> getConstrainedByMin()
        {
            return Optional.empty();
        }
    }

    public static class MaxAnnotationOnOptional
    {
        @SuppressWarnings("UnusedDeclaration")
        @MaxDataSize("1MB")
        public Optional<DataSize> getConstrainedByMin()
        {
            return Optional.empty();
        }
    }

    public static class BrokenOptionalMinAnnotation
    {
        @SuppressWarnings("UnusedDeclaration")
        public Optional<@MinDataSize("broken") DataSize> getConstrainedByMin()
        {
            return Optional.empty();
        }
    }

    public static class BrokenOptionalMaxAnnotation
    {
        @SuppressWarnings("UnusedDeclaration")
        public Optional<@MaxDataSize("broken") DataSize> getConstrainedByMax()
        {
            return Optional.empty();
        }
    }
}
