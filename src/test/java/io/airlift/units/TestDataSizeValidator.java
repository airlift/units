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
import static io.airlift.units.DataSize.Unit.GIGABYTE;
import static io.airlift.units.DataSize.Unit.KILOBYTE;
import static io.airlift.units.DataSize.Unit.MEGABYTE;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

public class TestDataSizeValidator
{
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
                .hasMessage("HV000032: Unable to initialize io.airlift.units.MinDataSizeValidator.")
                .hasRootCauseInstanceOf(IllegalArgumentException.class)
                .hasRootCauseMessage("size is not a valid data size string: broken");

        assertThatThrownBy(() -> assertValidates(new MinAnnotationOnOptional()))
                .isInstanceOf(ValidationException.class)
                .hasMessageContaining("No validator could be found for constraint 'io.airlift.units.MinDataSize' validating type 'java.util.Optional<io.airlift.units.DataSize>'. Check configuration for 'constrainedByMin'");

        assertThatThrownBy(() -> assertValidates(new BrokenOptionalMinAnnotation()))
                .isInstanceOf(ValidationException.class)
                .hasMessage("HV000032: Unable to initialize io.airlift.units.MinDataSizeValidator.")
                .hasRootCauseInstanceOf(IllegalArgumentException.class)
                .hasRootCauseMessage("size is not a valid data size string: broken");
    }

    @Test
    public void testDetectsBrokenMaxAnnotation()
    {
        assertThatThrownBy(() -> assertValidates(new BrokenMaxAnnotation()))
                .isInstanceOf(ValidationException.class)
                .hasMessage("HV000032: Unable to initialize io.airlift.units.MaxDataSizeValidator.")
                .hasRootCauseInstanceOf(IllegalArgumentException.class)
                .hasRootCauseMessage("size is not a valid data size string: broken");

        assertThatThrownBy(() -> assertValidates(new MaxAnnotationOnOptional()))
                .isInstanceOf(ValidationException.class)
                .hasMessageContaining("No validator could be found for constraint 'io.airlift.units.MaxDataSize' validating type 'java.util.Optional<io.airlift.units.DataSize>'. Check configuration for 'constrainedByMin'");

        assertThatThrownBy(() -> assertValidates(new BrokenOptionalMaxAnnotation()))
                .isInstanceOf(ValidationException.class)
                .hasMessage("HV000032: Unable to initialize io.airlift.units.MaxDataSizeValidator.")
                .hasRootCauseInstanceOf(IllegalArgumentException.class)
                .hasRootCauseMessage("size is not a valid data size string: broken");
    }

    @Test
    public void testPassesValidation()
    {
        assertValidates(new ConstrainedDataSize(new DataSize(7, MEGABYTE)));
        assertValidates(new ConstrainedOptionalDataSize(Optional.of(new DataSize(7, MEGABYTE))));
        assertValidates(new ConstrainedOptionalDataSize(Optional.empty()));
        assertValidates(new ConstrainedOptionalDataSize(null));
    }

    @Test
    public void testFailsMaxDataSizeConstraint()
    {
        assertFailsValidation(new ConstrainedDataSize(new DataSize(11, MEGABYTE)), "constrainedByMinAndMax", "must be less than or equal to 10000kB", MaxDataSize.class);
        assertFailsValidation(new ConstrainedDataSize(new DataSize(11, MEGABYTE)), "constrainedByMax", "must be less than or equal to 10MB", MaxDataSize.class);

        assertFailsValidation(new ConstrainedOptionalDataSize(Optional.of(new DataSize(11, MEGABYTE))), "constrainedByMinAndMax", "must be less than or equal to 10000kB", MaxDataSize.class);
        assertFailsValidation(new ConstrainedOptionalDataSize(Optional.of(new DataSize(11, MEGABYTE))), "constrainedByMax", "must be less than or equal to 10MB", MaxDataSize.class);
    }

    @Test
    public void testFailsMinDataSizeConstraint()
    {
        assertFailsValidation(new ConstrainedDataSize(new DataSize(1, MEGABYTE)), "constrainedByMin", "must be greater than or equal to 5MB", MinDataSize.class);
        assertFailsValidation(new ConstrainedDataSize(new DataSize(1, MEGABYTE)), "constrainedByMinAndMax", "must be greater than or equal to 5000kB", MinDataSize.class);

        assertFailsValidation(new ConstrainedOptionalDataSize(Optional.of(new DataSize(1, MEGABYTE))), "constrainedByMin", "must be greater than or equal to 5MB", MinDataSize.class);
        assertFailsValidation(new ConstrainedOptionalDataSize(Optional.of(new DataSize(1, MEGABYTE))), "constrainedByMinAndMax", "must be greater than or equal to 5000kB", MinDataSize.class);
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
        @MaxDataSize("broken")
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
