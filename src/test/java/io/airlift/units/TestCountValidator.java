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

import java.util.Set;

import static io.airlift.units.ConstraintValidatorAssert.assertThat;
import static io.airlift.units.Count.Magnitude.BILLION;
import static io.airlift.units.Count.Magnitude.MILLION;
import static io.airlift.units.Count.Magnitude.THOUSAND;
import static org.assertj.core.api.Assertions.assertThat;
import static org.testng.Assert.assertTrue;
import static org.testng.Assert.fail;

public class TestCountValidator
{
    private static final Validator VALIDATOR = Validation.byProvider(ApacheValidationProvider.class).configure().buildValidatorFactory().getValidator();

    @Test
    public void testMaxCountValidator()
    {
        MaxCountValidator maxValidator = new MaxCountValidator();
        maxValidator.initialize(new MockMaxCount(new Count(8, MILLION)));

        assertThat(maxValidator).isValidFor(new Count(0, THOUSAND));
        assertThat(maxValidator).isValidFor(new Count(5, THOUSAND));
        assertThat(maxValidator).isValidFor(new Count(5005, THOUSAND));
        assertThat(maxValidator).isValidFor(new Count(5, MILLION));
        assertThat(maxValidator).isValidFor(new Count(8, MILLION));
        assertThat(maxValidator).isValidFor(new Count(8000, THOUSAND));
        assertThat(maxValidator).isInvalidFor(new Count(8001, THOUSAND));
        assertThat(maxValidator).isInvalidFor(new Count(9, MILLION));
        assertThat(maxValidator).isInvalidFor(new Count(1, BILLION));
    }

    @Test
    public void testMinCountValidator()
    {
        MinCountValidator minValidator = new MinCountValidator();
        minValidator.initialize(new MockMinCount(new Count(4, MILLION)));

        assertThat(minValidator).isValidFor(new Count(4, MILLION));
        assertThat(minValidator).isValidFor(new Count(4096, THOUSAND));
        assertThat(minValidator).isValidFor(new Count(5, MILLION));
        assertThat(minValidator).isInvalidFor(new Count(0, BILLION));
        assertThat(minValidator).isInvalidFor(new Count(1, MILLION));
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
        try {
            VALIDATOR.validate(new BrokenMinAnnotation());
            fail("expected a ValidationException caused by an IllegalArgumentException");
        }
        catch (ValidationException e) {
            assertThat(e).hasRootCauseInstanceOf(IllegalArgumentException.class);
        }
    }

    @Test
    public void testDetectsBrokenMaxAnnotation()
    {
        try {
            VALIDATOR.validate(new BrokenMaxAnnotation());
            fail("expected a ValidationException caused by an IllegalArgumentException");
        }
        catch (ValidationException e) {
            assertThat(e).hasRootCauseInstanceOf(IllegalArgumentException.class);
        }
    }

    @Test
    public void testPassesValidation()
    {
        ConstrainedCount object = new ConstrainedCount(new Count(7, MILLION));
        Set<ConstraintViolation<ConstrainedCount>> violations = VALIDATOR.validate(object);
        assertTrue(violations.isEmpty());
    }

    @Test
    public void testFailsMaxCountConstraint()
    {
        ConstrainedCount object = new ConstrainedCount(new Count(11, MILLION));
        Set<ConstraintViolation<ConstrainedCount>> violations = VALIDATOR.validate(object);
        assertThat(violations).hasSize(2);

        for (ConstraintViolation<ConstrainedCount> violation : violations) {
            assertThat(violation.getConstraintDescriptor().getAnnotation()).isInstanceOf(MaxCount.class);
        }
    }

    @Test
    public void testFailsMinCountConstraint()
    {
        ConstrainedCount object = new ConstrainedCount(new Count(1, MILLION));
        Set<ConstraintViolation<ConstrainedCount>> violations = VALIDATOR.validate(object);
        assertThat(violations).hasSize(2);

        for (ConstraintViolation<ConstrainedCount> violation : violations) {
            assertThat(violation.getConstraintDescriptor().getAnnotation()).isInstanceOf(MinCount.class);
        }
    }

    @SuppressWarnings("UnusedDeclaration")
    public static class ConstrainedCount
    {
        private final Count count;

        public ConstrainedCount(Count count)
        {
            this.count = count;
        }

        @MinCount("5M")
        public Count getConstrainedByMin()
        {
            return count;
        }

        @MaxCount("10M")
        public Count getConstrainedByMax()
        {
            return count;
        }

        @MinCount("5000K")
        @MaxCount("10000K")
        public Count getConstrainedByMinAndMax()
        {
            return count;
        }
    }

    @SuppressWarnings("UnusedDeclaration")
    public static class NullMinAnnotation
    {
        @MinCount("1M")
        public Count getConstrainedByMin()
        {
            return null;
        }
    }

    @SuppressWarnings("UnusedDeclaration")
    public static class NullMaxAnnotation
    {
        @MaxCount("1M")
        public Count getConstrainedByMin()
        {
            return null;
        }
    }

    @SuppressWarnings("UnusedDeclaration")
    public static class BrokenMinAnnotation
    {
        @MinCount("broken")
        public Count getConstrainedByMin()
        {
            return new Count(32, THOUSAND);
        }
    }

    @SuppressWarnings("UnusedDeclaration")
    public static class BrokenMaxAnnotation
    {
        @MinCount("broken")
        public Count getConstrainedByMin()
        {
            return new Count(32, THOUSAND);
        }
    }
}
