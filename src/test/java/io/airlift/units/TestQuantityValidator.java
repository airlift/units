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
import static io.airlift.units.Quantity.Magnitude.BILLION;
import static io.airlift.units.Quantity.Magnitude.MILLION;
import static io.airlift.units.Quantity.Magnitude.THOUSAND;
import static org.assertj.core.api.Assertions.assertThat;
import static org.testng.Assert.assertTrue;
import static org.testng.Assert.fail;

public class TestQuantityValidator
{
    private static final Validator VALIDATOR = Validation.byProvider(ApacheValidationProvider.class).configure().buildValidatorFactory().getValidator();

    @Test
    public void testMaxQuantityValidator()
    {
        MaxQuantityValidator maxValidator = new MaxQuantityValidator();
        maxValidator.initialize(new MockMaxQuantity(new Quantity(8, MILLION)));

        assertThat(maxValidator).isValidFor(new Quantity(0, THOUSAND));
        assertThat(maxValidator).isValidFor(new Quantity(5, THOUSAND));
        assertThat(maxValidator).isValidFor(new Quantity(5005, THOUSAND));
        assertThat(maxValidator).isValidFor(new Quantity(5, MILLION));
        assertThat(maxValidator).isValidFor(new Quantity(8, MILLION));
        assertThat(maxValidator).isValidFor(new Quantity(8000, THOUSAND));
        assertThat(maxValidator).isInvalidFor(new Quantity(8001, THOUSAND));
        assertThat(maxValidator).isInvalidFor(new Quantity(9, MILLION));
        assertThat(maxValidator).isInvalidFor(new Quantity(1, BILLION));
    }

    @Test
    public void testMinQuantityValidator()
    {
        MinQuantityValidator minValidator = new MinQuantityValidator();
        minValidator.initialize(new MockMinQuantity(new Quantity(4, MILLION)));

        assertThat(minValidator).isValidFor(new Quantity(4, MILLION));
        assertThat(minValidator).isValidFor(new Quantity(4096, THOUSAND));
        assertThat(minValidator).isValidFor(new Quantity(5, MILLION));
        assertThat(minValidator).isInvalidFor(new Quantity(0, BILLION));
        assertThat(minValidator).isInvalidFor(new Quantity(1, MILLION));
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
        ConstrainedQuantity object = new ConstrainedQuantity(new Quantity(7, MILLION));
        Set<ConstraintViolation<ConstrainedQuantity>> violations = VALIDATOR.validate(object);
        assertTrue(violations.isEmpty());
    }

    @Test
    public void testFailsMaxQuantityConstraint()
    {
        ConstrainedQuantity object = new ConstrainedQuantity(new Quantity(11, MILLION));
        Set<ConstraintViolation<ConstrainedQuantity>> violations = VALIDATOR.validate(object);
        assertThat(violations).hasSize(2);

        for (ConstraintViolation<ConstrainedQuantity> violation : violations) {
            assertThat(violation.getConstraintDescriptor().getAnnotation()).isInstanceOf(MaxQuantity.class);
        }
    }

    @Test
    public void testFailsMinQuantityConstraint()
    {
        ConstrainedQuantity object = new ConstrainedQuantity(new Quantity(1, MILLION));
        Set<ConstraintViolation<ConstrainedQuantity>> violations = VALIDATOR.validate(object);
        assertThat(violations).hasSize(2);

        for (ConstraintViolation<ConstrainedQuantity> violation : violations) {
            assertThat(violation.getConstraintDescriptor().getAnnotation()).isInstanceOf(MinQuantity.class);
        }
    }

    @SuppressWarnings("UnusedDeclaration")
    public static class ConstrainedQuantity
    {
        private final Quantity quantity;

        public ConstrainedQuantity(Quantity quantity)
        {
            this.quantity = quantity;
        }

        @MinQuantity("5M")
        public Quantity getConstrainedByMin()
        {
            return quantity;
        }

        @MaxQuantity("10M")
        public Quantity getConstrainedByMax()
        {
            return quantity;
        }

        @MinQuantity("5000K")
        @MaxQuantity("10000K")
        public Quantity getConstrainedByMinAndMax()
        {
            return quantity;
        }
    }

    @SuppressWarnings("UnusedDeclaration")
    public static class NullMinAnnotation
    {
        @MinQuantity("1M")
        public Quantity getConstrainedByMin()
        {
            return null;
        }
    }

    @SuppressWarnings("UnusedDeclaration")
    public static class NullMaxAnnotation
    {
        @MaxQuantity("1M")
        public Quantity getConstrainedByMin()
        {
            return null;
        }
    }

    @SuppressWarnings("UnusedDeclaration")
    public static class BrokenMinAnnotation
    {
        @MinQuantity("broken")
        public Quantity getConstrainedByMin()
        {
            return new Quantity(32, THOUSAND);
        }
    }

    @SuppressWarnings("UnusedDeclaration")
    public static class BrokenMaxAnnotation
    {
        @MinQuantity("broken")
        public Quantity getConstrainedByMin()
        {
            return new Quantity(32, THOUSAND);
        }
    }
}
