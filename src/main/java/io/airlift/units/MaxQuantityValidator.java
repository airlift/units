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

import javax.validation.ConstraintValidator;
import javax.validation.ConstraintValidatorContext;

public class MaxQuantityValidator
        implements ConstraintValidator<MaxQuantity, Quantity>
{
    private Quantity max;

    @Override
    public void initialize(MaxQuantity quantity)
    {
        this.max = Quantity.valueOf(quantity.value());
    }

    @Override
    public boolean isValid(Quantity quantity, ConstraintValidatorContext context)
    {
        return (quantity == null) || (quantity.compareTo(max) <= 0);
    }

    @Override
    public String toString()
    {
        return "max:" + max;
    }
}
