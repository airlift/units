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

public class MinCountValidator
        implements ConstraintValidator<MinCount, Count>
{
    private Count min;

    @Override
    public void initialize(MinCount dataSize)
    {
        this.min = Count.valueOf(dataSize.value());
    }

    @Override
    public boolean isValid(Count count, ConstraintValidatorContext context)
    {
        return (count == null) || (count.compareTo(min) >= 0);
    }

    @Override
    public String toString()
    {
        return "min:" + min;
    }
}
