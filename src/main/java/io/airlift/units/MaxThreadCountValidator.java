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

import jakarta.validation.ConstraintValidator;
import jakarta.validation.ConstraintValidatorContext;

public class MaxThreadCountValidator
        implements ConstraintValidator<MaxThreadCount, ThreadCount>
{
    private ThreadCount max;

    @Override
    public void initialize(MaxThreadCount annotation)
    {
        this.max = ThreadCount.valueOf(annotation.value());
    }

    @Override
    public boolean isValid(ThreadCount threadCount, ConstraintValidatorContext constraintValidatorContext)
    {
        return threadCount == null || threadCount.compareTo(max) <= 0;
    }

    @Override
    public String toString()
    {
        return "max: " + max;
    }
}
