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

import com.google.common.truth.FailureStrategy;
import com.google.common.truth.Subject;
import com.google.common.truth.SubjectFactory;

import javax.validation.ConstraintValidator;
import javax.validation.ConstraintValidatorContext;

import java.lang.annotation.Annotation;

import static com.google.common.truth.Truth.assertAbout;

final class ConstraintValidatorSubject<A extends Annotation, T>
        extends Subject<ConstraintValidatorSubject<A, T>, ConstraintValidator<A, T>>
{
    public ConstraintValidatorSubject(FailureStrategy failureStrategy, ConstraintValidator<A, T> actual)
    {
        super(failureStrategy, actual);
    }

    public static <A extends Annotation, T> ConstraintValidatorSubject<A, T> assertThat(ConstraintValidator<A, T> actual)
    {
        return assertAbout(new SubjectFactory<ConstraintValidatorSubject<A, T>, ConstraintValidator<A, T>>()
        {
            @Override
            public ConstraintValidatorSubject<A, T> getSubject(FailureStrategy failureStrategy, ConstraintValidator<A, T> target)
            {
                return new ConstraintValidatorSubject<>(failureStrategy, target);
            }
        }).that(actual);
    }

    public void isValidFor(T value)
    {
        if (!actual().isValid(value, new MockContext())) {
            fail("is valid for", value);
        }
    }

    public void isInvalidFor(T value)
    {
        if (actual().isValid(value, new MockContext())) {
            fail("is invalid for", value);
        }
    }

    private static class MockContext
            implements ConstraintValidatorContext
    {
        @Override
        public void disableDefaultConstraintViolation()
        {
            throw new UnsupportedOperationException();
        }

        @Override
        public String getDefaultConstraintMessageTemplate()
        {
            throw new UnsupportedOperationException();
        }

        @Override
        public ConstraintViolationBuilder buildConstraintViolationWithTemplate(String s)
        {
            throw new UnsupportedOperationException();
        }

        @Override
        public <T> T unwrap(Class<T> type)
        {
            throw new UnsupportedOperationException();
        }
    }
}
