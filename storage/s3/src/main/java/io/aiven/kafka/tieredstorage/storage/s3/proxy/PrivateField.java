/*
 * Copyright 2024 Aiven Oy
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package io.aiven.kafka.tieredstorage.storage.s3.proxy;

import java.lang.reflect.Field;

class PrivateField<C, V> {
    private final C object;
    private final Field field;
    private final Class<V> valueClass;

    private PrivateField(
        final Class<C> declaringClass, final C object, final Class<V> valueClass, final String name
    ) throws ReflectiveOperationException {
        this.object = object;
        this.field = declaringClass.getDeclaredField(name);
        this.field.setAccessible(true);
        this.valueClass = valueClass;
    }

    V getValue() throws ReflectiveOperationException {
        return valueClass.cast(this.field.get(this.object));
    }

    void setValue(final V value) throws ReflectiveOperationException {
        this.field.set(this.object, value);
    }

    static <C, V> PrivateField<C, V> of(
        final Class<C> declaringClass, final C object, final Class<V> valueClass, final String name
    ) throws ReflectiveOperationException {
        return new PrivateField<>(declaringClass, object, valueClass, name);
    }
}
