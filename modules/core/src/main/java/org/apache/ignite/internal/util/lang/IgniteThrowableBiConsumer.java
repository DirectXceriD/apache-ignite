/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.ignite.internal.util.lang;

import java.io.Serializable;
import org.apache.ignite.IgniteCheckedException;

/**
 * Represents an operation that accepts a single input argument and returns no result. Unlike most other functional
 * interfaces, {@code IgniteThrowableConsumer} is expected to operate via side-effects.
 *
 * @param <E> Type of closure parameter.
 * @param <R> Type of result value.
 */
public interface IgniteThrowableBiConsumer<E, R> extends Serializable {
    /**
     * Consumer body.
     *
     * @param e Consumer parameter.
     * @return Result of consumer operation.
     * @throws IgniteCheckedException if body execution was failed.
     */
    public void accept(E e, R r) throws IgniteCheckedException;
}
