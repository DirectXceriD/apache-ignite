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

package org.apache.ignite.math;

import java.io.*;

/**
 * TODO: add description.
 */
public interface VectorStorage extends Externalizable {
    /**
     *
     * @return
     */
    public int size();

    /**
     *
     * @param i
     * @return
     */
    public double get(int i);

    /**
     *
     * @return
     */
    public boolean isSequentialAccess();

    /**
     *
     * @return
     */
    public boolean isDense();

    /**
     *
     * @return
     */
    public double getLookupCost();

    /**
     * 
     * @return
     */
    public boolean isAddConstantTime();

    /**
     * 
     * @param i
     * @param v
     */
    public void set(int i, double v);

    /**
     * Tests whether or not the implementation is based on local on-heap array. Can be used
     * for performance optimizations.
     *
     * @see #data()
     */
    public boolean isArrayBased();

    /**
     * Gets underlying array if {@link #isArrayBased()} returns {@code true}.
     *
     * @see #isArrayBased()
     */
    public double[] data();

    /**
     * Destroy off heap vector storage, in on heap case do nothing.
     */
    public default void destroy() {

    }
}
