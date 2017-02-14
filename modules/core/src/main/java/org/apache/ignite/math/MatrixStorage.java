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
public interface MatrixStorage extends Externalizable, StorageOpsKinds {
    /**
     *
     * @param x
     * @param y
     * @return
     */
    public double get(int x, int y);

    /**
     *
     * @param x
     * @param y
     * @param v
     */
    public void set(int x, int y, double v);

    /**
     *
     * @return
     */
    public int columnSize();

    /**
     * 
     * @return
     */
    public int rowSize();

    /**
     * Gets underlying array if {@link StorageOpsKinds#isArrayBased()} returns {@code true}.
     *
     * @see StorageOpsKinds#isArrayBased()
     */
    public double[][] data();

    /**
     * Destroys matrix storage if managed outside of JVM. It's a no-op in all other cases.
     */
    default void destroy(){
        // No-op.
    }
}
