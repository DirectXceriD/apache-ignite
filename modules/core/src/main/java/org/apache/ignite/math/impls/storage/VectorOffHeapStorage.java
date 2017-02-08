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

package org.apache.ignite.math.impls.storage;

import org.apache.ignite.math.*;
import java.io.*;

/**
 * TODO: add description.
 */
public class VectorOffHeapStorage implements VectorStorage {
    @Override
    public int size() {
        return 0; // TODO
    }

    @Override
    public double get(int i) {
        return 0; // TODO
    }

    @Override
    public void set(int i, double v) {
        // TODO
    }

    @Override
    public boolean isArrayBased() {
        return false; // TODO
    }

    @Override
    public double[] data() {
        return new double[0]; // TODO
    }

    @Override
    public boolean isSequentialAccess() {
        return false; // TODO
    }

    @Override
    public boolean isDense() {
        return false; // TODO
    }

    @Override
    public double getLookupCost() {
        return 0; // TODO
    }

    @Override
    public boolean isAddConstantTime() {
        return false; // TODO
    }

    @Override
    public void writeExternal(ObjectOutput out) throws IOException {
        // TODO
    }

    @Override
    public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
        // TODO
    }
}
