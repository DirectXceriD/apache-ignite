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

package org.apache.ignite.math.impls.matrix;

import java.io.*;
import org.apache.ignite.math.Matrix;
import org.apache.ignite.math.Vector;
import org.apache.ignite.math.MatrixStorage;
import org.apache.ignite.math.impls.storage.matrix.RandomMatrixStorage;
import org.apache.ignite.math.impls.vector.RandomVector;

/**
 * TODO: add description.
 */
public class RandomMatrix extends AbstractMatrix {
    private boolean fastHash;

    /**
     * @param rows
     * @oaram cols
     * @param fastHash
     */
    private MatrixStorage mkStorage(int rows, int cols, boolean fastHash) {
        this.fastHash = fastHash;

        return new RandomMatrixStorage(rows, cols, fastHash);
    }

    /**
     *
     * @param rows
     * @param cols
     * @param fastHash
     */
    public RandomMatrix(int rows, int cols, boolean fastHash) {
        setStorage(mkStorage(rows, cols, fastHash));
    }

    /**
     *
     * @param rows
     * @param cols
     */
    public RandomMatrix(int rows, int cols) {
        this(rows, cols, true);
    }

    /** */
    public RandomMatrix() {
        // No-op.
    }

    /** {@inheritDoc} */
    @Override public Matrix copy() {
        return new RandomMatrix(rowSize(), columnSize(), fastHash);
    }

    /** {@inheritDoc} */
    @Override public Matrix like(int rows, int cols) {
        return new RandomMatrix(rows, cols);
    }

    /** {@inheritDoc} */
    @Override public Vector likeVector(int crd) {
        return new RandomVector(crd);
    }

    /** {@inheritDoc} */
    @Override public void writeExternal(ObjectOutput out) throws IOException {
        super.writeExternal(out);

        out.writeBoolean(fastHash);
    }

    /** {@inheritDoc} */
    @Override public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
        super.readExternal(in);

        fastHash = in.readBoolean();
    }
}
