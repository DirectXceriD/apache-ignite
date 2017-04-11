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

import org.apache.ignite.math.impls.vector.ConstantVector;
import org.apache.ignite.math.impls.vector.DenseLocalOnHeapVector;
import org.apache.ignite.math.Matrix;
import org.apache.ignite.math.Vector;
import org.apache.ignite.math.impls.storage.matrix.DiagonalMatrixStorage;
import org.apache.ignite.math.impls.vector.SingleElementVectorView;

/**
 * TODO: add description.
 */
public class DiagonalMatrix extends AbstractMatrix {
    /**
     *
     */
    public DiagonalMatrix() {
        // No-op.
    }

    /**
     *
     * @param diagonal
     */
    public DiagonalMatrix(Vector diagonal) {
        super(new DiagonalMatrixStorage(diagonal));
    }

    /**
     *
     * @param mtx
     */
    public DiagonalMatrix(Matrix mtx) {
        super(new DiagonalMatrixStorage(mtx == null ? null : mtx.viewDiagonal()));
    }

    /**
     *
     * @param vals
     */
    public DiagonalMatrix(double[] vals) {
        super(new DiagonalMatrixStorage(new DenseLocalOnHeapVector(vals)));
    }

    /**
     *
     *
     */
    private DiagonalMatrixStorage storage() {
        return (DiagonalMatrixStorage)getStorage();
    }

    /**
     *
     * @param size
     * @param val
     */
    public DiagonalMatrix(int size, double val) {
        super(new DiagonalMatrixStorage(new ConstantVector(size, val)));
    }

    @Override public Vector viewRow(int row) {
        return new SingleElementVectorView(storage().diagonal(), row);
    }

    @Override public Vector viewColumn(int col) {
        return new SingleElementVectorView(storage().diagonal(), col);
    }

    @Override public Matrix copy() {
        return new DiagonalMatrix(storage().diagonal());
    }

    @Override public Matrix like(int rows, int cols) {
        return storage().diagonal().likeMatrix(rows, cols);
    }

    @Override public Vector likeVector(int crd) {
        return storage().diagonal().like(crd);
    }
}
