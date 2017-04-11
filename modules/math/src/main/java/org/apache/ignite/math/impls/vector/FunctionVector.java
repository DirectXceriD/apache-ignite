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

package org.apache.ignite.math.impls.vector;

import org.apache.ignite.math.*;
import org.apache.ignite.math.Vector;
import org.apache.ignite.math.exceptions.UnsupportedOperationException;
import org.apache.ignite.math.functions.IgniteFunction;
import org.apache.ignite.math.functions.IntDoubleToVoidFunction;
import org.apache.ignite.math.impls.storage.vector.FunctionVectorStorage;

import java.util.*;

/**
 * TODO: add description.
 */
public class FunctionVector extends AbstractVector {
    /**
     *
     */
    public FunctionVector() {
        // No-op.
    }

    /**
     * Creates read-write or read-only function vector.
     * @param size
     * @param getFunc
     * @param setFunc Set function. If {@code null} - this will be a read-only vector.
     */
    public FunctionVector(int size, IgniteFunction<Integer, Double> getFunc, IntDoubleToVoidFunction setFunc) {
        setStorage(new FunctionVectorStorage(size, getFunc, setFunc));
    }

    /**
     * Creates read-only function vector.
     *
     * @param size
     * @param getFunc
     */
    public FunctionVector(int size, IgniteFunction<Integer, Double> getFunc) {
        setStorage(new FunctionVectorStorage(size, getFunc));
    }

    /**
     * @param args
     */
    public FunctionVector(Map<String, Object> args) {
        assert args != null;

        if (args.containsKey("size") && args.containsKey("getFunc") && args.containsKey("setFunc")) {
            IgniteFunction<Integer, Double> getFunc = (IgniteFunction<Integer, Double>)args.get("getFunc");
            IntDoubleToVoidFunction setFunc = (IntDoubleToVoidFunction)args.get("setFunc");
            int size = (int)args.get("size");

            setStorage(new FunctionVectorStorage(size, getFunc, setFunc));
        }
        else if (args.containsKey("size") && args.containsKey("getFunc")) {
            IgniteFunction<Integer, Double> getFunc = (IgniteFunction<Integer, Double>)args.get("getFunc");
            int size = (int)args.get("size");

            setStorage(new FunctionVectorStorage(size, getFunc));
        }
        else
            throw new UnsupportedOperationException("Invalid constructor argument(s).");
    }

    /**
     *
     *
     */
    private FunctionVectorStorage storage() {
        return (FunctionVectorStorage)getStorage();
    }

    /** {@inheritDoc} */
    @Override public org.apache.ignite.math.Vector like(int crd) {
        FunctionVectorStorage sto = storage();

        return new FunctionVector(crd, sto.getFunction(), sto.setFunction());
    }

    /** {@inheritDoc} */
    @Override public Matrix likeMatrix(int rows, int cols) {
        throw new UnsupportedOperationException();
    }

    /** {@inheritDoc} */
    @Override public Vector times(double x) {
        if (x == 0.0)
            return like(size()).assign(0);
        else
            return super.times(x);
    }
}
