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

package org.apache.ignite.ml.util.generators.primitives.variable;

import org.apache.ignite.internal.util.typedef.internal.A;

public class GaussRandomProducer extends RandomProducerWithGenerator {
    private final double mean;
    private final double variance;

    public GaussRandomProducer() {
        this(0.0, 1.0, System.currentTimeMillis());
    }

    public GaussRandomProducer(long seed) {
        this(0.0, 1.0, seed);
    }

    public GaussRandomProducer(double mean, double variance) {
        this(mean, variance, System.currentTimeMillis());
    }

    public GaussRandomProducer(double mean, double variance, long seed) {
        super(seed);

        A.ensure(variance > 0, "variance > 0");

        this.mean = mean;
        this.variance = variance;
    }

    @Override public Double get() {
        return mean + generator().nextGaussian() * Math.sqrt(variance);
    }
}
