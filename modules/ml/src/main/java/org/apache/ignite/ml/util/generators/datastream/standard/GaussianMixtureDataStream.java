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

package org.apache.ignite.ml.util.generators.datastream.standard;

import java.util.ArrayList;
import java.util.List;
import java.util.stream.IntStream;
import java.util.stream.Stream;
import org.apache.ignite.ml.math.functions.IgniteFunction;
import org.apache.ignite.ml.math.primitives.vector.Vector;
import org.apache.ignite.ml.structures.LabeledVector;
import org.apache.ignite.ml.util.Utils;
import org.apache.ignite.ml.util.generators.datastream.DataStreamGenerator;
import org.apache.ignite.ml.util.generators.datastream.RandomVectorsGenerator;
import org.apache.ignite.ml.util.generators.function.ParametricVectorGenerator;
import org.apache.ignite.ml.util.generators.variable.DiscreteRandomProducer;
import org.apache.ignite.ml.util.generators.variable.GaussRandomProducer;
import org.apache.ignite.ml.util.generators.variable.UniformRandomProducer;

public class GaussianMixtureDataStream implements DataStreamGenerator {
    public static class Builder {
        private final List<Vector> means = new ArrayList<>();
        private final List<Double> variances = new ArrayList<>();

        public Builder add(Vector mean, double variance) {
            means.add(mean);
            variances.add(variance);
            return this;
        }

        public GaussianMixtureDataStream build() {
            Vector[] means = new Vector[this.means.size()];
            double[] variances = new double[this.variances.size()];
            for(int i = 0; i<this.means.size(); i++) {
                means[i] = this.means.get(i);
                variances[i] = this.variances.get(i);
            }

            return new GaussianMixtureDataStream(means, variances);
        }
    }

    private final Vector[] points;
    private final double[] variances;

    private GaussianMixtureDataStream(Vector[] points, double[] variances) {
        this.points = points;
        this.variances = variances;
    }

    @Override public Stream<LabeledVector<Vector, Double>> labeled() {
        DiscreteRandomProducer selector = new DiscreteRandomProducer(Stream.generate(() -> 1.0 / points.length)
            .mapToDouble(x -> x).limit(points.length).toArray());
        List<ParametricVectorGenerator> generators = new ArrayList<>();
        long seed = System.currentTimeMillis();
        for (int i = 0; i < points.length; i++) {
            IgniteFunction<Double, Double>[] dimGenerators = new IgniteFunction[points[0].size()];
            for (int j = 0; j < points[0].size(); j++) {
                seed = seed >> 2;
                final int fi = i;
                final int fj = j;
                final GaussRandomProducer producer = new GaussRandomProducer(0.0, variances[fi], seed);
                dimGenerators[fj] = t -> points[fi].get(fj) + producer.get();
            }
            generators.add(new ParametricVectorGenerator(dimGenerators));
        }

        RandomVectorsGenerator vectorsGenerator = new RandomVectorsGenerator(generators, selector, UniformRandomProducer.zero());
        return Utils.asStream(vectorsGenerator).map(v -> new LabeledVector<>(v.vector(), (double)v.distributionFamilyId()));
    }
}
