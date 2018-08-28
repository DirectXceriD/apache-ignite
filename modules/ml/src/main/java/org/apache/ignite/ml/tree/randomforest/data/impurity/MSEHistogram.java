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

package org.apache.ignite.ml.tree.randomforest.data.impurity;

import java.util.HashSet;
import java.util.Optional;
import java.util.Set;
import java.util.TreeMap;
import java.util.TreeSet;
import org.apache.ignite.ml.dataset.feature.BucketMeta;
import org.apache.ignite.ml.dataset.feature.ObjectHistogram;
import org.apache.ignite.ml.dataset.impl.bootstrapping.BootstrappedVector;
import org.apache.ignite.ml.tree.randomforest.data.NodeSplit;
import sun.reflect.generics.reflectiveObjects.NotImplementedException;

/**
 * Class contains implementation of splitting point finding algorithm based on MSE metric (see https://en.wikipedia.org/wiki/Mean_squared_error)
 * and represents a set of histograms in according to this metric.
 */
public class MSEHistogram implements ImpurityComputer<BootstrappedVector, MSEHistogram> {
    /** Serial version uid. */
    private static final long serialVersionUID = 9175485616887867623L;

    /** Bucket meta. */
    private final BucketMeta bucketMeta;

    /** Feature id. */
    private final int featureId;

    /** Sample id. */
    private final int sampleId;

    /** Bucket ids. */
    private final Set<Integer> bucketIds;

    /** Counters. */
    private ObjectHistogram<BootstrappedVector> counters;

    /** Sums of label values. */
    private ObjectHistogram<BootstrappedVector> sumOfLabels;

    /** Sums of squared label values. */
    private ObjectHistogram<BootstrappedVector> sumOfSquaredLabels;

    /**
     * Creates an instance of MSEHistogram.
     *
     * @param sampleId Sample id.
     * @param bucketMeta Bucket meta.
     */
    public MSEHistogram(int sampleId, BucketMeta bucketMeta) {
        this.bucketMeta = bucketMeta;
        this.featureId = bucketMeta.getFeatureMeta().getFeatureId();
        this.sampleId = sampleId;

        counters = new ObjectHistogram<>(this::bucketMap, this::counterMap);
        sumOfLabels = new ObjectHistogram<>(this::bucketMap, this::ysMap);
        sumOfSquaredLabels = new ObjectHistogram<>(this::bucketMap, this::y2sMap);
        bucketIds = new TreeSet<>();
    }

    /** {@inheritDoc} */
    @Override public void addElement(BootstrappedVector vector) {
        counters.addElement(vector);
        sumOfLabels.addElement(vector);
        sumOfSquaredLabels.addElement(vector);
    }

    /** {@inheritDoc} */
    @Override public MSEHistogram plus(MSEHistogram other) {
        MSEHistogram res = new MSEHistogram(sampleId, bucketMeta);
        res.counters = this.counters.plus(other.counters);
        res.sumOfLabels = this.sumOfLabels.plus(other.sumOfLabels);
        res.sumOfSquaredLabels = this.sumOfSquaredLabels.plus(other.sumOfSquaredLabels);
        res.bucketIds.addAll(this.bucketIds);
        res.bucketIds.addAll(bucketIds);
        return res;
    }

    /** {@inheritDoc} */
    @Override public Set<Integer> buckets() {
        return bucketIds;
    }

    /** {@inheritDoc} */
    @Override public Optional<Double> getValue(Integer bucketId) {
        throw new NotImplementedException();
    }

    /** {@inheritDoc} */
    @Override public Optional<NodeSplit> findBestSplit() {
        double bestImpurity = Double.POSITIVE_INFINITY;
        double bestSplitValue = Double.NEGATIVE_INFINITY;
        int bestBucketId = -1;

        TreeMap<Integer, Double> counterDistrib = counters.computeDistributionFunction();
        TreeMap<Integer, Double> ysDistrib = sumOfLabels.computeDistributionFunction();
        TreeMap<Integer, Double> y2sDistrib = sumOfSquaredLabels.computeDistributionFunction();

        double cntrMax = counterDistrib.lastEntry().getValue();
        double ysMax = ysDistrib.lastEntry().getValue();
        double y2sMax = y2sDistrib.lastEntry().getValue();

        double lastLeftCntrVal = 0.0;
        double lastLeftYVal = 0.0;
        double lastLeftY2Val = 0.0;

        for (Integer bucketId : bucketIds) {
            double leftCnt = counterDistrib.getOrDefault(bucketId, lastLeftCntrVal);
            double rightCnt = cntrMax - leftCnt;
            double leftY = ysDistrib.getOrDefault(bucketId, lastLeftYVal);
            double rightY = ysMax - leftY;
            double leftY2 = y2sDistrib.getOrDefault(bucketId, lastLeftY2Val);
            double rightY2 = y2sMax - leftY2;

            double impurity = 0.0;

            if (leftCnt > 0)
                impurity += impurity(leftCnt, leftY, leftY2);
            if (rightCnt > 0)
                impurity += impurity(rightCnt, rightY, rightY2);

            if (impurity < bestImpurity) {
                bestImpurity = impurity;
                bestSplitValue = bucketMeta.bucketIdToValue(bucketId);
                bestBucketId = bucketId;
            }
        }

        int minBucketId = Integer.MAX_VALUE;
        int maxBucketId = Integer.MIN_VALUE;
        for (Integer bucketId : bucketIds) {
            minBucketId = Math.min(minBucketId, bucketId);
            maxBucketId = Math.max(maxBucketId, bucketId);
        }

        if (bestBucketId == minBucketId || bestBucketId == maxBucketId)
            return Optional.empty();
        else
            return Optional.of(new NodeSplit(featureId, bestSplitValue, bestImpurity));
    }

    /**
     * Computes impurity function value.
     *
     * @param count Counter value.
     * @param ys plus of Ys.
     * @param y2s plus of Y^2 s.
     * @return impurity value.
     */
    private double impurity(double count, double ys, double y2s) {
        return y2s - 2.0 * ys / count * ys + Math.pow(ys / count, 2) * count;
    }

    /**
     * Maps vector to bucket id.
     *
     * @param vec Vector.
     * @return Bucket id.
     */
    private Integer bucketMap(BootstrappedVector vec) {
        int bucketId = bucketMeta.getBucketId(vec.features().get(featureId));
        this.bucketIds.add(bucketId);
        return bucketId;
    }

    /**
     * Maps vector to counter value.
     *
     * @param vec Vector.
     * @return Counter value.
     */
    private Double counterMap(BootstrappedVector vec) {
        return (double)vec.counters()[sampleId];
    }

    /**
     * Maps vector to Y-value.
     *
     * @param vec Vector.
     * @return Y value.
     */
    private Double ysMap(BootstrappedVector vec) {
        return vec.counters()[sampleId] * vec.label();
    }

    /**
     * Maps vector to Y^2 value.
     *
     * @param vec Vec.
     * @return Y^2 value.
     */
    private Double y2sMap(BootstrappedVector vec) {
        return vec.counters()[sampleId] * Math.pow(vec.label(), 2);
    }

    /**
     * @return Counters histogram.
     */
    ObjectHistogram<BootstrappedVector> getCounters() {
        return counters;
    }

    /**
     * @return Ys histogram.
     */
    ObjectHistogram<BootstrappedVector> getSumOfLabels() {
        return sumOfLabels;
    }

    /**
     * @return Y^2s histogram.
     */
    ObjectHistogram<BootstrappedVector> getSumOfSquaredLabels() {
        return sumOfSquaredLabels;
    }

    /** {@inheritDoc} */
    @Override public boolean isEqualTo(MSEHistogram other) {
        HashSet<Integer> unionBuckets = new HashSet<>(buckets());
        unionBuckets.addAll(other.bucketIds);
        if(unionBuckets.size() != bucketIds.size())
            return false;

        if(!this.counters.isEqualTo(other.counters))
            return false;
        if(!this.sumOfLabels.isEqualTo(other.sumOfLabels))
            return false;

        return this.sumOfSquaredLabels.isEqualTo(other.sumOfSquaredLabels);
    }
}
