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

package org.apache.ignite.ml.dataset.impl.local;

import java.io.Serializable;
import java.util.*;
import java.util.function.Function;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.apache.ignite.lang.IgniteBiPredicate;
import org.apache.ignite.ml.dataset.DatasetBuilder;
import org.apache.ignite.ml.dataset.PartitionContextBuilder;
import org.apache.ignite.ml.dataset.PartitionDataBuilder;
import org.apache.ignite.ml.dataset.UpstreamEntry;
import org.apache.ignite.ml.math.functions.IgniteBiFunction;
import org.apache.ignite.ml.math.functions.IgniteFunction;
import org.apache.ignite.ml.math.functions.IgniteSupplier;
import org.apache.ignite.ml.util.Utils;

/**
 * A dataset builder that makes {@link LocalDataset}. Encapsulate logic of building local dataset such as allocation
 * required data structures and initialization of {@code context} part of partitions.
 *
 * @param <K> Type of a key in {@code upstream} data.
 * @param <V> Type of a value in {@code upstream} data.
 */
public class LocalDatasetBuilder<K, V> implements DatasetBuilder<K, V> {
    /** {@code Map} with upstream data. */
    private final Map<K, V> upstreamMap;

    /** Number of partitions. */
    private final int partitions;

    /** Filter for {@code upstream} data. */
    private final IgniteBiPredicate<K, V> filter;

    private List<IgniteBiFunction<Stream<UpstreamEntry<K, V>>, ?, Stream<UpstreamEntry<K, V>>>> transformers;

    private List<IgniteSupplier<?>> transformerDataSuppliers;

    /**
     * Constructs a new instance of local dataset builder that makes {@link LocalDataset} with default predicate that
     * passes all upstream entries to dataset.
     *
     * @param upstreamMap {@code Map} with upstream data.
     * @param partitions Number of partitions.
     */
    public LocalDatasetBuilder(Map<K, V> upstreamMap, int partitions) {
        this(upstreamMap, (a, b) -> true, partitions);
    }

    /**
     * Constructs a new instance of local dataset builder that makes {@link LocalDataset}.
     *
     * @param upstreamMap {@code Map} with upstream data.
     * @param filter Filter for {@code upstream} data.
     * @param partitions Number of partitions.
     */
    public LocalDatasetBuilder(Map<K, V> upstreamMap, IgniteBiPredicate<K, V> filter, int partitions) {
        this.upstreamMap = upstreamMap;
        this.filter = filter;
        this.partitions = partitions;
        this.transformers = new LinkedList<>();
        this.transformerDataSuppliers = new LinkedList<>();
    }

    /** {@inheritDoc} */
    @SuppressWarnings("unchecked")
    @Override public <C extends Serializable, D extends AutoCloseable> LocalDataset<C, D> build(
        PartitionContextBuilder<K, V, C> partCtxBuilder, PartitionDataBuilder<K, V, C, D> partDataBuilder) {
        List<C> ctxList = new ArrayList<>();
        List<D> dataList = new ArrayList<>();

        // TODO: Do we have any constraint telling us that in upstream data key is unique?
        List<UpstreamEntry<K, V>> entriesList = new LinkedList<>();

        Stream<UpstreamEntry<K, V>> filteredStream = upstreamMap
            .entrySet()
            .stream()
            .filter(en -> filter.apply(en.getKey(), en.getValue()))
            .map(en -> new UpstreamEntry<>(en.getKey(), en.getValue()));

        List<?> suppliersData = transformerDataSuppliers.stream().map(Supplier::get).collect(Collectors.toList());

        transformStream(filteredStream, suppliersData)
            .forEach(entriesList::add);

        int partSize = Math.max(1, entriesList.size() / partitions);

        Iterator<UpstreamEntry<K, V>> firstKeysIter = entriesList.iterator();
        Iterator<UpstreamEntry<K, V>> secondKeysIter = entriesList.iterator();

        int ptr = 0;

        for (int part = 0; part < partitions; part++) {
            int cnt = part == partitions - 1 ? entriesList.size() - ptr : Math.min(partSize, entriesList.size() - ptr);

            IteratorWindow<UpstreamEntry<K, V>, UpstreamEntry<K, V>> iter = new IteratorWindow<>(
                    firstKeysIter, k -> k, cnt);
            C ctx = cnt > 0 ? partCtxBuilder.build(
                    Utils.asStream(iter),
                cnt
            ) : null;

            Iterator<UpstreamEntry<K, V>> iter1 = new IteratorWindow<>(
                    secondKeysIter, k -> k, cnt);
            Stream<UpstreamEntry<K, V>> tStream = Utils.asStream(iter1);
            D data = cnt > 0 ? partDataBuilder.build(
                    tStream,
                cnt,
                ctx
            ) : null;

            ctxList.add(ctx);
            dataList.add(data);

            ptr += cnt;
        }

        return new LocalDataset<>(ctxList, dataList);
    }

    @Override
    public <T> DatasetBuilder<K, V> addStreamTransformer(
        IgniteBiFunction<Stream<UpstreamEntry<K, V>>, T, Stream<UpstreamEntry<K, V>>> transformer,
        IgniteSupplier<T> transformerDataSupplier) {
        transformers.add(transformer);
        transformerDataSuppliers.add(transformerDataSupplier);

        return this;
    }

    /**  */
    @Override
    public <T> DatasetBuilder<K, V> addStreamTransformer(IgniteBiFunction<Stream<UpstreamEntry<K, V>>, T, Stream<UpstreamEntry<K, V>>> transformer) {
        return addStreamTransformer(transformer, () -> null);
    }

    /** {@inheritDoc} */
    @Override public DatasetBuilder<K, V> withFilter(IgniteBiPredicate<K, V> filterToAdd) {
        return new LocalDatasetBuilder<>(upstreamMap,
            (e1, e2) -> filter.apply(e1, e2) && filterToAdd.apply(e1, e2), partitions);
    }

    private Stream<UpstreamEntry<K, V>> transformStream(Stream<UpstreamEntry<K, V>> upstream, List<?> data) {
        assert transformers.size() == data.size();

        Iterator<?> dataSuppliersIter = data.iterator();

        Stream<UpstreamEntry<K, V>> res = upstream;

        for (IgniteBiFunction transformer : transformers) {
            Object d = dataSuppliersIter.next();
            res = (Stream<UpstreamEntry<K, V>>) transformer.apply(res, d);
        }

        return res;
    }

    /**
     * Utils class that wraps iterator so that it produces only specified number of entries and allows to transform
     * entries from one type to another.
     *
     * @param <K> Initial type of entries.
     * @param <T> Target type of entries.
     */
    private static class IteratorWindow<K, T> implements Iterator<T> {
        /** Delegate iterator. */
        private final Iterator<K> delegate;

        /** Transformer that transforms entries from one type to another. */
        private final IgniteFunction<K, T> map;

        /** Count of entries to produce. */
        private final int cnt;

        /** Number of already produced entries. */
        private int ptr;

        /**
         * Constructs a new instance of iterator window wrapper.
         *
         * @param delegate Delegate iterator.
         * @param map Transformer that transforms entries from one type to another.
         * @param cnt Count of entries to produce.
         */
        IteratorWindow(Iterator<K> delegate, IgniteFunction<K, T> map, int cnt) {
            this.delegate = delegate;
            this.map = map;
            this.cnt = cnt;
        }

        /** {@inheritDoc} */
        @Override public boolean hasNext() {
            return delegate.hasNext() && ptr < cnt;
        }

        /** {@inheritDoc} */
        @Override public T next() {
            ++ptr;

            return map.apply(delegate.next());
        }
    }
}
