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

package org.apache.ignite.examples.ml.tree;

import java.util.List;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.Ignition;
import org.apache.ignite.binary.BinaryObject;
import org.apache.ignite.cache.affinity.rendezvous.RendezvousAffinityFunction;
import org.apache.ignite.cache.query.QueryCursor;
import org.apache.ignite.cache.query.SqlFieldsQuery;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.internal.util.IgniteUtils;
import org.apache.ignite.ml.dataset.impl.cache.CacheBasedDatasetBuilder;
import org.apache.ignite.ml.math.primitives.vector.Vector;
import org.apache.ignite.ml.math.primitives.vector.VectorUtils;
import org.apache.ignite.ml.tree.DecisionTreeClassificationTrainer;
import org.apache.ignite.ml.tree.DecisionTreeNode;

/**
 * Example of using distributed {@link DecisionTreeClassificationTrainer} on a data stored in SQL table.
 */
public class DecisionTreeClassificationTrainerSQLTableExample {
    /** Dummy cache name. */
    private static final String DUMMY_CACHE_NAME = "dummy_cache";

    /** Training data. */
    private static final String TRAIN_DATA_RES = "examples/src/main/resources/datasets/titanik_train.csv";

    /** Test data. */
    private static final String TEST_DATA_RES = "examples/src/main/resources/datasets/titanik_test.csv";

    /** Run example. */
    public static void main(String[] args) {
        System.out.println(">>> Decision tree classification trainer example started.");

        // Start ignite grid.
        try (Ignite ignite = Ignition.start("examples/config/example-ignite.xml")) {
            System.out.println(">>> Ignite grid started.");

            // Dummy cache is required to perform SQL queries.
            CacheConfiguration<?, ?> cacheCfg = new CacheConfiguration<>(DUMMY_CACHE_NAME)
                .setSqlSchema("PUBLIC");

            IgniteCache<?, ?> cache = ignite.createCache(cacheCfg);

            System.out.println(">>> Creating table with training data...");
            cache.query(new SqlFieldsQuery("create table titanik_train (\n" +
                "    passengerid int primary key,\n" +
                "    survived int,\n" +
                "    pclass int,\n" +
                "    name varchar(255),\n" +
                "    sex varchar(255),\n" +
                "    age float,\n" +
                "    sibsp int,\n" +
                "    parch int,\n" +
                "    ticket varchar(255),\n" +
                "    fare float,\n" +
                "    cabin varchar(255),\n" +
                "    embarked varchar(255)\n" +
                ") with \"template=partitioned\";")).getAll();

            System.out.println(">>> Filling training data...");
            cache.query(new SqlFieldsQuery("insert into titanik_train select * from csvread('" +
                IgniteUtils.resolveIgnitePath(TRAIN_DATA_RES).getAbsolutePath() + "')")).getAll();

            System.out.println(">>> Creating table with test data...");
            cache.query(new SqlFieldsQuery("create table titanik_test (\n" +
                "    passengerid int primary key,\n" +
                "    pclass int,\n" +
                "    name varchar(255),\n" +
                "    sex varchar(255),\n" +
                "    age float,\n" +
                "    sibsp int,\n" +
                "    parch int,\n" +
                "    ticket varchar(255),\n" +
                "    fare float,\n" +
                "    cabin varchar(255),\n" +
                "    embarked varchar(255)\n" +
                ") with \"template=partitioned\";")).getAll();

            System.out.println(">>> Filling training data...");
            cache.query(new SqlFieldsQuery("insert into titanik_test select * from csvread('" +
                IgniteUtils.resolveIgnitePath(TEST_DATA_RES).getAbsolutePath() + "')")).getAll();

            System.out.println(">>> Prepare trainer...");
            DecisionTreeClassificationTrainer trainer = new DecisionTreeClassificationTrainer(4, 0);

            System.out.println(">>> Perform training...");
            IgniteCache<Integer, BinaryObject> titanicTrainCache = ignite.cache("SQL_PUBLIC_TITANIK_TRAIN");
            DecisionTreeNode mdl = trainer.fit(
                // We have to specify ".withKeepBinary(true)" because SQL caches contains only binary objects and this
                // information has to be passed into the trainer.
                new CacheBasedDatasetBuilder<>(ignite, titanicTrainCache).withKeepBinary(true),
                (k, v) -> VectorUtils.of(
                    // We have to handle null values here to avoid NpE during unboxing.
                    replaceNull(v.<Integer>field("pclass")),
                    "male".equals(v.<String>field("sex")) ? 1 : 0,
                    replaceNull(v.<Double>field("age")),
                    replaceNull(v.<Integer>field("sibsp")),
                    replaceNull(v.<Integer>field("parch")),
                    replaceNull(v.<Double>field("fare"))
                ),
                (k, v) -> replaceNull(v.<Integer>field("survived"))
            );

            System.out.println(">>> Perform inference...");
            try (QueryCursor<List<?>> cursor = cache.query(new SqlFieldsQuery("select " +
                "pclass, " +
                "sex, " +
                "age, " +
                "sibsp, " +
                "parch, " +
                "fare from titanik_test"))) {
                for (List<?> passenger : cursor) {
                    Vector input = VectorUtils.of(
                        // We have to handle null values here to avoid NpE during unboxing.
                        replaceNull((Integer)passenger.get(0)),
                        "male".equals(passenger.get(1)) ? 1 : 0,
                        replaceNull((Double)passenger.get(2)),
                        replaceNull((Integer)passenger.get(3)),
                        replaceNull((Integer)passenger.get(4)),
                        replaceNull((Double)passenger.get(5))
                    );

                    double prediction = mdl.predict(input);

                    System.out.printf("Passenger %s will %s.\n", passenger, prediction == 0 ? "die" : "survive");
                }
            }

            System.out.println(">>> Example completed.");
        }
    }

    /**
     * Replaces NULL values by 0.
     *
     * @param obj Input value.
     * @param <T> Type of value.
     * @return Input value of 0 if value is null.
     */
    private static <T extends Number> double replaceNull(T obj) {
        if (obj == null)
            return 0;

        return obj.doubleValue();
    }
}
