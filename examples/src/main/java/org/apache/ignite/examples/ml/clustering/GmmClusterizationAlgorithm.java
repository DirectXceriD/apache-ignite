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

package org.apache.ignite.examples.ml.clustering;

import java.io.FileNotFoundException;
import javax.cache.Cache;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.Ignition;
import org.apache.ignite.cache.query.QueryCursor;
import org.apache.ignite.cache.query.ScanQuery;
import org.apache.ignite.ml.clustering.gmm.GmmModel;
import org.apache.ignite.ml.clustering.gmm.GmmTrainer;
import org.apache.ignite.ml.math.Tracer;
import org.apache.ignite.ml.math.primitives.vector.Vector;
import org.apache.ignite.ml.util.MLSandboxDatasets;
import org.apache.ignite.ml.util.SandboxMLCache;

public class GmmClusterizationAlgorithm {
    public static void main(String[] args) throws FileNotFoundException {
        System.out.println();
        System.out.println(">>> KMeans clustering algorithm over cached dataset usage example started.");
        // Start ignite grid.
        try (Ignite ignite = Ignition.start("examples/config/example-ignite.xml")) {
            System.out.println(">>> Ignite grid started.");

            IgniteCache<Integer, Vector> dataCache = new SandboxMLCache(ignite)
                .fillCacheWith(MLSandboxDatasets.TWO_CLASSED_IRIS);

            GmmTrainer trainer = new GmmTrainer(2);

            GmmModel mdl = trainer.fit(
                ignite,
                dataCache,
                (k, v) -> v.copyOfRange(1, v.size()),
                (k, v) -> v.get(0)
            );

            System.out.println(">>> KMeans centroids");
            Tracer.showAscii(mdl.distributions().get(0).mean());
            Tracer.showAscii(mdl.distributions().get(1).mean());
            System.out.println(">>>");

            System.out.println(">>> --------------------------------------------");
            System.out.println(">>> | Predicted cluster\t| Erased class label\t|");
            System.out.println(">>> --------------------------------------------");

            try (QueryCursor<Cache.Entry<Integer, Vector>> observations = dataCache.query(new ScanQuery<>())) {
                for (Cache.Entry<Integer, Vector> observation : observations) {
                    Vector val = observation.getValue();
                    Vector inputs = val.copyOfRange(1, val.size());
                    double groundTruth = val.get(0);

                    double prediction = mdl.predict(inputs);

                    System.out.printf(">>> | %.4f\t\t\t| %.4f\t\t|\n", prediction, groundTruth);
                }

                System.out.println(">>> ---------------------------------");
                System.out.println(">>> KMeans clustering algorithm over cached dataset usage example completed.");
            }
        }
    }
}
