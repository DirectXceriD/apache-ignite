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

package org.apache.ignite.examples.ml.inference.spark.modelparser;

import java.io.FileNotFoundException;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.Ignition;
import org.apache.ignite.examples.ml.tutorial.TitanicUtils;
import org.apache.ignite.ml.composition.ModelsComposition;
import org.apache.ignite.ml.math.functions.IgniteBiFunction;
import org.apache.ignite.ml.math.primitives.vector.Vector;
import org.apache.ignite.ml.math.primitives.vector.VectorUtils;
import org.apache.ignite.ml.selection.scoring.evaluator.BinaryClassificationEvaluator;
import org.apache.ignite.ml.selection.scoring.metric.Accuracy;
import org.apache.ignite.ml.sparkmodelparser.SparkModelParser;
import org.apache.ignite.ml.sparkmodelparser.SupportedSparkModels;

/**
 * Run Gradient Boosted trees model loaded from snappy.parquet file.
 * The snappy.parquet file was generated by Spark MLLib model.write.overwrite().save(..) operator.
 * <p>
 * You can change the test data used in this example and re-run it to explore this algorithm further.</p>
 */
public class GBTFromSparkExample {
    /** Path to Spark LogReg model. */
    public static final String SPARK_MDL_PATH = "examples/src/main/resources/models/spark/serialized/gbt/data" +
        "/part-00000-ea23dcda-6344-4b1f-9716-fbedf7caba2d-c000.snappy.parquet";

    /** Spark model metadata path. */
    private static final String SPARK_MDL_METADATA_PATH = "examples/src/main/resources/models/spark/serialized/gbt/treesMetadata" +
        "/part-00000-9033203a-e1e6-4d24-9900-be8a4396710b-c000.snappy.parquet";

    /** Run example. */
    public static void main(String[] args) throws FileNotFoundException {
        System.out.println();
        System.out.println(">>> Gradient Boosted trees model loaded from Spark through serialization over partitioned dataset usage example started.");
        // Start ignite grid.
        try (Ignite ignite = Ignition.start("examples/config/example-ignite.xml")) {
            System.out.println(">>> Ignite grid started.");

            IgniteCache<Integer, Object[]> dataCache = TitanicUtils.readPassengers(ignite);

            IgniteBiFunction<Integer, Object[], Vector> featureExtractor = (k, v) -> {
                double[] data = new double[] {(double)v[0], (double)v[5], (double)v[6]};
                data[0] = Double.isNaN(data[0]) ? 0 : data[0];
                data[1] = Double.isNaN(data[1]) ? 0 : data[1];
                data[2] = Double.isNaN(data[2]) ? 0 : data[2];

                return VectorUtils.of(data);
            };

            IgniteBiFunction<Integer, Object[], Double> lbExtractor = (k, v) -> (double)v[1];

            ModelsComposition mdl = (ModelsComposition)SparkModelParser.parseWithMetadata(
                SPARK_MDL_PATH, SPARK_MDL_METADATA_PATH,
                SupportedSparkModels.GRADIENT_BOOSTED_TREES
            );

            System.out.println(">>> GBT: " + mdl.toString(true));

            double accuracy = BinaryClassificationEvaluator.evaluate(
                dataCache,
                mdl,
                featureExtractor,
                lbExtractor,
                new Accuracy<>()
            );

            System.out.println("\n>>> Accuracy " + accuracy);
            System.out.println("\n>>> Test Error " + (1 - accuracy));
        }
    }
}
