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

package org.apache.ignite.examples.ml.mleap;

import java.io.File;
import java.util.HashMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import org.apache.ignite.Ignite;
import org.apache.ignite.Ignition;
import org.apache.ignite.internal.util.IgniteUtils;
import org.apache.ignite.ml.inference.Model;
import org.apache.ignite.ml.inference.builder.AsyncModelBuilder;
import org.apache.ignite.ml.inference.builder.IgniteDistributedModelBuilder;
import org.apache.ignite.ml.inference.reader.FileSystemModelReader;
import org.apache.ignite.ml.inference.reader.ModelReader;
import org.apache.ignite.ml.mleap.MLeapModelParser;

/**
 * This example demonstrates how to import MLeap model and use imported model for distributed inference in Apache
 * Ignite.
 */
public class MLeapModelParserExample {
    /** Test model resource name. */
    private static final String TEST_MODEL_RES = "examples/src/main/resources/models/mleap/scikit-airbnb.rf.zip";

    /** Parser. */
    private static final MLeapModelParser parser = new MLeapModelParser();

    /** Run example. */
    public static void main(String... args) throws ExecutionException, InterruptedException {
        try (Ignite ignite = Ignition.start("examples/config/example-ignite.xml")) {
            File mdlRsrc = IgniteUtils.resolveIgnitePath(TEST_MODEL_RES);
            if (mdlRsrc == null)
                throw new IllegalArgumentException("File not found [resource_path=" + TEST_MODEL_RES + "]");

            ModelReader reader = new FileSystemModelReader(mdlRsrc.getPath());

            AsyncModelBuilder mdlBuilder = new IgniteDistributedModelBuilder(ignite, 4, 4);

            try (Model<HashMap<String, Double>, Future<Double>> mdl = mdlBuilder.build(reader, parser)) {
                HashMap<String, Double> input = new HashMap<>();
                input.put("imp_bathrooms", 1.0);
                input.put("imp_bedrooms", 1.0);
                input.put("imp_security_deposit", 1.0);
                input.put("imp_cleaning_fee", 1.0);
                input.put("imp_extra_people", 1.0);
                input.put("imp_number_of_reviews", 1.0);
                input.put("imp_square_feet", 1.0);
                input.put("imp_review_scores_rating", 1.0);

                Future<Double> prediction = mdl.predict(input);

                System.out.println(prediction.get());
            }
        }
    }
}
