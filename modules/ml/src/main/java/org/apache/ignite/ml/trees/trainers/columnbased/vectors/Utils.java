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

package org.apache.ignite.ml.trees.trainers.columnbased.vectors;

import com.zaxxer.sparsebits.SparseBitSet;
import org.apache.ignite.lang.IgniteBiTuple;

public class Utils {
    /** */
    public static IgniteBiTuple<SampleInfo[], SampleInfo[]> splitByBitSet(int lSize, int rSize, SampleInfo[] samples, SparseBitSet bs) {
        SampleInfo lArr[] = new SampleInfo[lSize];
        SampleInfo rArr[] = new SampleInfo[rSize];

        int lc = 0;
        int rc = 0;

        for (int i = 0; i < lSize + rSize; i++) {
            SampleInfo fi = samples[i];

            if (bs.get(fi.sampleInd())) {
                lArr[lc] = fi;
                lc++;
            } else {
                rArr[rc] = fi;
                rc++;
            }
        }

        return new IgniteBiTuple<>(lArr, rArr);
    }
}
