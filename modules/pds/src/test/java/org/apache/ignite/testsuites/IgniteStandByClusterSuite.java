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

package org.apache.ignite.testsuites;

import junit.framework.TestSuite;
import org.apache.ignite.cache.database.standbycluster.IgniteChangeGlobalStateCacheTest;
import org.apache.ignite.cache.database.standbycluster.IgniteChangeGlobalStateDataStreamerTest;
import org.apache.ignite.cache.database.standbycluster.IgniteChangeGlobalStateDataStructureTest;
import org.apache.ignite.cache.database.standbycluster.IgniteChangeGlobalStateFailOverTest;
import org.apache.ignite.cache.database.standbycluster.IgniteChangeGlobalStateTest;

/**
 *
 */
public class IgniteStandByClusterSuite extends TestSuite {
    /**
     *
     */
    public static TestSuite suite() {
        TestSuite suite = new TestSuite("Ignite Activate/DeActivate Cluster Test Suit");

        suite.addTestSuite(IgniteChangeGlobalStateTest.class);
        suite.addTestSuite(IgniteChangeGlobalStateCacheTest.class);
        suite.addTestSuite(IgniteChangeGlobalStateDataStructureTest.class);
        suite.addTestSuite(IgniteChangeGlobalStateDataStreamerTest.class);
        suite.addTestSuite(IgniteChangeGlobalStateFailOverTest.class);
//        suite.addTestSuite(IgniteChangeGlobalStateServiceTest.class);

        return suite;
    }
}
