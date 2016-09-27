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

package org.apache.ignite.internal.processors.cache;

import org.apache.ignite.internal.binary.BinaryMarshaller;
import org.apache.ignite.internal.processors.cache.binary.CacheObjectBinaryProcessor;
import org.apache.ignite.testframework.config.GridTestProperties;

/**
 *
 */
public class IgniteCacheBinaryMarshallerInsertSqlQuerySelfTest extends IgniteCacheInsertSqlQuerySelfTest {
    static {
        GridTestProperties.setProperty(GridTestProperties.MARSH_CLASS_NAME, BinaryMarshaller.class.getName());
    }

    /** {@inheritDoc} */
    @Override protected void createCaches() {
        createBinaryCaches();
    }

    /** {@inheritDoc} */
    @Override protected void beforeTestsStarted() throws Exception {
        super.beforeTestsStarted();

        // We have to register these types on the node we'll call 'get' on
        CacheObjectBinaryProcessor binProc = (CacheObjectBinaryProcessor)grid(0).context().cacheObjects();

        // Key types only!
        binProc.registerType(Key.class);
        binProc.registerType(Key2.class);
    }

    /** {@inheritDoc} */
    @Override protected Object createPerson(int id, String name) {
        return createPersonBinary(id, name);
    }
}
