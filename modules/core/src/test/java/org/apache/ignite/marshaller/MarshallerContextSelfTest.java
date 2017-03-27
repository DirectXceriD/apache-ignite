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

package org.apache.ignite.marshaller;

import java.io.File;
import java.io.IOException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import org.apache.ignite.Ignite;
import org.apache.ignite.internal.IgniteKernal;
import org.apache.ignite.internal.MarshallerContextImpl;
import org.apache.ignite.internal.processors.closure.GridClosureProcessor;
import org.apache.ignite.internal.processors.marshaller.MarshallerMappingItem;
import org.apache.ignite.internal.processors.pool.PoolProcessor;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.testframework.junits.GridTestKernalContext;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;

import static java.nio.file.Files.readAllBytes;
import static org.apache.ignite.internal.MarshallerPlatformIds.JAVA_ID;

/**
 * Test marshaller context.
 */
public class MarshallerContextSelfTest extends GridCommonAbstractTest {
    /** */
    private GridTestKernalContext ctx;

    /** */
    private ExecutorService execSvc;

    /** {@inheritDoc} */
    @Override protected void beforeTest() throws Exception {
        ctx = newContext();
        execSvc = Executors.newSingleThreadExecutor();

        ctx.setSystemExecutorService(execSvc);

        ctx.add(new PoolProcessor(ctx));

        ctx.add(new GridClosureProcessor(ctx));
    }

    /**
     * @throws Exception If failed.
     */
    public void testClassName() throws Exception {
        MarshallerContextImpl ctx = new MarshallerContextImpl(null);

        ctx.onMarshallerProcessorStarted(this.ctx, null);

        MarshallerMappingItem item = new MarshallerMappingItem(JAVA_ID, 1, String.class.getName());

        ctx.onMappingProposed(item);

        ctx.onMappingAccepted(item);

        try (Ignite g1 = startGrid(1)) {
            MarshallerContextImpl marshCtx = ((IgniteKernal)g1).context().marshallerContext();
            String clsName = marshCtx.getClassName(JAVA_ID, 1);

            assertEquals("java.lang.String", clsName);
        }
    }

    /**
     * @throws Exception If failed.
     */
    public void testOnUpdated() throws Exception {
        File workDir = U.resolveWorkDirectory(U.defaultWorkDirectory(), "marshaller", false);
        MarshallerContextImpl ctx = new MarshallerContextImpl(null);

        ctx.onMarshallerProcessorStarted(this.ctx, null);

        MarshallerMappingItem item1 = new MarshallerMappingItem(JAVA_ID, 1, String.class.getName());

        ctx.onMappingAccepted(item1);

        checkFileName("java.lang.String", Paths.get(workDir + "/1.classname0"));

        MarshallerMappingItem item2 = new MarshallerMappingItem((byte) 2, 2, "Random.Class.Name");

        ctx.onMappingProposed(item2);
        ctx.onMappingAccepted(item2);

        execSvc.shutdown();
        if (execSvc.awaitTermination(1000, TimeUnit.MILLISECONDS))
            checkFileName("Random.Class.Name", Paths.get(workDir + "/2.classname2"));
        else
            fail("Failed to wait for executor service to shutdown");
    }

    /**
     * Tests that there is a null value inserted in allCaches list
     * if platform ids passed to marshaller cache were not sequential (like 0, 2).
     */
    public void testCacheStructure0() throws Exception {
        MarshallerContextImpl ctx = new MarshallerContextImpl(null);

        ctx.onMarshallerProcessorStarted(this.ctx, null);

        MarshallerMappingItem item1 = new MarshallerMappingItem(JAVA_ID, 1, String.class.getName());

        ctx.onMappingAccepted(item1);

        MarshallerMappingItem item2 = new MarshallerMappingItem((byte) 2, 2, "Random.Class.Name");

        ctx.onMappingProposed(item2);

        List list = U.field(ctx, "allCaches");

        assertNotNull("Mapping cache is null for platformId: 0" , list.get(0));
        assertNull("Mapping cache is not null for platformId: 1", list.get(1));
        assertNotNull("Mapping cache is null for platformId: 2", list.get(2));

        boolean excObserved = false;
        try {
            list.get(3);
        }
        catch (ArrayIndexOutOfBoundsException ignored) {
            excObserved = true;
        }
        assertTrue("ArrayIndexOutOfBoundsException had to be thrown", excObserved);
    }

    /**
     * Tests that there are no null values in allCaches list
     * if platform ids passed to marshaller context were sequential.
     */
    public void testCacheStructure1() throws Exception {
        MarshallerContextImpl ctx = new MarshallerContextImpl(null);

        ctx.onMarshallerProcessorStarted(this.ctx, null);

        MarshallerMappingItem item1 = new MarshallerMappingItem(JAVA_ID, 1, String.class.getName());

        ctx.onMappingAccepted(item1);

        MarshallerMappingItem item2 = new MarshallerMappingItem((byte) 1, 2, "Random.Class.Name");

        ctx.onMappingProposed(item2);

        List list = U.field(ctx, "allCaches");

        assertNotNull("Mapping cache is null for platformId: 0" , list.get(0));
        assertNotNull("Mapping cache is null for platformId: 1", list.get(1));

        boolean excObserved = false;

        try {
            list.get(2);
        }
        catch (ArrayIndexOutOfBoundsException ignored) {
            excObserved = true;
        }

        assertTrue("ArrayIndexOutOfBoundsException had to be thrown", excObserved);
    }

    /**
     * @param expected Expected.
     * @param pathToReal Path to real.
     */
    private void checkFileName(String expected, Path pathToReal) throws IOException {
        assertEquals(expected, new String(readAllBytes(pathToReal)));
    }
}
