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

package org.apache.ignite.spi;

import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

/**
 * Test for {@link ExponentialBackoffTimeoutStrategyTest}.
 */
@RunWith(JUnit4.class)
public class ExponentialBackoffTimeoutStrategyTest extends GridCommonAbstractTest {
    /** */
    @Test
    public void checkTimeout() {
        ExponentialBackoffTimeoutStrategy helper = new ExponentialBackoffTimeoutStrategy(
            5_000L,
            1000L,
            3000L,
            3,
            2.
        );

        checkTimeout(helper, 5_000L);
    }

    /** */
    @Test
    public void backoff() throws IgniteSpiOperationTimeoutException {
        ExponentialBackoffTimeoutStrategy helper = new ExponentialBackoffTimeoutStrategy(
            5_000L,
            1000L,
            3_000L,
            3,
            2.
        );

        assertEquals(1000L, helper.getAndCalculateNextTimeout());

        assertEquals(2000L, helper.getAndCalculateNextTimeout());

        helper.getAndCalculateNextTimeout();

        assertEquals(3000L, helper.getAndCalculateNextTimeout());
    }

    /** */
    @Test
    public void totalBackoffTimeout() {
        assertEquals(8_000, ExponentialBackoffTimeoutStrategy.totalBackoffTimeout(1000, 5000, 3, 2.0));
        assertEquals(45_000, ExponentialBackoffTimeoutStrategy.totalBackoffTimeout(5_000, 60_000, 3, 2.0));
    }

    /** */
    private void checkTimeout(
        ExponentialBackoffTimeoutStrategy helper,
        long timeout
    ) {
        long start = System.currentTimeMillis();

        while (true) {
            boolean timedOut = helper.checkTimeout();

            if (timedOut) {
                assertTrue( (System.currentTimeMillis() + 100 - start) >= timeout);

                try {
                    helper.getAndCalculateNextTimeout();

                    fail("Should fail with IOException");
                } catch (IgniteSpiOperationTimeoutException ignored) {
                    //No-op
                }

                return;
            }
        }


    }
}
