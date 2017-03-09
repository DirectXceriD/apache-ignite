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
 *
 */

package org.apache.ignite.internal.processors.cache.distributed.dht.preloader;

import java.io.Serializable;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import org.apache.ignite.internal.util.typedef.T2;

public class IgniteDhtDemandedPartitionsMap implements Serializable {
    /** */
    private static final long serialVersionUID = 0L;

    private Map<Integer, T2<Long, Long>> historical;

    private Set<Integer> full;

    public void addHistorical(int partId, long from, long to) {
        assert !hasFull(partId);

        if (historical == null)
            historical = new HashMap<>();

        historical.put(partId, new T2<>(from, to));
    }

    public void addFull(int partId) {
        assert !hasHistorical(partId);

        if (full == null)
            full = new HashSet<>();

        full.add(partId);
    }

    public boolean remove(int partId) {
        assert !(hasFull(partId) && hasHistorical(partId));

        if (full != null && full.remove(partId))
            return true;

        if (historical != null && historical.remove(partId) != null)
            return true;

        return false;
    }

    public boolean hasHistorical() {
        return historical != null && !historical.isEmpty();
    }

    public boolean hasHistorical(int partId) {
        return historical != null && historical.containsKey(partId);
    }

    public boolean hasFull() {
        return full != null && !full.isEmpty();
    }

    public boolean hasFull(int partId) {
        return full != null && full.contains(partId);
    }

    public boolean isEmpty() {
        return !hasFull() && !hasHistorical();
    }

    public int size() {
        int histSize = historical != null ? historical.size() : 0;
        int fullSize = full != null ? full.size() : 0;

        return histSize + fullSize;
    }

    public Map<Integer, T2<Long, Long>> historicalMap() {
        if (historical == null)
            return Collections.emptyMap();

        return Collections.unmodifiableMap(historical);
    }

    public Set<Integer> fullSet() {
        if (full == null)
            return Collections.emptySet();

        return Collections.unmodifiableSet(full);
    }
}
