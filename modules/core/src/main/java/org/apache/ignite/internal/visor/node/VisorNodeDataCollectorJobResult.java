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

package org.apache.ignite.internal.visor.node;

import org.apache.ignite.internal.visor.cache.*;
import org.apache.ignite.internal.visor.event.*;
import org.apache.ignite.internal.visor.igfs.*;
import org.apache.ignite.internal.visor.streamer.*;

import java.io.*;
import java.util.*;

/**
 * Data collector job result.
 */
public class VisorNodeDataCollectorJobResult implements Serializable {
    /** */
    private static final long serialVersionUID = 0L;

    /** Grid name. */
    private String gridName;

    /** Node topology version. */
    private long topVer;

    /** Task monitoring state collected from node. */
    private boolean taskMonitoringEnabled;

    /** Node events. */
    private final Collection<VisorGridEvent> evts = new ArrayList<>();

    /** Exception while collecting node events. */
    private Throwable evtsEx;

    /** Node caches. */
    private final Collection<VisorCache> caches = new ArrayList<>();

    /** Exception while collecting node caches. */
    private Throwable cachesEx;

    /** Node IGFSs. */
    private final Collection<VisorIgfs> igfss = new ArrayList<>();

    /** All IGFS endpoints collected from nodes. */
    private final Collection<VisorIgfsEndpoint> igfsEndpoints = new ArrayList<>();

    /** Exception while collecting node IGFSs. */
    private Throwable igfssEx;

    /** Node streamers. */
    private final Collection<VisorStreamer> streamers = new ArrayList<>();

    /** Exception while collecting node streamers. */
    private Throwable streamersEx;

    /** Errors count. */
    private long errCnt;

    /**
     * @return Grid name.
     */
    public String gridName() {
        return gridName;
    }

    /**
     * @param gridName New grid name value.
     */
    public void gridName(String gridName) {
        this.gridName = gridName;
    }

    /**
     * @return Current topology version.
     */
    public long topologyVersion() {
        return topVer;
    }

    /**
     * @param topVer New topology version value.
     */
    public void topologyVersion(long topVer) {
        this.topVer = topVer;
    }

    /**
     * @return Current task monitoring state.
     */
    public boolean taskMonitoringEnabled() {
        return taskMonitoringEnabled;
    }

    /**
     * @param taskMonitoringEnabled New value of task monitoring state.
     */
    public void taskMonitoringEnabled(boolean taskMonitoringEnabled) {
        this.taskMonitoringEnabled = taskMonitoringEnabled;
    }

    /**
     * @return Collection of collected events.
     */
    public Collection<VisorGridEvent> events() {
        return evts;
    }

    /**
     * @return Exception caught during collecting events.
     */
    public Throwable eventsEx() {
        return evtsEx;
    }

    /**
     * @param evtsEx Exception caught during collecting events.
     */
    public void eventsEx(Throwable evtsEx) {
        this.evtsEx = evtsEx;
    }

    /**
     * @return Collected cache metrics.
     */
    public Collection<VisorCache> caches() {
        return caches;
    }

    /**
     * @return Exception caught during collecting caches metrics.
     */
    public Throwable cachesEx() {
        return cachesEx;
    }

    /**
     * @param cachesEx Exception caught during collecting caches metrics.
     */
    public void cachesEx(Throwable cachesEx) {
        this.cachesEx = cachesEx;
    }

    /**
     * @return Collected IGFSs metrics.
     */
    public Collection<VisorIgfs> igfss() {
        return igfss;
    }

    /**
     * @return Collected IGFSs endpoints.
     */
    public Collection<VisorIgfsEndpoint> igfsEndpoints() {
        return igfsEndpoints;
    }

    /**
     * @return Exception caught during collecting IGFSs metrics.
     */
    public Throwable igfssEx() {
        return igfssEx;
    }

    /**
     * @param igfssEx Exception caught during collecting IGFSs metrics.
     */
    public void igfssEx(Throwable igfssEx) {
        this.igfssEx = igfssEx;
    }

    /**
     * @return Collection of streamers metrics.
     */
    public Collection<VisorStreamer> streamers() {
        return streamers;
    }

    /**
     * @return Exception caught during collecting streamers metrics.
     */
    public Throwable streamersEx() {
        return streamersEx;
    }

    /**
     * @param streamersEx Exception caught during collecting streamers metrics.
     */
    public void streamersEx(Throwable streamersEx) {
        this.streamersEx = streamersEx;
    }

    /**
     * @return Errors count.
     */
    public long errorCount() {
        return errCnt;
    }

    /**
     * @param errorCount Errors count.
     */
    public void errorCount(long errorCount) {
        errCnt = errorCount;
    }
}
