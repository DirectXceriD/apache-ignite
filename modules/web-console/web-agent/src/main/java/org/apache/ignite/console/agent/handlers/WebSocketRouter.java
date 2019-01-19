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

package org.apache.ignite.console.agent.handlers;

import java.util.UUID;
import io.vertx.core.buffer.Buffer;
import io.vertx.core.http.HttpClient;
import io.vertx.core.json.JsonObject;
import org.apache.ignite.console.agent.AgentConfiguration;

/**
 *
 */
public class WebSocketRouter extends AbstractHandler {
    /** */
    private final AgentConfiguration cfg;

    /** */
    private final String id = UUID.randomUUID().toString();

    /**
     * @param cfg Configuration.
     */
    public WebSocketRouter(AgentConfiguration cfg) {
        this.cfg = cfg;
    }


    /** {@inheritDoc} */
    @Override public void start() {
        log.info("Connecting to: {}", cfg.serverUri());

        HttpClient client = vertx.createHttpClient();

        client.websocket(3000, "localhost", "/eventbus", ws -> {
            System.out.println("Connected to web socket");

            ws.handler(data -> {
                System.out.println("Received data " + data.toString());
            });

            ws.closeHandler(p -> {
                System.out.println("Closed!");
            });

            JsonObject json = new JsonObject();
            json.put("agent", id);

            ws.writeTextMessage(json.toString());
        });

    }
}
