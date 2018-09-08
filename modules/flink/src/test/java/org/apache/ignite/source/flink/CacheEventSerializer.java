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

package org.apache.ignite.source.flink;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.Serializer;
import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.Output;
import org.apache.commons.lang3.SerializationException;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.events.CacheEvent;
import org.apache.ignite.marshaller.Marshaller;
import org.apache.ignite.marshaller.jdk.JdkMarshaller;
import java.io.Serializable;

/**
 * Serializer based on {@link JdkMarshaller}.
 */
public class CacheEventSerializer extends Serializer<CacheEvent> implements Serializable {

    private static final long serialVersionUID = 0L;
    /** Marshaller. */
    private Marshaller marsh;

    /**
     * If true, the type this serializer will be used for is considered immutable.
     * This causes {@link #copy(Kryo, Object)} to return the original object.
     * */
    public CacheEventSerializer(){
        setImmutable(true);
    }

    /**
     * If true, the type this serializer will be used for is considered immutable.
     * This causes {@link #copy(Kryo, Object)} to return the original object. This will
     * initialize the marshaller from Ignite instance context.
     * */
    public CacheEventSerializer(Marshaller marsh){
        this.marsh = marsh;
        setImmutable(true);
    }

    /** {@inheritDoc} */
    @Override public void write(Kryo kryo, Output output, CacheEvent cacheEvt) {
        try {
            output.write(marsh.marshal(cacheEvt));
        } catch (IgniteCheckedException e) {
            throw new SerializationException("Failed to serialize cache event!", e);
        }
    }

    /** {@inheritDoc} */
    @Override public CacheEvent read(Kryo kryo, Input input, Class<CacheEvent> cacheEvtCls) {
        try {
            return marsh.unmarshal(input, getClass().getClassLoader());
        }
        catch (IgniteCheckedException e) {
            throw new SerializationException("Failed to deserialize cache event!", e);
        }
    }
}

