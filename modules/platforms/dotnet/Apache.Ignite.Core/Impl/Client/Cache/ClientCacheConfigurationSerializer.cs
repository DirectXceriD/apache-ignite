﻿/*
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

namespace Apache.Ignite.Core.Impl.Client.Cache
{
    using System;
    using System.Diagnostics;
    using Apache.Ignite.Core.Cache.Configuration;
    using Apache.Ignite.Core.Client.Cache;
    using Apache.Ignite.Core.Impl.Binary;
    using Apache.Ignite.Core.Impl.Binary.IO;

    /// <summary>
    /// Writes and reads <see cref="CacheConfiguration"/> for thin client mode.
    /// <para />
    /// Thin client supports a subset of <see cref="CacheConfiguration"/> properties, so
    /// <see cref="CacheConfiguration.Read"/> is not suitable.
    /// </summary>
    internal static class ClientCacheConfigurationSerializer
    {
        /** Config property opcodes. */
        private enum Op : short
        {
            AtomicityMode = 0, 
            Backups = 1, 
            CacheMode = 2, 
            CopyOnRead = 3, 
            DataRegionName = 4, 
            EagerTtl = 5, 
            StatisticsEnabled = 6, 
            GroupName = 7, 
            DefaultLockTimeout = 9, 
            MaxConcurrentAsyncOperations = 10, 
            MaxQueryIteratorsCount = 11, 
            Name = 12, 
            OnheapCacheEnabled = 13, 
            PartitionLossPolicy = 14, 
            QueryDetailMetricsSize = 15, 
            QueryParallelism = 16, 
            ReadFromBackup = 17, 
            RebalanceBatchSize = 18, 
            RebalanceBatchesPrefetchCount = 19, 
            RebalanceDelay = 20, 
            RebalanceMode = 21, 
            RebalanceOrder = 22, 
            RebalanceThrottle = 23, 
            RebalanceTimeout = 24, 
            SqlEscapeAll = 25, 
            SqlIndexMaxInlineSize = 26, 
            SqlSchema = 27, 
            WriteSynchronizationMode = 28, 
            KeyConfiguration = 29, 
            QueryEntities = 30
        }

        /** Property count. */
        private static readonly short PropertyCount = (short) Enum.GetValues(typeof(Op)).Length;
        
        /// <summary>
        /// Copies one cache configuration to another.
        /// </summary>
        public static void Copy(CacheConfiguration from, CacheClientConfiguration to, bool ignoreUnsupportedProperties)
        {
            Debug.Assert(from != null);
            Debug.Assert(to != null);

            to.AtomicityMode = from.AtomicityMode;
            to.Backups = from.Backups;
            to.CacheMode = from.CacheMode;
            to.CopyOnRead = from.CopyOnRead;
            to.DataRegionName = from.DataRegionName;
            to.EagerTtl = from.EagerTtl;
            to.EnableStatistics = from.EnableStatistics;
            to.GroupName = from.GroupName;
            to.LockTimeout = from.LockTimeout;
            to.MaxConcurrentAsyncOperations = from.MaxConcurrentAsyncOperations;
            to.MaxQueryIteratorsCount = from.MaxQueryIteratorsCount;
            to.Name = from.Name;
            to.OnheapCacheEnabled = from.OnheapCacheEnabled;
            to.PartitionLossPolicy = from.PartitionLossPolicy;
            to.QueryDetailMetricsSize = from.QueryDetailMetricsSize;
            to.QueryParallelism = from.QueryParallelism;
            to.ReadFromBackup = from.ReadFromBackup;
            to.RebalanceBatchSize = from.RebalanceBatchSize;
            to.RebalanceBatchesPrefetchCount = from.RebalanceBatchesPrefetchCount;
            to.RebalanceDelay = from.RebalanceDelay;
            to.RebalanceMode = from.RebalanceMode;
            to.RebalanceOrder = from.RebalanceOrder;
            to.RebalanceThrottle = from.RebalanceThrottle;
            to.RebalanceTimeout = from.RebalanceTimeout;
            to.SqlEscapeAll = from.SqlEscapeAll;
            to.SqlIndexMaxInlineSize = from.SqlIndexMaxInlineSize;
            to.SqlSchema = from.SqlSchema;
            to.WriteSynchronizationMode = from.WriteSynchronizationMode;

            to.KeyConfiguration = from.KeyConfiguration;
            to.QueryEntities = from.QueryEntities;

            if (!ignoreUnsupportedProperties)
            {
                // Unsupported complex properties.
                ThrowUnsupportedIfNotDefault(from.AffinityFunction, "AffinityFunction");
                ThrowUnsupportedIfNotDefault(from.EvictionPolicy, "EvictionPolicy");
                ThrowUnsupportedIfNotDefault(from.ExpiryPolicyFactory, "ExpiryPolicyFactory");
                ThrowUnsupportedIfNotDefault(from.PluginConfigurations, "PluginConfigurations");
                ThrowUnsupportedIfNotDefault(from.CacheStoreFactory, "CacheStoreFactory");
                ThrowUnsupportedIfNotDefault(from.NearConfiguration, "NearConfiguration");

                // Unsupported store-related properties.
                ThrowUnsupportedIfNotDefault(from.KeepBinaryInStore, "KeepBinaryInStore");
                ThrowUnsupportedIfNotDefault(from.LoadPreviousValue, "LoadPreviousValue");
                ThrowUnsupportedIfNotDefault(from.ReadThrough, "ReadThrough");
                ThrowUnsupportedIfNotDefault(from.WriteThrough, "WriteThrough");
                ThrowUnsupportedIfNotDefault(from.StoreConcurrentLoadAllThreshold, "StoreConcurrentLoadAllThreshold",
                    CacheConfiguration.DefaultStoreConcurrentLoadAllThreshold);
                ThrowUnsupportedIfNotDefault(from.WriteBehindBatchSize, "WriteBehindBatchSize",
                    CacheConfiguration.DefaultWriteBehindBatchSize);
                ThrowUnsupportedIfNotDefault(from.WriteBehindCoalescing, "WriteBehindCoalescing",
                    CacheConfiguration.DefaultWriteBehindCoalescing);
                ThrowUnsupportedIfNotDefault(from.WriteBehindEnabled, "WriteBehindEnabled");
                ThrowUnsupportedIfNotDefault(from.WriteBehindFlushFrequency, "WriteBehindFlushFrequency",
                    CacheConfiguration.DefaultWriteBehindFlushFrequency);
                ThrowUnsupportedIfNotDefault(from.WriteBehindFlushSize, "WriteBehindFlushSize",
                    CacheConfiguration.DefaultWriteBehindFlushSize);
                ThrowUnsupportedIfNotDefault(from.WriteBehindFlushThreadCount, "WriteBehindFlushThreadCount",
                    CacheConfiguration.DefaultWriteBehindFlushThreadCount);
            }
        }
        /// <summary>
        /// Copies one cache configuration to another.
        /// </summary>
        public static void Copy(CacheClientConfiguration from, CacheConfiguration to)
        {
            Debug.Assert(from != null);
            Debug.Assert(to != null);

            to.AtomicityMode = from.AtomicityMode;
            to.Backups = from.Backups;
            to.CacheMode = from.CacheMode;
            to.CopyOnRead = from.CopyOnRead;
            to.DataRegionName = from.DataRegionName;
            to.EagerTtl = from.EagerTtl;
            to.EnableStatistics = from.EnableStatistics;
            to.GroupName = from.GroupName;
            to.LockTimeout = from.LockTimeout;
            to.MaxConcurrentAsyncOperations = from.MaxConcurrentAsyncOperations;
            to.MaxQueryIteratorsCount = from.MaxQueryIteratorsCount;
            to.Name = from.Name;
            to.OnheapCacheEnabled = from.OnheapCacheEnabled;
            to.PartitionLossPolicy = from.PartitionLossPolicy;
            to.QueryDetailMetricsSize = from.QueryDetailMetricsSize;
            to.QueryParallelism = from.QueryParallelism;
            to.ReadFromBackup = from.ReadFromBackup;
            to.RebalanceBatchSize = from.RebalanceBatchSize;
            to.RebalanceBatchesPrefetchCount = from.RebalanceBatchesPrefetchCount;
            to.RebalanceDelay = from.RebalanceDelay;
            to.RebalanceMode = from.RebalanceMode;
            to.RebalanceOrder = from.RebalanceOrder;
            to.RebalanceThrottle = from.RebalanceThrottle;
            to.RebalanceTimeout = from.RebalanceTimeout;
            to.SqlEscapeAll = from.SqlEscapeAll;
            to.SqlIndexMaxInlineSize = from.SqlIndexMaxInlineSize;
            to.SqlSchema = from.SqlSchema;
            to.WriteSynchronizationMode = from.WriteSynchronizationMode;

            to.KeyConfiguration = from.KeyConfiguration;
            to.QueryEntities = from.QueryEntities;
        }

        /// <summary>
        /// Writes the specified config.
        /// </summary>
        public static void Write(IBinaryStream stream, CacheClientConfiguration cfg, bool skipCodes = false)
        {
            Debug.Assert(stream != null);
            Debug.Assert(cfg != null);

            // Configuration should be written with a system marshaller.
            var writer = BinaryUtils.Marshaller.StartMarshal(stream);
            var pos = writer.Stream.Position;
            writer.WriteInt(0);  // Reserve for length.

            if (!skipCodes)
            {
                writer.WriteShort(PropertyCount); // Property count.
            }

            var code = skipCodes
                ? (Action<Op>) (o => { })
                : o => writer.WriteShort((short) o);

            code(Op.AtomicityMode);
            writer.WriteInt((int)cfg.AtomicityMode);
            
            code(Op.Backups);
            writer.WriteInt(cfg.Backups);
            
            code(Op.CacheMode);
            writer.WriteInt((int)cfg.CacheMode);
            
            code(Op.CopyOnRead);
            writer.WriteBoolean(cfg.CopyOnRead);
            
            code(Op.DataRegionName);
            writer.WriteString(cfg.DataRegionName);
            
            code(Op.EagerTtl);
            writer.WriteBoolean(cfg.EagerTtl);
            
            code(Op.StatisticsEnabled);
            writer.WriteBoolean(cfg.EnableStatistics);
            
            code(Op.GroupName);
            writer.WriteString(cfg.GroupName);
            
            code(Op.DefaultLockTimeout);
            writer.WriteTimeSpanAsLong(cfg.LockTimeout);

            code(Op.MaxConcurrentAsyncOperations);
            writer.WriteInt(cfg.MaxConcurrentAsyncOperations);

            code(Op.MaxQueryIteratorsCount);
            writer.WriteInt(cfg.MaxQueryIteratorsCount);

            code(Op.Name);
            writer.WriteString(cfg.Name);
            
            code(Op.OnheapCacheEnabled);
            writer.WriteBoolean(cfg.OnheapCacheEnabled);
            
            code(Op.PartitionLossPolicy);
            writer.WriteInt((int)cfg.PartitionLossPolicy);
            
            code(Op.QueryDetailMetricsSize);
            writer.WriteInt(cfg.QueryDetailMetricsSize);
            
            code(Op.QueryParallelism);
            writer.WriteInt(cfg.QueryParallelism);
            
            code(Op.ReadFromBackup);
            writer.WriteBoolean(cfg.ReadFromBackup);
            
            code(Op.RebalanceBatchSize);
            writer.WriteInt(cfg.RebalanceBatchSize);
            
            code(Op.RebalanceBatchesPrefetchCount);
            writer.WriteLong(cfg.RebalanceBatchesPrefetchCount);
            
            code(Op.RebalanceDelay);
            writer.WriteTimeSpanAsLong(cfg.RebalanceDelay);
            
            code(Op.RebalanceMode);
            writer.WriteInt((int)cfg.RebalanceMode);
            
            code(Op.RebalanceOrder);
            writer.WriteInt(cfg.RebalanceOrder);
            
            code(Op.RebalanceThrottle);
            writer.WriteTimeSpanAsLong(cfg.RebalanceThrottle);
            
            code(Op.RebalanceTimeout);
            writer.WriteTimeSpanAsLong(cfg.RebalanceTimeout);
            
            code(Op.SqlEscapeAll);
            writer.WriteBoolean(cfg.SqlEscapeAll);
            
            code(Op.SqlIndexMaxInlineSize);
            writer.WriteInt(cfg.SqlIndexMaxInlineSize);
            
            code(Op.SqlSchema);
            writer.WriteString(cfg.SqlSchema);
            
            code(Op.WriteSynchronizationMode);
            writer.WriteInt((int)cfg.WriteSynchronizationMode);

            code(Op.KeyConfiguration);
            writer.WriteCollectionRaw(cfg.KeyConfiguration);
            
            code(Op.QueryEntities);
            writer.WriteCollectionRaw(cfg.QueryEntities);

            // Write length (so that part of the config can be skipped).
            var len = writer.Stream.Position - pos - 4;
            writer.Stream.WriteInt(pos, len);
        }
        
        /// <summary>
        /// Reads the config.
        /// </summary>
        public static void Read(IBinaryStream stream, CacheClientConfiguration cfg)
        {
            Debug.Assert(stream != null);

            // Configuration should be read with system marshaller.
            var reader = BinaryUtils.Marshaller.StartUnmarshal(stream);

            var len = reader.ReadInt();
            var pos = reader.Stream.Position;

            cfg.AtomicityMode = (CacheAtomicityMode)reader.ReadInt();
            cfg.Backups = reader.ReadInt();
            cfg.CacheMode = (CacheMode)reader.ReadInt();
            cfg.CopyOnRead = reader.ReadBoolean();
            cfg.DataRegionName = reader.ReadString();
            cfg.EagerTtl = reader.ReadBoolean();
            cfg.EnableStatistics = reader.ReadBoolean();
            cfg.GroupName = reader.ReadString();
            cfg.LockTimeout = reader.ReadLongAsTimespan();
            cfg.MaxConcurrentAsyncOperations = reader.ReadInt();
            cfg.MaxQueryIteratorsCount = reader.ReadInt();
            cfg.Name = reader.ReadString();
            cfg.OnheapCacheEnabled = reader.ReadBoolean();
            cfg.PartitionLossPolicy = (PartitionLossPolicy)reader.ReadInt();
            cfg.QueryDetailMetricsSize = reader.ReadInt();
            cfg.QueryParallelism = reader.ReadInt();
            cfg.ReadFromBackup = reader.ReadBoolean();
            cfg.RebalanceBatchSize = reader.ReadInt();
            cfg.RebalanceBatchesPrefetchCount = reader.ReadLong();
            cfg.RebalanceDelay = reader.ReadLongAsTimespan();
            cfg.RebalanceMode = (CacheRebalanceMode)reader.ReadInt();
            cfg.RebalanceOrder = reader.ReadInt();
            cfg.RebalanceThrottle = reader.ReadLongAsTimespan();
            cfg.RebalanceTimeout = reader.ReadLongAsTimespan();
            cfg.SqlEscapeAll = reader.ReadBoolean();
            cfg.SqlIndexMaxInlineSize = reader.ReadInt();
            cfg.SqlSchema = reader.ReadString();
            cfg.WriteSynchronizationMode = (CacheWriteSynchronizationMode)reader.ReadInt();
            cfg.KeyConfiguration = reader.ReadCollectionRaw(r => new CacheKeyConfiguration(r));
            cfg.QueryEntities = reader.ReadCollectionRaw(r => new QueryEntity(r));

            Debug.Assert(len == reader.Stream.Position - pos);
        }

        /// <summary>
        /// Throws the unsupported exception if property is not default.
        /// </summary>
        // ReSharper disable ParameterOnlyUsedForPreconditionCheck.Local
        // ReSharper disable UnusedParameter.Local
        private static void ThrowUnsupportedIfNotDefault<T>(T obj, string propertyName, T defaultValue = default(T))
        {
            if (!Equals(obj, defaultValue))
            {
                throw new NotSupportedException(
                    string.Format("{0}.{1} property is not supported in thin client mode.",
                        typeof(CacheConfiguration).Name, propertyName));
            }
        }
    }
}
