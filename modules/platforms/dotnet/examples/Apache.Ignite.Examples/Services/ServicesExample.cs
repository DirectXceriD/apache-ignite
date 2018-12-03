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

namespace Apache.Ignite.Examples.Services
{
    using System;
    using Apache.Ignite.Core;
    using Apache.Ignite.Core.Cluster;
    using Apache.Ignite.Core.Services;
    using Apache.Ignite.ExamplesDll.Services;

    /// <summary>
    /// Example demonstrating Ignite services.
    /// <para />
    /// 1) Build the project Apache.Ignite.ExamplesDll (select it -> right-click -> Build).
    ///    Apache.Ignite.ExamplesDll.dll must appear in %IGNITE_HOME%/platforms/dotnet/examples/Apache.Ignite.ExamplesDll/bin/${Platform]/${Configuration} folder.
    /// 2) Set this class as startup object (Apache.Ignite.Examples project -> right-click -> Properties ->
    ///     Application -> Startup object);
    /// 3) Start example (F5 or Ctrl+F5).
    /// <para />
    /// This example can be run with standalone Apache Ignite.NET node:
    /// 1) Run %IGNITE_HOME%/platforms/dotnet/bin/Apache.Ignite.exe:
    /// Apache.Ignite.exe -configFileName=platforms\dotnet\examples\apache.ignite.examples\app.config -assembly=[path_to_Apache.Ignite.ExamplesDll.dll]
    /// 2) Start example.
    /// </summary>
    public class ServicesExample
    {
        /// <summary>
        /// Runs the example.
        /// </summary>
        [STAThread]
        public static void Main()
        {
            using (var ignite = Ignition.StartFromApplicationConfiguration())
            {
                using (var ignite2 = Ignition.StartFromApplicationConfiguration("igniteConfiguration2"))
                {

                    Console.WriteLine(">>> Services example started.");
                    Console.WriteLine();

                    // Deploy a service
                    var svc = new MapService<int, string>();
                    Console.WriteLine(">>> Deploying service to all nodes...");

                    var nodeFilter = new WfeNodeFilter();

                    var svcCfg = new ServiceConfiguration()
                    {
                        MaxPerNodeCount = 1,
                        TotalCount = 0,
                        Name = "service",
                        Service = svc,
                        NodeFilter = new WfeNodeFilter()
                    };

                    IClusterGroup dotNetNodes = ignite.GetCluster().ForDotNet();
                    IClusterGroup wfeNodes = dotNetNodes.ForPredicate(nodeFilter.Invoke);

                    wfeNodes.GetServices().Deploy(svcCfg);

                    Console.WriteLine(">>> Getting service proxy...");

                    // Get a sticky service proxy so that we will always be contacting the same remote node.
                    var prx = wfeNodes.GetServices().GetServiceProxy<IMapService<int, string>>("service", true);

                    Console.WriteLine(">>> Calling the service...");
                    for (var i = 0; i < 10; i++)
                        prx.Put(i, i.ToString());

                    var mapSize = prx.Size;

                    Console.WriteLine(">>> Map service size: " + mapSize);

                    dotNetNodes.GetServices().CancelAll();

                    Console.WriteLine();
                    Console.WriteLine(">>> Example finished, press any key to exit ...");
                    Console.ReadKey();
                }
            }
        }

        [Serializable]
        private class WfeNodeFilter : IClusterNodeFilter
        {
            /// <summary>
            /// Gets or sets the node identifier.
            /// </summary>
            public Guid NodeId { get; set; }

            /** <inheritdoc /> */
            public bool Invoke(IClusterNode node)
            {
                return node.Attributes.ContainsKey("ROLE") && "WFE".Equals(node.Attributes["ROLE"]);
            }
        }
    }
}
