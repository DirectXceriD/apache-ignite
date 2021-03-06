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

package org.apache.ignite.spi.communication.tcp;

import org.apache.ignite.*;
import org.apache.ignite.cluster.*;
import org.apache.ignite.configuration.*;
import org.apache.ignite.events.*;
import org.apache.ignite.internal.*;
import org.apache.ignite.internal.managers.eventstorage.*;
import org.apache.ignite.internal.util.*;
import org.apache.ignite.internal.util.future.*;
import org.apache.ignite.internal.util.lang.*;
import org.apache.ignite.internal.util.nio.*;
import org.apache.ignite.internal.util.typedef.*;
import org.apache.ignite.internal.util.typedef.internal.*;
import org.apache.ignite.lang.*;
import org.apache.ignite.plugin.extensions.communication.*;
import org.apache.ignite.resources.*;
import org.apache.ignite.spi.*;
import org.apache.ignite.spi.communication.*;
import org.jdk8.backport.*;
import org.jetbrains.annotations.*;

import java.io.*;
import java.net.*;
import java.nio.*;
import java.nio.channels.*;
import java.nio.channels.spi.*;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.*;

import static org.apache.ignite.IgniteSystemProperties.*;
import static org.apache.ignite.events.EventType.*;

/**
 * <tt>GridTcpCommunicationSpi</tt> is default communication SPI which uses
 * TCP/IP protocol and Java NIO to communicate with other nodes.
 * <p>
 * To enable communication with other nodes, this SPI adds {@link #ATTR_ADDRS}
 * and {@link #ATTR_PORT} local node attributes (see {@link ClusterNode#attributes()}.
 * <p>
 * At startup, this SPI tries to start listening to local port specified by
 * {@link #setLocalPort(int)} method. If local port is occupied, then SPI will
 * automatically increment the port number until it can successfully bind for
 * listening. {@link #setLocalPortRange(int)} configuration parameter controls
 * maximum number of ports that SPI will try before it fails. Port range comes
 * very handy when starting multiple grid nodes on the same machine or even
 * in the same VM. In this case all nodes can be brought up without a single
 * change in configuration.
 * <p>
 * This SPI caches connections to remote nodes so it does not have to reconnect every
 * time a message is sent. By default, idle connections are kept active for
 * {@link #DFLT_IDLE_CONN_TIMEOUT} period and then are closed. Use
 * {@link #setIdleConnectionTimeout(long)} configuration parameter to configure
 * you own idle connection timeout.
 * <p>
 * <h1 class="header">Configuration</h1>
 * <h2 class="header">Mandatory</h2>
 * This SPI has no mandatory configuration parameters.
 * <h2 class="header">Optional</h2>
 * The following configuration parameters are optional:
 * <ul>
 * <li>Node local IP address (see {@link #setLocalAddress(String)})</li>
 * <li>Node local port number (see {@link #setLocalPort(int)})</li>
 * <li>Local port range (see {@link #setLocalPortRange(int)}</li>
 * <li>Connection buffer flush frequency (see {@link #setConnectionBufferFlushFrequency(long)})</li>
 * <li>Connection buffer size (see {@link #setConnectionBufferSize(int)})</li>
 * <li>Idle connection timeout (see {@link #setIdleConnectionTimeout(long)})</li>
 * <li>Direct or heap buffer allocation (see {@link #setDirectBuffer(boolean)})</li>
 * <li>Direct or heap buffer allocation for sending (see {@link #setDirectSendBuffer(boolean)})</li>
 * <li>Count of selectors and selector threads for NIO server (see {@link #setSelectorsCount(int)})</li>
 * <li>{@code TCP_NODELAY} socket option for sockets (see {@link #setTcpNoDelay(boolean)})</li>
 * <li>Message queue limit (see {@link #setMessageQueueLimit(int)})</li>
 * <li>Minimum buffered message count (see {@link #setMinimumBufferedMessageCount(int)})</li>
 * <li>Buffer size ratio (see {@link #setBufferSizeRatio(double)})</li>
 * <li>Connect timeout (see {@link #setConnectTimeout(long)})</li>
 * <li>Maximum connect timeout (see {@link #setMaxConnectTimeout(long)})</li>
 * <li>Reconnect attempts count (see {@link #setReconnectCount(int)})</li>
 * <li>Socket receive buffer size (see {@link #setSocketReceiveBuffer(int)})</li>
 * <li>Socket send buffer size (see {@link #setSocketSendBuffer(int)})</li>
 * <li>Socket write timeout (see {@link #setSocketWriteTimeout(long)})</li>
 * <li>Number of received messages after which acknowledgment is sent (see {@link #setAckSendThreshold(int)})</li>
 * <li>Maximum number of unacknowledged messages (see {@link #setUnacknowledgedMessagesBufferSize(int)})</li>
 * </ul>
 * <h2 class="header">Java Example</h2>
 * GridTcpCommunicationSpi is used by default and should be explicitly configured
 * only if some SPI configuration parameters need to be overridden.
 * <pre name="code" class="java">
 * GridTcpCommunicationSpi commSpi = new GridTcpCommunicationSpi();
 *
 * // Override local port.
 * commSpi.setLocalPort(4321);
 *
 * GridConfiguration cfg = new GridConfiguration();
 *
 * // Override default communication SPI.
 * cfg.setCommunicationSpi(commSpi);
 *
 * // Start grid.
 * Ignition.start(cfg);
 * </pre>
 * <h2 class="header">Spring Example</h2>
 * GridTcpCommunicationSpi can be configured from Spring XML configuration file:
 * <pre name="code" class="xml">
 * &lt;bean id="grid.custom.cfg" class="org.apache.ignite.configuration.IgniteConfiguration" singleton="true"&gt;
 *         ...
 *         &lt;property name="communicationSpi"&gt;
 *             &lt;bean class="org.apache.ignite.spi.communication.tcp.GridTcpCommunicationSpi"&gt;
 *                 &lt;!-- Override local port. --&gt;
 *                 &lt;property name="localPort" value="4321"/&gt;
 *             &lt;/bean&gt;
 *         &lt;/property&gt;
 *         ...
 * &lt;/bean&gt;
 * </pre>
 * <p>
 * <img src="http://ignite.incubator.apache.org/images/spring-small.png">
 * <br>
 * For information about Spring framework visit <a href="http://www.springframework.org/">www.springframework.org</a>
 * @see CommunicationSpi
 */
@IgniteSpiMultipleInstancesSupport(true)
@IgniteSpiConsistencyChecked(optional = false)
public class TcpCommunicationSpi extends IgniteSpiAdapter
    implements CommunicationSpi<Message>, TcpCommunicationSpiMBean {
    /** Node attribute that is mapped to node IP addresses (value is <tt>comm.tcp.addrs</tt>). */
    public static final String ATTR_ADDRS = "comm.tcp.addrs";

    /** Node attribute that is mapped to node host names (value is <tt>comm.tcp.host.names</tt>). */
    public static final String ATTR_HOST_NAMES = "comm.tcp.host.names";

    /** Node attribute that is mapped to node port number (value is <tt>comm.tcp.port</tt>). */
    public static final String ATTR_PORT = "comm.tcp.port";

    /** Node attribute that is mapped to node's external addresses (value is <tt>comm.tcp.ext-addrs</tt>). */
    public static final String ATTR_EXT_ADDRS = "comm.tcp.ext-addrs";

    /** Default port which node sets listener to (value is <tt>47100</tt>). */
    public static final int DFLT_PORT = 47100;

    /** Default idle connection timeout (value is <tt>30000</tt>ms). */
    public static final long DFLT_IDLE_CONN_TIMEOUT = 30000;

    /** Default value for connection buffer flush frequency (value is <tt>100</tt> ms). */
    public static final long DFLT_CONN_BUF_FLUSH_FREQ = 100;

    /** Default value for connection buffer size (value is <tt>0</tt>). */
    public static final int DFLT_CONN_BUF_SIZE = 0;

    /** Default socket send and receive buffer size. */
    public static final int DFLT_SOCK_BUF_SIZE = 32 * 1024;

    /** Default connection timeout (value is <tt>1000</tt>ms). */
    public static final long DFLT_CONN_TIMEOUT = 1000;

    /** Default Maximum connection timeout (value is <tt>600,000</tt>ms). */
    public static final long DFLT_MAX_CONN_TIMEOUT = 10 * 60 * 1000;

    /** Default reconnect attempts count (value is <tt>10</tt>). */
    public static final int DFLT_RECONNECT_CNT = 10;

    /** Default message queue limit per connection (for incoming and outgoing . */
    public static final int DFLT_MSG_QUEUE_LIMIT = GridNioServer.DFLT_SEND_QUEUE_LIMIT;

    /**
     * Default count of selectors for TCP server equals to
     * {@code "Math.min(4, Runtime.getRuntime().availableProcessors())"}.
     */
    public static final int DFLT_SELECTORS_CNT = Math.min(4, Runtime.getRuntime().availableProcessors());

    /** Node ID meta for session. */
    private static final int NODE_ID_META = GridNioSessionMetaKey.nextUniqueKey();

    /** Message tracker meta for session. */
    private static final int TRACKER_META = GridNioSessionMetaKey.nextUniqueKey();

    /**
     * Default local port range (value is <tt>100</tt>).
     * See {@link #setLocalPortRange(int)} for details.
     */
    public static final int DFLT_PORT_RANGE = 100;

    /** Default value for {@code TCP_NODELAY} socket option (value is <tt>true</tt>). */
    public static final boolean DFLT_TCP_NODELAY = true;

    /** Default received messages threshold for sending ack. */
    public static final int DFLT_ACK_SND_THRESHOLD = 16;

    /** Default socket write timeout. */
    public static final long DFLT_SOCK_WRITE_TIMEOUT = GridNioServer.DFLT_SES_WRITE_TIMEOUT;

    /** No-op runnable. */
    private static final IgniteRunnable NOOP = new IgniteRunnable() {
        @Override public void run() {
            // No-op.
        }
    };

    /** Node ID message type. */
    public static final byte NODE_ID_MSG_TYPE = -1;

    /** */
    public static final byte RECOVERY_LAST_ID_MSG_TYPE = -2;

    /** */
    public static final byte HANDSHAKE_MSG_TYPE = -3;

    /** Server listener. */
    private final GridNioServerListener<Message> srvLsnr =
        new GridNioServerListenerAdapter<Message>() {
            @Override public void onSessionWriteTimeout(GridNioSession ses) {
                LT.warn(log, null, "Communication SPI Session write timed out (consider increasing " +
                    "'socketWriteTimeout' " + "configuration property) [remoteAddr=" + ses.remoteAddress() +
                    ", writeTimeout=" + sockWriteTimeout + ']');

                if (log.isDebugEnabled())
                    log.debug("Closing communication SPI session on write timeout [remoteAddr=" + ses.remoteAddress() +
                        ", writeTimeout=" + sockWriteTimeout + ']');

                ses.close();
            }

            @Override public void onConnected(GridNioSession ses) {
                if (ses.accepted()) {
                    if (log.isDebugEnabled())
                        log.debug("Sending local node ID to newly accepted session: " + ses);

                    ses.send(nodeIdMsg);
                }
            }

            @Override public void onDisconnected(GridNioSession ses, @Nullable Exception e) {
                UUID id = ses.meta(NODE_ID_META);

                if (id != null) {
                    GridCommunicationClient rmv = clients.get(id);

                    if (rmv instanceof GridTcpNioCommunicationClient &&
                        ((GridTcpNioCommunicationClient)rmv).session() == ses &&
                        clients.remove(id, rmv)) {
                        rmv.forceClose();

                        if (!stopping) {
                            GridNioRecoveryDescriptor recoveryData = ses.recoveryDescriptor();

                            if (recoveryData != null) {
                                if (recoveryData.nodeAlive(getSpiContext().node(id))) {
                                    if (!recoveryData.messagesFutures().isEmpty()) {
                                        if (log.isDebugEnabled())
                                            log.debug("Session was closed but there are unacknowledged messages, " +
                                                "will try to reconnect [rmtNode=" + recoveryData.node().id() + ']');

                                        recoveryWorker.addReconnectRequest(recoveryData);
                                    }
                                }
                                else
                                    recoveryData.onNodeLeft();
                            }
                        }
                    }

                    CommunicationListener<Message> lsnr0 = lsnr;

                    if (lsnr0 != null)
                        lsnr0.onDisconnected(id);
                }
            }

            @Override public void onMessage(GridNioSession ses, Message msg) {
                UUID sndId = ses.meta(NODE_ID_META);

                if (sndId == null) {
                    assert ses.accepted();

                    if (msg instanceof NodeIdMessage)
                        sndId = U.bytesToUuid(((NodeIdMessage) msg).nodeIdBytes, 0);
                    else {
                        assert msg instanceof HandshakeMessage : msg;

                        sndId = ((HandshakeMessage)msg).nodeId();
                    }

                    if (log.isDebugEnabled())
                        log.debug("Remote node ID received: " + sndId);

                    final UUID old = ses.addMeta(NODE_ID_META, sndId);

                    assert old == null;

                    final ClusterNode rmtNode = getSpiContext().node(sndId);

                    if (rmtNode == null) {
                        ses.close();

                        return;
                    }

                    ClusterNode locNode = getSpiContext().localNode();

                    if (ses.remoteAddress() == null)
                        return;

                    GridCommunicationClient oldClient = clients.get(sndId);

                    if (oldClient != null) {
                        if (oldClient instanceof GridTcpNioCommunicationClient) {
                            if (log.isDebugEnabled())
                                log.debug("Received incoming connection when already connected " +
                                    "to this node, rejecting [locNode=" + locNode.id() +
                                    ", rmtNode=" + sndId + ']');

                            ses.send(new RecoveryLastReceivedMessage(-1));

                            return;
                        }
                    }

                    GridFutureAdapter<GridCommunicationClient> fut = new GridFutureAdapter<>();

                    GridFutureAdapter<GridCommunicationClient> oldFut = clientFuts.putIfAbsent(sndId, fut);

                    assert msg instanceof HandshakeMessage : msg;

                    HandshakeMessage msg0 = (HandshakeMessage)msg;

                    final GridNioRecoveryDescriptor recoveryDesc = recoveryDescriptor(rmtNode);

                    if (oldFut == null) {
                        oldClient = clients.get(sndId);

                        if (oldClient != null) {
                            if (oldClient instanceof GridTcpNioCommunicationClient) {
                                if (log.isDebugEnabled())
                                    log.debug("Received incoming connection when already connected " +
                                        "to this node, rejecting [locNode=" + locNode.id() +
                                        ", rmtNode=" + sndId + ']');

                                ses.send(new RecoveryLastReceivedMessage(-1));

                                return;
                            }
                        }

                        boolean reserved = recoveryDesc.tryReserve(msg0.connectCount(),
                            new ConnectClosure(ses, recoveryDesc, rmtNode, msg0, fut));

                        if (log.isDebugEnabled())
                            log.debug("Received incoming connection from remote node " +
                                "[rmtNode=" + rmtNode.id() + ", reserved=" + reserved + ']');

                        if (reserved) {
                            try {
                                GridTcpNioCommunicationClient client =
                                    connected(recoveryDesc, ses, rmtNode, msg0.received(), true);

                                fut.onDone(client);
                            }
                            finally {
                                clientFuts.remove(rmtNode.id(), fut);
                            }
                        }
                    }
                    else {
                        if (oldFut instanceof ConnectFuture && locNode.order() < rmtNode.order()) {
                            if (log.isDebugEnabled()) {
                                log.debug("Received incoming connection from remote node while " +
                                    "connecting to this node, rejecting [locNode=" + locNode.id() +
                                    ", locNodeOrder=" + locNode.order() + ", rmtNode=" + rmtNode.id() +
                                    ", rmtNodeOrder=" + rmtNode.order() + ']');
                            }

                            ses.send(new RecoveryLastReceivedMessage(-1));
                        }
                        else {
                            boolean reserved = recoveryDesc.tryReserve(msg0.connectCount(),
                                new ConnectClosure(ses, recoveryDesc, rmtNode, msg0, fut));

                            if (reserved) {
                                GridTcpNioCommunicationClient client =
                                    connected(recoveryDesc, ses, rmtNode, msg0.received(), true);

                                fut.onDone(client);
                            }
                        }
                    }
                }
                else {
                    rcvdMsgsCnt.increment();

                    GridNioRecoveryDescriptor recovery = ses.recoveryDescriptor();

                    if (recovery != null) {
                        if (msg instanceof RecoveryLastReceivedMessage) {
                            RecoveryLastReceivedMessage msg0 = (RecoveryLastReceivedMessage)msg;

                            if (log.isDebugEnabled())
                                log.debug("Received recovery acknowledgement [rmtNode=" + sndId +
                                    ", rcvCnt=" + msg0.received() + ']');

                            recovery.ackReceived(msg0.received());

                            return;
                        }
                        else {
                            long rcvCnt = recovery.onReceived();

                            if (rcvCnt % ackSndThreshold == 0) {
                                if (log.isDebugEnabled())
                                    log.debug("Send recovery acknowledgement [rmtNode=" + sndId +
                                        ", rcvCnt=" + rcvCnt + ']');

                                nioSrvr.sendSystem(ses, new RecoveryLastReceivedMessage(rcvCnt));

                                recovery.lastAcknowledged(rcvCnt);
                            }
                        }
                    }

                    IgniteRunnable c;

                    if (msgQueueLimit > 0) {
                        GridNioMessageTracker tracker = ses.meta(TRACKER_META);

                        if (tracker == null) {
                            GridNioMessageTracker old = ses.addMeta(TRACKER_META, tracker =
                                new GridNioMessageTracker(ses, msgQueueLimit));

                            assert old == null;
                        }

                        tracker.onMessageReceived();

                        c = tracker;
                    }
                    else
                        c = NOOP;

                    notifyListener(sndId, msg, c);
                }
            }

            /**
             * @param recovery Recovery descriptor.
             * @param ses Session.
             * @param node Node.
             * @param rcvCnt Number of received messages..
             * @param sndRes If {@code true} sends response for recovery handshake.
             * @return Client.
             */
            private GridTcpNioCommunicationClient connected(
                GridNioRecoveryDescriptor recovery,
                GridNioSession ses,
                ClusterNode node,
                long rcvCnt,
                boolean sndRes) {
                recovery.onHandshake(rcvCnt);

                ses.recoveryDescriptor(recovery);

                nioSrvr.resend(ses);

                if (sndRes)
                    nioSrvr.sendSystem(ses, new RecoveryLastReceivedMessage(recovery.receivedCount()));

                recovery.connected();

                GridTcpNioCommunicationClient client = new GridTcpNioCommunicationClient(ses, log);

                GridCommunicationClient oldClient = clients.putIfAbsent(node.id(), client);

                assert oldClient == null : "Client already created [node=" + node + ", client=" + client +
                        ", oldClient=" + oldClient + ", recoveryDesc=" + recovery + ']';

                return client;
            }

            /**
             *
             */
            @SuppressWarnings("PackageVisibleInnerClass")
            class ConnectClosure implements IgniteInClosure<Boolean> {
                /** */
                private static final long serialVersionUID = 0L;

                /** */
                private final GridNioSession ses;

                /** */
                private final GridNioRecoveryDescriptor recoveryDesc;

                /** */
                private final ClusterNode rmtNode;

                /** */
                private final HandshakeMessage msg;

                /** */
                private final GridFutureAdapter<GridCommunicationClient> fut;

                /**
                 * @param ses Incoming session.
                 * @param recoveryDesc Recovery descriptor.
                 * @param rmtNode Remote node.
                 * @param msg Handshake message.
                 * @param fut Connect future.
                 */
                ConnectClosure(GridNioSession ses,
                    GridNioRecoveryDescriptor recoveryDesc,
                    ClusterNode rmtNode,
                    HandshakeMessage msg,
                    GridFutureAdapter<GridCommunicationClient> fut) {
                    this.ses = ses;
                    this.recoveryDesc = recoveryDesc;
                    this.rmtNode = rmtNode;
                    this.msg = msg;
                    this.fut = fut;
                }

                /** {@inheritDoc} */
                @Override public void apply(Boolean success) {
                    if (success) {
                        IgniteInClosure<IgniteInternalFuture<?>> lsnr = new IgniteInClosure<IgniteInternalFuture<?>>() {
                            @Override public void apply(IgniteInternalFuture<?> msgFut) {
                                try {
                                    msgFut.get();

                                    GridTcpNioCommunicationClient client =
                                        connected(recoveryDesc, ses, rmtNode, msg.received(), false);

                                    fut.onDone(client);
                                }
                                catch (IgniteCheckedException e) {
                                    if (log.isDebugEnabled())
                                        log.debug("Failed to send recovery handshake " +
                                            "[rmtNode=" + rmtNode.id() + ", err=" + e + ']');

                                    recoveryDesc.release();

                                    fut.onDone();
                                }
                                finally {
                                    clientFuts.remove(rmtNode.id(), fut);
                                }
                            }
                        };

                        nioSrvr.sendSystem(ses, new RecoveryLastReceivedMessage(recoveryDesc.receivedCount()), lsnr);
                    }
                    else {
                        try {
                            fut.onDone();
                        }
                        finally {
                            clientFuts.remove(rmtNode.id(), fut);
                        }
                    }
                }
            }
        };

    /** Logger. */
    @LoggerResource
    private IgniteLogger log;

    /** Local IP address. */
    private String locAddr;

    /** Complex variable that represents this node IP address. */
    private volatile InetAddress locHost;

    /** Local port which node uses. */
    private int locPort = DFLT_PORT;

    /** Local port range. */
    private int locPortRange = DFLT_PORT_RANGE;

    /** Grid name. */
    private String gridName;

    /** Allocate direct buffer or heap buffer. */
    private boolean directBuf = true;

    /** Allocate direct buffer or heap buffer. */
    private boolean directSndBuf;

    /** Idle connection timeout. */
    private long idleConnTimeout = DFLT_IDLE_CONN_TIMEOUT;

    /** Connection buffer flush frequency. */
    private volatile long connBufFlushFreq = DFLT_CONN_BUF_FLUSH_FREQ;

    /** Connection buffer size. */
    @SuppressWarnings("RedundantFieldInitialization")
    private int connBufSize = DFLT_CONN_BUF_SIZE;

    /** Connect timeout. */
    private long connTimeout = DFLT_CONN_TIMEOUT;

    /** Maximum connect timeout. */
    private long maxConnTimeout = DFLT_MAX_CONN_TIMEOUT;

    /** Reconnect attempts count. */
    @SuppressWarnings({"FieldAccessedSynchronizedAndUnsynchronized"})
    private int reconCnt = DFLT_RECONNECT_CNT;

    /** Socket send buffer. */
    private int sockSndBuf = DFLT_SOCK_BUF_SIZE;

    /** Socket receive buffer. */
    private int sockRcvBuf = DFLT_SOCK_BUF_SIZE;

    /** Message queue limit. */
    private int msgQueueLimit = DFLT_MSG_QUEUE_LIMIT;

    /** Min buffered message count. */
    private int minBufferedMsgCnt = Integer.getInteger(IGNITE_MIN_BUFFERED_COMMUNICATION_MSG_CNT, 512);

    /** Buffer size ratio. */
    private double bufSizeRatio = IgniteSystemProperties.getDouble(IGNITE_COMMUNICATION_BUF_RESIZE_RATIO, 0.8);

    /** NIO server. */
    private GridNioServer<Message> nioSrvr;

    /** {@code TCP_NODELAY} option value for created sockets. */
    private boolean tcpNoDelay = DFLT_TCP_NODELAY;

    /** Number of received messages after which acknowledgment is sent. */
    private int ackSndThreshold = DFLT_ACK_SND_THRESHOLD;

    /** Maximum number of unacknowledged messages. */
    private int unackedMsgsBufSize;

    /** Socket write timeout. */
    private long sockWriteTimeout = DFLT_SOCK_WRITE_TIMEOUT;

    /** Idle client worker. */
    private IdleClientWorker idleClientWorker;

    /** Flush client worker. */
    private ClientFlushWorker clientFlushWorker;

    /** Socket timeout worker. */
    private SocketTimeoutWorker sockTimeoutWorker;

    /** Recovery worker. */
    private RecoveryWorker recoveryWorker;

    /** Clients. */
    private final ConcurrentMap<UUID, GridCommunicationClient> clients = GridConcurrentFactory.newMap();

    /** SPI listener. */
    private volatile CommunicationListener<Message> lsnr;

    /** Bound port. */
    private int boundTcpPort = -1;

    /** Count of selectors to use in TCP server. */
    private int selectorsCnt = DFLT_SELECTORS_CNT;

    /** Address resolver. */
    private AddressResolver addrRslvr;

    /** Local node ID message. */
    private NodeIdMessage nodeIdMsg;

    /** Received messages count. */
    private final LongAdder rcvdMsgsCnt = new LongAdder();

    /** Sent messages count.*/
    private final LongAdder sentMsgsCnt = new LongAdder();

    /** Received bytes count. */
    private final LongAdder rcvdBytesCnt = new LongAdder();

    /** Sent bytes count.*/
    private final LongAdder sentBytesCnt = new LongAdder();

    /** Context initialization latch. */
    private final CountDownLatch ctxInitLatch = new CountDownLatch(1);

    /** Stopping flag. */
    private volatile boolean stopping;

    /** metrics listener. */
    private final GridNioMetricsListener metricsLsnr = new GridNioMetricsListener() {
        @Override public void onBytesSent(int bytesCnt) {
            sentBytesCnt.add(bytesCnt);
        }

        @Override public void onBytesReceived(int bytesCnt) {
            rcvdBytesCnt.add(bytesCnt);
        }
    };

    /** Client connect futures. */
    private final ConcurrentMap<UUID, GridFutureAdapter<GridCommunicationClient>> clientFuts =
        GridConcurrentFactory.newMap();

    /** */
    private final ConcurrentMap<ClientKey, GridNioRecoveryDescriptor> recoveryDescs = GridConcurrentFactory.newMap();

    /** Discovery listener. */
    private final GridLocalEventListener discoLsnr = new GridLocalEventListener() {
        @Override public void onEvent(Event evt) {
            assert evt instanceof DiscoveryEvent;
            assert evt.type() == EVT_NODE_LEFT || evt.type() == EVT_NODE_FAILED;

            onNodeLeft(((DiscoveryEvent)evt).eventNode().id());
        }
    };

    /**
     * Sets address resolver.
     *
     * @param addrRslvr Address resolver.
     */
    @IgniteSpiConfiguration(optional = true)
    public void setAddressResolver(AddressResolver addrRslvr) {
        // Injection should not override value already set by Spring or user.
        if (this.addrRslvr == null)
            this.addrRslvr = addrRslvr;
    }

    /**
     * Injects resources.
     *
     * @param ignite Ignite.
     */
    @IgniteInstanceResource
    protected void injectResources(Ignite ignite) {
        if (ignite != null) {
            setAddressResolver(ignite.configuration().getAddressResolver());
            setLocalAddress(ignite.configuration().getLocalHost());
            gridName = ignite.name();
        }
    }

    /**
     * Sets local host address for socket binding. Note that one node could have
     * additional addresses beside the loopback one. This configuration
     * parameter is optional.
     *
     * @param locAddr IP address. Default value is any available local
     *      IP address.
     */
    @IgniteSpiConfiguration(optional = true)
    public void setLocalAddress(String locAddr) {
        // Injection should not override value already set by Spring or user.
        if (this.locAddr == null)
            this.locAddr = locAddr;
    }

    /** {@inheritDoc} */
    @Override public String getLocalAddress() {
        return locAddr;
    }

    /**
     * Sets local port for socket binding.
     * <p>
     * If not provided, default value is {@link #DFLT_PORT}.
     *
     * @param locPort Port number.
     */
    @IgniteSpiConfiguration(optional = true)
    public void setLocalPort(int locPort) {
        this.locPort = locPort;
    }

    /** {@inheritDoc} */
    @Override public int getLocalPort() {
        return locPort;
    }

    /**
     * Sets local port range for local host ports (value must greater than or equal to <tt>0</tt>).
     * If provided local port (see {@link #setLocalPort(int)}} is occupied,
     * implementation will try to increment the port number for as long as it is less than
     * initial value plus this range.
     * <p>
     * If port range value is <tt>0</tt>, then implementation will try bind only to the port provided by
     * {@link #setLocalPort(int)} method and fail if binding to this port did not succeed.
     * <p>
     * Local port range is very useful during development when more than one grid nodes need to run
     * on the same physical machine.
     * <p>
     * If not provided, default value is {@link #DFLT_PORT_RANGE}.
     *
     * @param locPortRange New local port range.
     */
    @IgniteSpiConfiguration(optional = true)
    public void setLocalPortRange(int locPortRange) {
        this.locPortRange = locPortRange;
    }

    /** {@inheritDoc} */
    @Override public int getLocalPortRange() {
        return locPortRange;
    }

    /**
     * Sets maximum idle connection timeout upon which a connection
     * to client will be closed.
     * <p>
     * If not provided, default value is {@link #DFLT_IDLE_CONN_TIMEOUT}.
     *
     * @param idleConnTimeout Maximum idle connection time.
     */
    @IgniteSpiConfiguration(optional = true)
    public void setIdleConnectionTimeout(long idleConnTimeout) {
        this.idleConnTimeout = idleConnTimeout;
    }

    /** {@inheritDoc} */
    @Override public long getIdleConnectionTimeout() {
        return idleConnTimeout;
    }

    /** {@inheritDoc} */
    @Override public long getSocketWriteTimeout() {
        return sockWriteTimeout;
    }

    /**
     * Sets socket write timeout for TCP connection. If message can not be written to
     * socket within this time then connection is closed and reconnect is attempted.
     * <p>
     * Default to {@link #DFLT_SOCK_WRITE_TIMEOUT}.
     *
     * @param sockWriteTimeout Socket write timeout for TCP connection.
     */
    @IgniteSpiConfiguration(optional = true)
    public void setSocketWriteTimeout(long sockWriteTimeout) {
        this.sockWriteTimeout = sockWriteTimeout;
    }

    /** {@inheritDoc} */
    @Override public int getAckSendThreshold() {
        return ackSndThreshold;
    }

    /**
     * Sets number of received messages per connection to node after which acknowledgment message is sent.
     * <p>
     * Default to {@link #DFLT_ACK_SND_THRESHOLD}.
     *
     * @param ackSndThreshold Number of received messages after which acknowledgment is sent.
     */
    @IgniteSpiConfiguration(optional = true)
    public void setAckSendThreshold(int ackSndThreshold) {
        this.ackSndThreshold = ackSndThreshold;
    }

    /** {@inheritDoc} */
    @Override public int getUnacknowledgedMessagesBufferSize() {
        return unackedMsgsBufSize;
    }

    /**
     * Sets maximum number of stored unacknowledged messages per connection to node.
     * If number of unacknowledged messages exceeds this number then connection to node is
     * closed and reconnect is attempted.
     *
     * @param unackedMsgsBufSize Maximum number of unacknowledged messages.
     */
    @IgniteSpiConfiguration(optional = true)
    public void setUnacknowledgedMessagesBufferSize(int unackedMsgsBufSize) {
        this.unackedMsgsBufSize = unackedMsgsBufSize;
    }

    /**
     * Sets connection buffer size. If set to {@code 0} connection buffer is disabled.
     * <p>
     * If not provided, default value is {@link #DFLT_CONN_BUF_SIZE}.
     *
     * @param connBufSize Connection buffer size.
     * @see #setConnectionBufferFlushFrequency(long)
     */
    @IgniteSpiConfiguration(optional = true)
    public void setConnectionBufferSize(int connBufSize) {
        this.connBufSize = connBufSize;
    }

    /** {@inheritDoc} */
    @Override public int getConnectionBufferSize() {
        return connBufSize;
    }

    /** {@inheritDoc} */
    @IgniteSpiConfiguration(optional = true)
    @Override public void setConnectionBufferFlushFrequency(long connBufFlushFreq) {
        this.connBufFlushFreq = connBufFlushFreq;
    }

    /** {@inheritDoc} */
    @Override public long getConnectionBufferFlushFrequency() {
        return connBufFlushFreq;
    }

    /**
     * Sets connect timeout used when establishing connection
     * with remote nodes.
     * <p>
     * {@code 0} is interpreted as infinite timeout.
     * <p>
     * If not provided, default value is {@link #DFLT_CONN_TIMEOUT}.
     *
     * @param connTimeout Connect timeout.
     */
    @IgniteSpiConfiguration(optional = true)
    public void setConnectTimeout(long connTimeout) {
        this.connTimeout = connTimeout;
    }

    /** {@inheritDoc} */
    @Override public long getConnectTimeout() {
        return connTimeout;
    }

    /**
     * Sets maximum connect timeout. If handshake is not established within connect timeout,
     * then SPI tries to repeat handshake procedure with increased connect timeout.
     * Connect timeout can grow till maximum timeout value,
     * if maximum timeout value is reached then the handshake is considered as failed.
     * <p>
     * {@code 0} is interpreted as infinite timeout.
     * <p>
     * If not provided, default value is {@link #DFLT_MAX_CONN_TIMEOUT}.
     *
     * @param maxConnTimeout Maximum connect timeout.
     */
    @IgniteSpiConfiguration(optional = true)
    public void setMaxConnectTimeout(long maxConnTimeout) {
        this.maxConnTimeout = maxConnTimeout;
    }

    /** {@inheritDoc} */
    @Override public long getMaxConnectTimeout() {
        return maxConnTimeout;
    }

    /**
     * Sets maximum number of reconnect attempts used when establishing connection
     * with remote nodes.
     * <p>
     * If not provided, default value is {@link #DFLT_RECONNECT_CNT}.
     *
     * @param reconCnt Maximum number of reconnection attempts.
     */
    @IgniteSpiConfiguration(optional = true)
    public void setReconnectCount(int reconCnt) {
        this.reconCnt = reconCnt;
    }

    /** {@inheritDoc} */
    @Override public int getReconnectCount() {
        return reconCnt;
    }

    /**
     * Sets flag to allocate direct or heap buffer in SPI.
     * If value is {@code true}, then SPI will use {@link ByteBuffer#allocateDirect(int)} call.
     * Otherwise, SPI will use {@link ByteBuffer#allocate(int)} call.
     * <p>
     * If not provided, default value is {@code true}.
     *
     * @param directBuf Flag indicates to allocate direct or heap buffer in SPI.
     */
    @IgniteSpiConfiguration(optional = true)
    public void setDirectBuffer(boolean directBuf) {
        this.directBuf = directBuf;
    }

    /** {@inheritDoc} */
    @Override public boolean isDirectBuffer() {
        return directBuf;
    }

    /** {@inheritDoc} */
    @Override public boolean isDirectSendBuffer() {
        return directSndBuf;
    }

    /**
     * Sets whether to use direct buffer for sending.
     * <p>
     * If not provided default is {@code false}.
     *
     * @param directSndBuf {@code True} to use direct buffers for send.
     */
    @IgniteSpiConfiguration(optional = true)
    public void setDirectSendBuffer(boolean directSndBuf) {
        this.directSndBuf = directSndBuf;
    }

    /**
     * Sets the count of selectors te be used in TCP server.
     * <p/>
     * If not provided, default value is {@link #DFLT_SELECTORS_CNT}.
     *
     * @param selectorsCnt Selectors count.
     */
    @IgniteSpiConfiguration(optional = true)
    public void setSelectorsCount(int selectorsCnt) {
        this.selectorsCnt = selectorsCnt;
    }

    /** {@inheritDoc} */
    @Override public int getSelectorsCount() {
        return selectorsCnt;
    }

    /**
     * Sets value for {@code TCP_NODELAY} socket option. Each
     * socket will be opened using provided value.
     * <p>
     * Setting this option to {@code true} disables Nagle's algorithm
     * for socket decreasing latency and delivery time for small messages.
     * <p>
     * For systems that work under heavy network load it is advisable to
     * set this value to {@code false}.
     * <p>
     * If not provided, default value is {@link #DFLT_TCP_NODELAY}.
     *
     * @param tcpNoDelay {@code True} to disable TCP delay.
     */
    @IgniteSpiConfiguration(optional = true)
    public void setTcpNoDelay(boolean tcpNoDelay) {
        this.tcpNoDelay = tcpNoDelay;
    }

    /** {@inheritDoc} */
    @Override public boolean isTcpNoDelay() {
        return tcpNoDelay;
    }

    /**
     * Sets receive buffer size for sockets created or accepted by this SPI.
     * <p>
     * If not provided, default is {@link #DFLT_SOCK_BUF_SIZE}.
     *
     * @param sockRcvBuf Socket receive buffer size.
     */
    @IgniteSpiConfiguration(optional = true)
    public void setSocketReceiveBuffer(int sockRcvBuf) {
        this.sockRcvBuf = sockRcvBuf;
    }

    /** {@inheritDoc} */
    @Override public int getSocketReceiveBuffer() {
        return sockRcvBuf;
    }

    /**
     * Sets send buffer size for sockets created or accepted by this SPI.
     * <p>
     * If not provided, default is {@link #DFLT_SOCK_BUF_SIZE}.
     *
     * @param sockSndBuf Socket send buffer size.
     */
    @IgniteSpiConfiguration(optional = true)
    public void setSocketSendBuffer(int sockSndBuf) {
        this.sockSndBuf = sockSndBuf;
    }

    /** {@inheritDoc} */
    @Override public int getSocketSendBuffer() {
        return sockSndBuf;
    }

    /**
     * Sets message queue limit for incoming and outgoing messages.
     * <p>
     * When set to positive number send queue is limited to the configured value.
     * {@code 0} disables the size limitations.
     * <p>
     * If not provided, default is {@link #DFLT_MSG_QUEUE_LIMIT}.
     *
     * @param msgQueueLimit Send queue size limit.
     */
    @IgniteSpiConfiguration(optional = true)
    public void setMessageQueueLimit(int msgQueueLimit) {
        this.msgQueueLimit = msgQueueLimit;
    }

    /** {@inheritDoc} */
    @Override public int getMessageQueueLimit() {
        return msgQueueLimit;
    }

    /**
     * Sets the minimum number of messages for this SPI, that are buffered
     * prior to sending.
     * <p>
     * Defaults to either {@code 512} or {@link IgniteSystemProperties#IGNITE_MIN_BUFFERED_COMMUNICATION_MSG_CNT}
     * system property (if specified).
     *
     * @param minBufferedMsgCnt Minimum buffered message count.
     */
    @IgniteSpiConfiguration(optional = true)
    public void setMinimumBufferedMessageCount(int minBufferedMsgCnt) {
        this.minBufferedMsgCnt = minBufferedMsgCnt;
    }

    /** {@inheritDoc} */
    @Override public int getMinimumBufferedMessageCount() {
        return minBufferedMsgCnt;
    }

    /**
     * Sets the buffer size ratio for this SPI. As messages are sent,
     * the buffer size is adjusted using this ratio.
     * <p>
     * Defaults to either {@code 0.8} or {@link IgniteSystemProperties#IGNITE_COMMUNICATION_BUF_RESIZE_RATIO}
     * system property (if specified).
     *
     * @param bufSizeRatio Buffer size ratio.
     */
    @IgniteSpiConfiguration(optional = true)
    public void setBufferSizeRatio(double bufSizeRatio) {
        this.bufSizeRatio = bufSizeRatio;
    }

    /** {@inheritDoc} */
    @Override public double getBufferSizeRatio() {
        return bufSizeRatio;
    }

    /** {@inheritDoc} */
    @Override public void setListener(CommunicationListener<Message> lsnr) {
        this.lsnr = lsnr;
    }

    /**
     * @return Listener.
     */
    public CommunicationListener getListener() {
        return lsnr;
    }

    /** {@inheritDoc} */
    @Override public int getSentMessagesCount() {
        return sentMsgsCnt.intValue();
    }

    /** {@inheritDoc} */
    @Override public long getSentBytesCount() {
        return sentBytesCnt.longValue();
    }

    /** {@inheritDoc} */
    @Override public int getReceivedMessagesCount() {
        return rcvdMsgsCnt.intValue();
    }

    /** {@inheritDoc} */
    @Override public long getReceivedBytesCount() {
        return rcvdBytesCnt.longValue();
    }

    /** {@inheritDoc} */
    @Override public int getOutboundMessagesQueueSize() {
        return nioSrvr.outboundMessagesQueueSize();
    }

    /** {@inheritDoc} */
    @Override public void resetMetrics() {
        // Can't use 'reset' method because it is not thread-safe
        // according to javadoc.
        sentMsgsCnt.add(-sentMsgsCnt.sum());
        rcvdMsgsCnt.add(-rcvdMsgsCnt.sum());
        sentBytesCnt.add(-sentBytesCnt.sum());
        rcvdBytesCnt.add(-rcvdBytesCnt.sum());
    }

    /** {@inheritDoc} */
    @Override public Map<String, Object> getNodeAttributes() throws IgniteSpiException {
        nodeIdMsg = new NodeIdMessage(ignite.configuration().getNodeId());

        assertParameter(locPort > 1023, "locPort > 1023");
        assertParameter(locPort <= 0xffff, "locPort < 0xffff");
        assertParameter(locPortRange >= 0, "locPortRange >= 0");
        assertParameter(idleConnTimeout > 0, "idleConnTimeout > 0");
        assertParameter(connBufFlushFreq > 0, "connBufFlushFreq > 0");
        assertParameter(connBufSize >= 0, "connBufSize >= 0");
        assertParameter(sockRcvBuf >= 0, "sockRcvBuf >= 0");
        assertParameter(sockSndBuf >= 0, "sockSndBuf >= 0");
        assertParameter(msgQueueLimit >= 0, "msgQueueLimit >= 0");
        assertParameter(reconCnt > 0, "reconnectCnt > 0");
        assertParameter(selectorsCnt > 0, "selectorsCnt > 0");
        assertParameter(minBufferedMsgCnt >= 0, "minBufferedMsgCnt >= 0");
        assertParameter(bufSizeRatio > 0 && bufSizeRatio < 1, "bufSizeRatio > 0 && bufSizeRatio < 1");
        assertParameter(connTimeout >= 0, "connTimeout >= 0");
        assertParameter(maxConnTimeout >= connTimeout, "maxConnTimeout >= connTimeout");
        assertParameter(sockWriteTimeout >= 0, "sockWriteTimeout >= 0");
        assertParameter(ackSndThreshold > 0, "ackSndThreshold > 0");
        assertParameter(unackedMsgsBufSize >= 0, "unackedMsgsBufSize >= 0");

        if (unackedMsgsBufSize > 0) {
            assertParameter(unackedMsgsBufSize >= msgQueueLimit * 5,
                "Specified 'unackedMsgsBufSize' is too low, it should be at least 'msgQueueLimit * 5'.");

            assertParameter(unackedMsgsBufSize >= ackSndThreshold * 5,
                "Specified 'unackedMsgsBufSize' is too low, it should be at least 'ackSndThreshold * 5'.");
        }

        try {
            locHost = U.resolveLocalHost(locAddr);
        }
        catch (IOException e) {
            throw new IgniteSpiException("Failed to initialize local address: " + locAddr, e);
        }

        try {
            // This method potentially resets local port to the value
            // local node was bound to.
            nioSrvr = resetNioServer();
        }
        catch (IgniteCheckedException e) {
            throw new IgniteSpiException("Failed to initialize TCP server: " + locHost, e);
        }

        // Set local node attributes.
        try {
            IgniteBiTuple<Collection<String>, Collection<String>> addrs = U.resolveLocalAddresses(locHost);

            Collection<InetSocketAddress> extAddrs = addrRslvr == null ? null :
                U.resolveAddresses(addrRslvr, F.flat(Arrays.asList(addrs.get1(), addrs.get2())), boundTcpPort);

            return F.asMap(
                createSpiAttributeName(ATTR_ADDRS), addrs.get1(),
                createSpiAttributeName(ATTR_HOST_NAMES), addrs.get2(),
                createSpiAttributeName(ATTR_PORT), boundTcpPort,
                createSpiAttributeName(ATTR_EXT_ADDRS), extAddrs);
        }
        catch (IOException | IgniteCheckedException e) {
            throw new IgniteSpiException("Failed to resolve local host to addresses: " + locHost, e);
        }
    }

    /** {@inheritDoc} */
    @Override public void spiStart(String gridName) throws IgniteSpiException {
        assert locHost != null;

        // Start SPI start stopwatch.
        startStopwatch();

        // Ack parameters.
        if (log.isDebugEnabled()) {
            log.debug(configInfo("locAddr", locAddr));
            log.debug(configInfo("locPort", locPort));
            log.debug(configInfo("locPortRange", locPortRange));
            log.debug(configInfo("idleConnTimeout", idleConnTimeout));
            log.debug(configInfo("directBuf", directBuf));
            log.debug(configInfo("directSendBuf", directSndBuf));
            log.debug(configInfo("connBufSize", connBufSize));
            log.debug(configInfo("connBufFlushFreq", connBufFlushFreq));
            log.debug(configInfo("selectorsCnt", selectorsCnt));
            log.debug(configInfo("tcpNoDelay", tcpNoDelay));
            log.debug(configInfo("sockSndBuf", sockSndBuf));
            log.debug(configInfo("sockRcvBuf", sockRcvBuf));
            log.debug(configInfo("msgQueueLimit", msgQueueLimit));
            log.debug(configInfo("minBufferedMsgCnt", minBufferedMsgCnt));
            log.debug(configInfo("bufSizeRatio", bufSizeRatio));
            log.debug(configInfo("connTimeout", connTimeout));
            log.debug(configInfo("maxConnTimeout", maxConnTimeout));
            log.debug(configInfo("reconCnt", reconCnt));
            log.debug(configInfo("sockWriteTimeout", sockWriteTimeout));
            log.debug(configInfo("ackSndThreshold", ackSndThreshold));
            log.debug(configInfo("unackedMsgsBufSize", unackedMsgsBufSize));
        }

        if (connBufSize > 8192)
            U.warn(log, "Specified communication IO buffer size is larger than recommended (ignore if done " +
                "intentionally) [specified=" + connBufSize + ", recommended=8192]",
                "Specified communication IO buffer size is larger than recommended (ignore if done intentionally).");

        if (!tcpNoDelay)
            U.quietAndWarn(log, "'TCP_NO_DELAY' for communication is off, which should be used with caution " +
                "since may produce significant delays with some scenarios.");

        registerMBean(gridName, this, TcpCommunicationSpiMBean.class);

        nioSrvr.start();

        idleClientWorker = new IdleClientWorker();

        idleClientWorker.start();

        recoveryWorker = new RecoveryWorker();

        recoveryWorker.start();

        if (connBufSize > 0) {
            clientFlushWorker = new ClientFlushWorker();

            clientFlushWorker.start();
        }

        sockTimeoutWorker = new SocketTimeoutWorker();

        sockTimeoutWorker.start();

        // Ack start.
        if (log.isDebugEnabled())
            log.debug(startInfo());
    }

    /** {@inheritDoc} }*/
    @Override public void onContextInitialized0(IgniteSpiContext spiCtx) throws IgniteSpiException {
        spiCtx.registerPort(boundTcpPort, IgnitePortProtocol.TCP);

        spiCtx.addLocalEventListener(discoLsnr, EVT_NODE_LEFT, EVT_NODE_FAILED);

        ctxInitLatch.countDown();
    }

    /** {@inheritDoc} */
    @Override public IgniteSpiContext getSpiContext() {
        if (ctxInitLatch.getCount() > 0) {
            if (log.isDebugEnabled())
                log.debug("Waiting for context initialization.");

            try {
                U.await(ctxInitLatch);

                if (log.isDebugEnabled())
                    log.debug("Context has been initialized.");
            }
            catch (IgniteInterruptedCheckedException e) {
                U.warn(log, "Thread has been interrupted while waiting for SPI context initialization.", e);
            }
        }

        return super.getSpiContext();
    }

    /**
     * Recreates tpcSrvr socket instance.
     *
     * @return Server instance.
     * @throws IgniteCheckedException Thrown if it's not possible to create server.
     */
    private GridNioServer<Message> resetNioServer() throws IgniteCheckedException {
        if (boundTcpPort >= 0)
            throw new IgniteCheckedException("Tcp NIO server was already created on port " + boundTcpPort);

        IgniteCheckedException lastEx = null;

        // If configured TCP port is busy, find first available in range.
        for (int port = locPort; port < locPort + locPortRange; port++) {
            try {
                MessageFactory messageFactory = new MessageFactory() {
                    private MessageFactory impl;

                    @Nullable @Override public Message create(byte type) {
                        if (impl == null)
                            impl = getSpiContext().messageFactory();

                        assert impl != null;

                        return impl.create(type);
                    }
                };

                MessageFormatter messageFormatter = new MessageFormatter() {
                    private MessageFormatter impl;

                    @Override public MessageWriter writer() {
                        if (impl == null)
                            impl = getSpiContext().messageFormatter();

                        assert impl != null;

                        return impl.writer();
                    }

                    @Override public MessageReader reader(MessageFactory factory) {
                        if (impl == null)
                            impl = getSpiContext().messageFormatter();

                        assert impl != null;

                        return impl.reader(factory);
                    }
                };

                GridDirectParser parser = new GridDirectParser(messageFactory, messageFormatter);

                IgnitePredicate<Message> skipRecoveryPred = new IgnitePredicate<Message>() {
                    @Override public boolean apply(Message msg) {
                        return msg instanceof RecoveryLastReceivedMessage;
                    }
                };

                GridNioServer<Message> srvr =
                    GridNioServer.<Message>builder()
                        .address(locHost)
                        .port(port)
                        .listener(srvLsnr)
                        .logger(log)
                        .selectorCount(selectorsCnt)
                        .gridName(gridName)
                        .tcpNoDelay(tcpNoDelay)
                        .directBuffer(directBuf)
                        .byteOrder(ByteOrder.nativeOrder())
                        .socketSendBufferSize(sockSndBuf)
                        .socketReceiveBufferSize(sockRcvBuf)
                        .sendQueueLimit(msgQueueLimit)
                        .directMode(true)
                        .metricsListener(metricsLsnr)
                        .writeTimeout(sockWriteTimeout)
                        .filters(new GridNioCodecFilter(parser, log, true),
                            new GridConnectionBytesVerifyFilter(log))
                        .messageFormatter(messageFormatter)
                        .skipRecoveryPredicate(skipRecoveryPred)
                        .build();

                boundTcpPort = port;

                // Ack Port the TCP server was bound to.
                if (log.isInfoEnabled())
                    log.info("Successfully bound to TCP port [port=" + boundTcpPort +
                        ", locHost=" + locHost + ']');

                srvr.idleTimeout(idleConnTimeout);

                return srvr;
            }
            catch (IgniteCheckedException e) {
                lastEx = e;

                if (log.isDebugEnabled())
                    log.debug("Failed to bind to local port (will try next port within range) [port=" + port +
                        ", locHost=" + locHost + ']');

                onException("Failed to bind to local port (will try next port within range) [port=" + port +
                    ", locHost=" + locHost + ']', e);
            }
        }

        // If free port wasn't found.
        throw new IgniteCheckedException("Failed to bind to any port within range [startPort=" + locPort +
            ", portRange=" + locPortRange + ", locHost=" + locHost + ']', lastEx);
    }

    /** {@inheritDoc} */
    @Override public void spiStop() throws IgniteSpiException {
        assert stopping;

        unregisterMBean();

        // Stop TCP server.
        if (nioSrvr != null)
            nioSrvr.stop();

        U.interrupt(idleClientWorker);
        U.interrupt(clientFlushWorker);
        U.interrupt(sockTimeoutWorker);
        U.interrupt(recoveryWorker);

        U.join(idleClientWorker, log);
        U.join(clientFlushWorker, log);
        U.join(sockTimeoutWorker, log);
        U.join(recoveryWorker, log);

        // Force closing on stop (safety).
        for (GridCommunicationClient client : clients.values())
            client.forceClose();

        // Clear resources.
        nioSrvr = null;
        idleClientWorker = null;

        boundTcpPort = -1;

        // Ack stop.
        if (log.isDebugEnabled())
            log.debug(stopInfo());
    }

    /** {@inheritDoc} */
    @Override protected void onContextDestroyed0() {
        stopping = true;

        if (ctxInitLatch.getCount() > 0)
            // Safety.
            ctxInitLatch.countDown();

        // Force closing.
        for (GridCommunicationClient client : clients.values())
            client.forceClose();

        getSpiContext().deregisterPorts();

        getSpiContext().removeLocalEventListener(discoLsnr);
    }

    /**
     * @param nodeId Left node ID.
     */
    void onNodeLeft(UUID nodeId) {
        assert nodeId != null;

        GridCommunicationClient client = clients.get(nodeId);

        if (client != null) {
            if (log.isDebugEnabled())
                log.debug("Forcing NIO client close since node has left [nodeId=" + nodeId +
                    ", client=" + client + ']');

            client.forceClose();

            clients.remove(nodeId, client);
        }
    }

    /** {@inheritDoc} */
    @Override protected void checkConfigurationConsistency0(IgniteSpiContext spiCtx, ClusterNode node, boolean starting)
        throws IgniteSpiException {
        // These attributes are set on node startup in any case, so we MUST receive them.
        checkAttributePresence(node, createSpiAttributeName(ATTR_ADDRS));
        checkAttributePresence(node, createSpiAttributeName(ATTR_HOST_NAMES));
        checkAttributePresence(node, createSpiAttributeName(ATTR_PORT));
    }

    /**
     * Checks that node has specified attribute and prints warning if it does not.
     *
     * @param node Node to check.
     * @param attrName Name of the attribute.
     */
    private void checkAttributePresence(ClusterNode node, String attrName) {
        if (node.attribute(attrName) == null)
            U.warn(log, "Remote node has inconsistent configuration (required attribute was not found) " +
                "[attrName=" + attrName + ", nodeId=" + node.id() +
                "spiCls=" + U.getSimpleName(TcpCommunicationSpi.class) + ']');
    }

    /** {@inheritDoc} */
    @Override public void sendMessage(ClusterNode node, Message msg) throws IgniteSpiException {
        assert node != null;
        assert msg != null;

        if (log.isTraceEnabled())
            log.trace("Sending message to node [node=" + node + ", msg=" + msg + ']');

        UUID locNodeId = ignite.configuration().getNodeId();

        if (node.id().equals(locNodeId))
            notifyListener(locNodeId, msg, NOOP);
        else {
            GridCommunicationClient client = null;

            try {
                boolean retry;

                do {
                    client = reserveClient(node);

                    UUID nodeId = null;

                    if (!client.async() && !getSpiContext().localNode().version().equals(node.version()))
                        nodeId = node.id();

                    retry = client.sendMessage(nodeId, msg);

                    client.release();

                    client = null;

                    if (!retry)
                        sentMsgsCnt.increment();
                    else {
                        ClusterNode node0 = getSpiContext().node(node.id());

                        if (node0 == null)
                            throw new IgniteCheckedException("Failed to send message to remote node " +
                                "(node has left the grid): " + node.id());
                    }
                }
                while (retry);
            }
            catch (IgniteCheckedException e) {
                throw new IgniteSpiException("Failed to send message to remote node: " + node, e);
            }
            finally {
                if (client != null && clients.remove(node.id(), client))
                    client.forceClose();
            }
        }
    }

    /**
     * Returns existing or just created client to node.
     *
     * @param node Node to which client should be open.
     * @return The existing or just created client.
     * @throws IgniteCheckedException Thrown if any exception occurs.
     */
    private GridCommunicationClient reserveClient(ClusterNode node) throws IgniteCheckedException {
        assert node != null;

        UUID nodeId = node.id();

        while (true) {
            GridCommunicationClient client = clients.get(nodeId);

            if (client == null) {
                if (stopping)
                    throw new IgniteSpiException("Grid is stopping.");

                // Do not allow concurrent connects.
                GridFutureAdapter<GridCommunicationClient> fut = new ConnectFuture();

                GridFutureAdapter<GridCommunicationClient> oldFut = clientFuts.putIfAbsent(nodeId, fut);

                if (oldFut == null) {
                    try {
                        GridCommunicationClient client0 = clients.get(nodeId);

                        if (client0 == null) {
                            client0 = createNioClient(node);

                            if (client0 != null) {
                                GridCommunicationClient old = clients.put(nodeId, client0);

                                assert old == null : "Client already created " +
                                        "[node=" + node + ", client=" + client0 + ", oldClient=" + old + ']';
                            }
                            else
                                U.sleep(200);
                        }

                        fut.onDone(client0);
                    }
                    catch (Throwable e) {
                        fut.onDone(e);

                        if (e instanceof Error)
                            throw (Error)e;
                    }
                    finally {
                        clientFuts.remove(nodeId, fut);
                    }
                }
                else
                    fut = oldFut;

                client = fut.get();

                if (client == null)
                    continue;

                if (getSpiContext().node(nodeId) == null) {
                    if (clients.remove(nodeId, client))
                        client.forceClose();

                    throw new IgniteSpiException("Destination node is not in topology: " + node.id());
                }
            }

            if (client.reserve())
                return client;
            else
                // Client has just been closed by idle worker. Help it and try again.
                clients.remove(nodeId, client);
        }
    }

    /**
     * @param node Node to create client for.
     * @return Client.
     * @throws IgniteCheckedException If failed.
     */
    @Nullable protected GridCommunicationClient createNioClient(ClusterNode node) throws IgniteCheckedException {
        assert node != null;

        if (getSpiContext().localNode() == null)
            throw new IgniteCheckedException("Failed to create NIO client (local node is stopping)");

        return createTcpClient(node);
    }

    /**
     * Establish TCP connection to remote node and returns client.
     *
     * @param node Remote node.
     * @return Client.
     * @throws IgniteCheckedException If failed.
     */
    protected GridCommunicationClient createTcpClient(ClusterNode node) throws IgniteCheckedException {
        Collection<String> rmtAddrs0 = node.attribute(createSpiAttributeName(ATTR_ADDRS));
        Collection<String> rmtHostNames0 = node.attribute(createSpiAttributeName(ATTR_HOST_NAMES));
        Integer boundPort = node.attribute(createSpiAttributeName(ATTR_PORT));
        Collection<InetSocketAddress> extAddrs = node.attribute(createSpiAttributeName(ATTR_EXT_ADDRS));

        boolean isRmtAddrsExist = (!F.isEmpty(rmtAddrs0) && boundPort != null);
        boolean isExtAddrsExist = !F.isEmpty(extAddrs);

        if (!isRmtAddrsExist && !isExtAddrsExist)
            throw new IgniteCheckedException("Failed to send message to the destination node. Node doesn't have any " +
                "TCP communication addresses or mapped external addresses. Check configuration and make sure " +
                "that you use the same communication SPI on all nodes. Remote node id: " + node.id());

        List<InetSocketAddress> addrs;

        // Try to connect first on bound addresses.
        if (isRmtAddrsExist) {
            addrs = new ArrayList<>(U.toSocketAddresses(rmtAddrs0, rmtHostNames0, boundPort));

            boolean sameHost = U.sameMacs(getSpiContext().localNode(), node);

            Collections.sort(addrs, U.inetAddressesComparator(sameHost));
        }
        else
            addrs = new ArrayList<>();

        // Then on mapped external addresses.
        if (isExtAddrsExist)
            addrs.addAll(extAddrs);

        boolean conn = false;
        GridCommunicationClient client = null;
        IgniteCheckedException errs = null;

        int connectAttempts = 1;

        for (InetSocketAddress addr : addrs) {
            long connTimeout0 = connTimeout;

            int attempt = 1;

            while (!conn) { // Reconnection on handshake timeout.
                try {
                    SocketChannel ch = SocketChannel.open();

                    ch.configureBlocking(true);

                    ch.socket().setTcpNoDelay(tcpNoDelay);
                    ch.socket().setKeepAlive(true);

                    if (sockRcvBuf > 0)
                        ch.socket().setReceiveBufferSize(sockRcvBuf);

                    if (sockSndBuf > 0)
                        ch.socket().setSendBufferSize(sockSndBuf);

                    GridNioRecoveryDescriptor recoveryDesc = recoveryDescriptor(node);

                    if (!recoveryDesc.reserve()) {
                        U.closeQuiet(ch);

                        return null;
                    }

                    long rcvCnt = -1;

                    try {
                        ch.socket().connect(addr, (int)connTimeout);

                        rcvCnt = safeHandshake(ch, recoveryDesc, node.id(), connTimeout0);

                        if (rcvCnt == -1)
                            return null;
                    }
                    finally {
                        if (recoveryDesc != null && rcvCnt == -1)
                            recoveryDesc.release();
                    }

                    try {
                        Map<Integer, Object> meta = new HashMap<>();

                        meta.put(NODE_ID_META, node.id());

                        if (recoveryDesc != null) {
                            recoveryDesc.onHandshake(rcvCnt);

                            meta.put(-1, recoveryDesc);
                        }

                        GridNioSession ses = nioSrvr.createSession(ch, meta).get();

                        client = new GridTcpNioCommunicationClient(ses, log);

                        conn = true;
                    }
                    finally {
                        if (!conn) {
                            if (recoveryDesc != null)
                                recoveryDesc.release();
                        }
                    }
                }
                catch (HandshakeTimeoutException e) {
                    if (client != null) {
                        client.forceClose();

                        client = null;
                    }

                    onException("Handshake timedout (will retry with increased timeout) [timeout=" + connTimeout0 +
                        ", addr=" + addr + ']', e);

                    if (log.isDebugEnabled())
                        log.debug(
                            "Handshake timedout (will retry with increased timeout) [timeout=" + connTimeout0 +
                                ", addr=" + addr + ", err=" + e + ']');

                    if (attempt == reconCnt || connTimeout0 > maxConnTimeout) {
                        if (log.isDebugEnabled())
                            log.debug("Handshake timedout (will stop attempts to perform the handshake) " +
                                "[timeout=" + connTimeout0 + ", maxConnTimeout=" + maxConnTimeout +
                                ", attempt=" + attempt + ", reconCnt=" + reconCnt +
                                ", err=" + e.getMessage() + ", addr=" + addr + ']');

                        if (errs == null)
                            errs = new IgniteCheckedException("Failed to connect to node (is node still alive?). " +
                                "Make sure that each GridComputeTask and GridCacheTransaction has a timeout set " +
                                "in order to prevent parties from waiting forever in case of network issues " +
                                "[nodeId=" + node.id() + ", addrs=" + addrs + ']');

                        errs.addSuppressed(new IgniteCheckedException("Failed to connect to address: " + addr, e));

                        break;
                    }
                    else {
                        attempt++;

                        connTimeout0 *= 2;

                        // Continue loop.
                    }
                }
                catch (Exception e) {
                    if (client != null) {
                        client.forceClose();

                        client = null;
                    }

                    onException("Client creation failed [addr=" + addr + ", err=" + e + ']', e);

                    if (log.isDebugEnabled())
                        log.debug("Client creation failed [addr=" + addr + ", err=" + e + ']');

                    if (X.hasCause(e, SocketTimeoutException.class))
                        LT.warn(log, null, "Connect timed out (consider increasing 'connTimeout' " +
                            "configuration property) [addr=" + addr + ']');

                    if (errs == null)
                        errs = new IgniteCheckedException("Failed to connect to node (is node still alive?). " +
                            "Make sure that each GridComputeTask and GridCacheTransaction has a timeout set " +
                            "in order to prevent parties from waiting forever in case of network issues " +
                            "[nodeId=" + node.id() + ", addrs=" + addrs + ']');

                    errs.addSuppressed(new IgniteCheckedException("Failed to connect to address: " + addr, e));

                    // Reconnect for the second time, if connection is not established.
                    if (connectAttempts < 2 &&
                        (e instanceof ConnectException || X.hasCause(e, ConnectException.class))) {
                        connectAttempts++;

                        continue;
                    }

                    break;
                }
            }

            if (conn)
                break;
        }

        if (client == null) {
            assert errs != null;

            if (X.hasCause(errs, ConnectException.class))
                LT.warn(log, null, "Failed to connect to a remote node " +
                    "(make sure that destination node is alive and " +
                    "operating system firewall is disabled on local and remote hosts) " +
                    "[addrs=" + addrs + ']');

            throw errs;
        }

        if (log.isDebugEnabled())
            log.debug("Created client: " + client);

        return client;
    }

    /**
     * Performs handshake in timeout-safe way.
     *
     * @param client Client.
     * @param recovery Recovery descriptor if use recovery handshake, otherwise {@code null}.
     * @param rmtNodeId Remote node.
     * @param timeout Timeout for handshake.
     * @throws IgniteCheckedException If handshake failed or wasn't completed withing timeout.
     * @return Handshake response.
     */
    @SuppressWarnings("ThrowFromFinallyBlock")
    private <T> long safeHandshake(
        T client,
        @Nullable GridNioRecoveryDescriptor recovery,
        UUID rmtNodeId,
        long timeout
    ) throws IgniteCheckedException {
        HandshakeTimeoutObject<T> obj = new HandshakeTimeoutObject<>(client, U.currentTimeMillis() + timeout);

        sockTimeoutWorker.addTimeoutObject(obj);

        long rcvCnt = 0;

        try {
            if (client instanceof GridCommunicationClient)
                ((GridCommunicationClient)client).doHandshake(new HandshakeClosure(rmtNodeId));
            else {
                SocketChannel ch = (SocketChannel)client;

                boolean success = false;

                try {
                    ByteBuffer buf = ByteBuffer.allocate(17);

                    for (int i = 0; i < 17; ) {
                        int read = ch.read(buf);

                        if (read == -1)
                            throw new IgniteCheckedException("Failed to read remote node ID (connection closed).");

                        i += read;
                    }

                    UUID rmtNodeId0 = U.bytesToUuid(buf.array(), 1);

                    if (!rmtNodeId.equals(rmtNodeId0))
                        throw new IgniteCheckedException("Remote node ID is not as expected [expected=" + rmtNodeId +
                            ", rcvd=" + rmtNodeId0 + ']');
                    else if (log.isDebugEnabled())
                        log.debug("Received remote node ID: " + rmtNodeId0);

                    ch.write(ByteBuffer.wrap(U.IGNITE_HEADER));

                    if (recovery != null) {
                        HandshakeMessage msg = new HandshakeMessage(ignite.configuration().getNodeId(),
                            recovery.incrementConnectCount(),
                            recovery.receivedCount());

                        if (log.isDebugEnabled())
                            log.debug("Write handshake message [rmtNode=" + rmtNodeId + ", msg=" + msg + ']');

                        buf = ByteBuffer.allocate(33);

                        buf.order(ByteOrder.nativeOrder());

                        boolean written = msg.writeTo(buf, getSpiContext().messageFormatter().writer());

                        assert written;

                        buf.flip();

                        ch.write(buf);
                    }
                    else
                        ch.write(ByteBuffer.wrap(nodeIdMsg.nodeIdBytesWithType));

                    if (recovery != null) {
                        if (log.isDebugEnabled())
                            log.debug("Waiting for handshake [rmtNode=" + rmtNodeId + ']');

                        buf = ByteBuffer.allocate(9);

                        buf.order(ByteOrder.nativeOrder());

                        for (int i = 0; i < 9; ) {
                            int read = ch.read(buf);

                            if (read == -1)
                                throw new IgniteCheckedException("Failed to read remote node recovery handshake " +
                                    "(connection closed).");

                            i += read;
                        }

                        rcvCnt = buf.getLong(1);

                        if (log.isDebugEnabled())
                            log.debug("Received handshake message [rmtNode=" + rmtNodeId + ", rcvCnt=" + rcvCnt + ']');

                        if (rcvCnt == -1) {
                            if (log.isDebugEnabled())
                                log.debug("Connection rejected, will retry client creation [rmtNode=" + rmtNodeId + ']');
                        }
                        else
                            success = true;
                    }
                    else
                        success = true;
                }
                catch (IOException e) {
                    if (log.isDebugEnabled())
                        log.debug("Failed to read from channel: " + e);

                    throw new IgniteCheckedException("Failed to read from channel.", e);
                }
                finally {
                    if (!success)
                        U.closeQuiet(ch);
                }
            }
        }
        finally {
            boolean cancelled = obj.cancel();

            if (cancelled)
                sockTimeoutWorker.removeTimeoutObject(obj);

            // Ignoring whatever happened after timeout - reporting only timeout event.
            if (!cancelled)
                throw new HandshakeTimeoutException("Failed to perform handshake due to timeout (consider increasing " +
                    "'connectionTimeout' configuration property).");
        }

        return rcvCnt;
    }

    /**
     * @param sndId Sender ID.
     * @param msg Communication message.
     * @param msgC Closure to call when message processing finished.
     */
    protected void notifyListener(UUID sndId, Message msg, IgniteRunnable msgC) {
        CommunicationListener<Message> lsnr = this.lsnr;

        if (lsnr != null)
            // Notify listener of a new message.
            lsnr.onMessage(sndId, msg, msgC);
        else if (log.isDebugEnabled())
            log.debug("Received communication message without any registered listeners (will ignore, " +
                "is node stopping?) [senderNodeId=" + sndId + ", msg=" + msg + ']');
    }

    /**
     * @param node Node.
     * @return Recovery receive data for given node.
     */
    private GridNioRecoveryDescriptor recoveryDescriptor(ClusterNode node) {
        ClientKey id = new ClientKey(node.id(), node.order());

        GridNioRecoveryDescriptor recovery = recoveryDescs.get(id);

        if (recovery == null) {
            int maxSize = Math.max(msgQueueLimit, ackSndThreshold);

            int queueLimit = unackedMsgsBufSize != 0 ? unackedMsgsBufSize : (maxSize * 5);

            GridNioRecoveryDescriptor old =
                recoveryDescs.putIfAbsent(id, recovery = new GridNioRecoveryDescriptor(queueLimit, node, log));

            if (old != null)
                recovery = old;
        }

        return recovery;
    }

    /**
     * @param msg Error message.
     * @param e Exception.
     */
    private void onException(String msg, Exception e) {
        getExceptionRegistry().onException(msg, e);
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(TcpCommunicationSpi.class, this);
    }

    /**
     *
     */
    private static class ClientKey {
        /** */
        private UUID nodeId;

        /** */
        private long order;

        /**
         * @param nodeId Node ID.
         * @param order Node order.
         */
        private ClientKey(UUID nodeId, long order) {
            this.nodeId = nodeId;
            this.order = order;
        }

        /** {@inheritDoc} */
        @Override public boolean equals(Object obj) {
            if (this == obj)
                return true;

            if (obj == null || getClass() != obj.getClass())
                return false;

            ClientKey other = (ClientKey)obj;

            return order == other.order && nodeId.equals(other.nodeId);

        }

        /** {@inheritDoc} */
        @Override public int hashCode() {
            int res = nodeId.hashCode();

            res = 31 * res + (int)(order ^ (order >>> 32));

            return res;
        }

        /** {@inheritDoc} */
        @Override public String toString() {
            return S.toString(ClientKey.class, this);
        }
    }

    /** Internal exception class for proper timeout handling. */
    private static class HandshakeTimeoutException extends IgniteCheckedException {
        /** */
        private static final long serialVersionUID = 0L;

        /**
         * @param msg Message.
         */
        HandshakeTimeoutException(String msg) {
            super(msg);
        }
    }

    /**
     *
     */
    private class IdleClientWorker extends IgniteSpiThread {
        /**
         *
         */
        IdleClientWorker() {
            super(gridName, "nio-idle-client-collector", log);
        }

        /** {@inheritDoc} */
        @SuppressWarnings({"BusyWait"})
        @Override protected void body() throws InterruptedException {
            while (!isInterrupted()) {
                cleanupRecovery();

                for (Map.Entry<UUID, GridCommunicationClient> e : clients.entrySet()) {
                    UUID nodeId = e.getKey();

                    GridCommunicationClient client = e.getValue();

                    ClusterNode node = getSpiContext().node(nodeId);

                    if (node == null) {
                        if (log.isDebugEnabled())
                            log.debug("Forcing close of non-existent node connection: " + nodeId);

                        client.forceClose();

                        clients.remove(nodeId, client);

                        continue;
                    }

                    GridNioRecoveryDescriptor recovery = null;

                    if (client instanceof GridTcpNioCommunicationClient) {
                        recovery = recoveryDescs.get(new ClientKey(node.id(), node.order()));

                        if (recovery != null && recovery.lastAcknowledged() != recovery.received()) {
                            RecoveryLastReceivedMessage msg = new RecoveryLastReceivedMessage(recovery.received());

                            if (log.isDebugEnabled())
                                log.debug("Send recovery acknowledgement on timeout [rmtNode=" + nodeId +
                                    ", rcvCnt=" + msg.received() + ']');

                            nioSrvr.sendSystem(((GridTcpNioCommunicationClient)client).session(), msg);

                            recovery.lastAcknowledged(msg.received());

                            continue;
                        }
                    }

                    long idleTime = client.getIdleTime();

                    if (idleTime >= idleConnTimeout) {
                        if (recovery != null &&
                            recovery.nodeAlive(getSpiContext().node(nodeId)) &&
                            !recovery.messagesFutures().isEmpty()) {
                            if (log.isDebugEnabled())
                                log.debug("Node connection is idle, but there are unacknowledged messages, " +
                                    "will wait: " + nodeId);

                            continue;
                        }

                        if (log.isDebugEnabled())
                            log.debug("Closing idle node connection: " + nodeId);

                        if (client.close() || client.closed())
                            clients.remove(nodeId, client);
                    }
                }

                Thread.sleep(idleConnTimeout);
            }
        }

        /**
         *
         */
        private void cleanupRecovery() {
            Set<ClientKey> left = null;

            for (Map.Entry<ClientKey, GridNioRecoveryDescriptor> e : recoveryDescs.entrySet()) {
                if (left != null && left.contains(e.getKey()))
                    continue;

                GridNioRecoveryDescriptor recoverySnd = e.getValue();

                if (!recoverySnd.nodeAlive(getSpiContext().node(recoverySnd.node().id()))) {
                    if (left == null)
                        left = new HashSet<>();

                    left.add(e.getKey());
                }
            }

            if (left != null) {
                assert !left.isEmpty();

                for (ClientKey id : left) {
                    GridNioRecoveryDescriptor recoverySnd = recoveryDescs.remove(id);

                    if (recoverySnd != null)
                        recoverySnd.onNodeLeft();
                }
            }
        }
    }

    /**
     *
     */
    private class ClientFlushWorker extends IgniteSpiThread {
        /**
         *
         */
        ClientFlushWorker() {
            super(gridName, "nio-client-flusher", log);
        }

        /** {@inheritDoc} */
        @SuppressWarnings({"BusyWait"})
        @Override protected void body() throws InterruptedException {
            while (!isInterrupted()) {
                long connBufFlushFreq0 = connBufFlushFreq;

                for (Map.Entry<UUID, GridCommunicationClient> entry : clients.entrySet()) {
                    GridCommunicationClient client = entry.getValue();

                    if (client.reserve()) {
                        boolean err = true;

                        try {
                            client.flushIfNeeded(connBufFlushFreq0);

                            err = false;
                        }
                        catch (IOException e) {
                            if (getSpiContext().pingNode(entry.getKey()))
                                U.error(log, "Failed to flush client: " + client, e);
                            else {
                                if (log.isDebugEnabled())
                                    log.debug("Failed to flush client (node left): " + client);

                                onException("Failed to flush client (node left): " + client, e);
                            }
                        }
                        finally {
                            if (err)
                                client.forceClose();
                            else
                                client.release();
                        }
                    }
                }

                Thread.sleep(connBufFlushFreq0);
            }
        }
    }

    /**
     * Handles sockets timeouts.
     */
    private class SocketTimeoutWorker extends IgniteSpiThread {
        /** Time-based sorted set for timeout objects. */
        private final GridConcurrentSkipListSet<HandshakeTimeoutObject> timeoutObjs =
            new GridConcurrentSkipListSet<>(new Comparator<HandshakeTimeoutObject>() {
                @Override public int compare(HandshakeTimeoutObject o1, HandshakeTimeoutObject o2) {
                    long time1 = o1.endTime();
                    long time2 = o2.endTime();

                    long id1 = o1.id();
                    long id2 = o2.id();

                    return time1 < time2 ? -1 : time1 > time2 ? 1 :
                        id1 < id2 ? -1 : id1 > id2 ? 1 : 0;
                }
            });

        /** Mutex. */
        private final Object mux0 = new Object();

        /**
         *
         */
        SocketTimeoutWorker() {
            super(gridName, "tcp-comm-sock-timeout-worker", log);
        }

        /**
         * @param timeoutObj Timeout object to add.
         */
        @SuppressWarnings({"NakedNotify"})
        public void addTimeoutObject(HandshakeTimeoutObject timeoutObj) {
            assert timeoutObj != null && timeoutObj.endTime() > 0 && timeoutObj.endTime() != Long.MAX_VALUE;

            timeoutObjs.add(timeoutObj);

            if (timeoutObjs.firstx() == timeoutObj) {
                synchronized (mux0) {
                    mux0.notifyAll();
                }
            }
        }

        /**
         * @param timeoutObj Timeout object to remove.
         */
        public void removeTimeoutObject(HandshakeTimeoutObject timeoutObj) {
            assert timeoutObj != null;

            timeoutObjs.remove(timeoutObj);
        }

        /** {@inheritDoc} */
        @Override protected void body() throws InterruptedException {
            if (log.isDebugEnabled())
                log.debug("Socket timeout worker has been started.");

            while (!isInterrupted()) {
                long now = U.currentTimeMillis();

                for (Iterator<HandshakeTimeoutObject> iter = timeoutObjs.iterator(); iter.hasNext(); ) {
                    HandshakeTimeoutObject timeoutObj = iter.next();

                    if (timeoutObj.endTime() <= now) {
                        iter.remove();

                        timeoutObj.onTimeout();
                    }
                    else
                        break;
                }

                synchronized (mux0) {
                    while (true) {
                        // Access of the first element must be inside of
                        // synchronization block, so we don't miss out
                        // on thread notification events sent from
                        // 'addTimeoutObject(..)' method.
                        HandshakeTimeoutObject first = timeoutObjs.firstx();

                        if (first != null) {
                            long waitTime = first.endTime() - U.currentTimeMillis();

                            if (waitTime > 0)
                                mux0.wait(waitTime);
                            else
                                break;
                        }
                        else
                            mux0.wait(5000);
                    }
                }
            }
        }
    }

    /**
     *
     */
    private class RecoveryWorker extends IgniteSpiThread {
        /** */
        private final BlockingQueue<GridNioRecoveryDescriptor> q = new LinkedBlockingQueue<>();

        /**
         *
         */
        private RecoveryWorker() {
            super(gridName, "tcp-comm-recovery-worker", log);
        }

        /** {@inheritDoc} */
        @Override protected void body() throws InterruptedException {
            if (log.isDebugEnabled())
                log.debug("Recovery worker has been started.");

            while (!isInterrupted()) {
                GridNioRecoveryDescriptor recoveryDesc = q.take();

                assert recoveryDesc != null;

                ClusterNode node = recoveryDesc.node();

                if (clients.containsKey(node.id()) || !recoveryDesc.nodeAlive(getSpiContext().node(node.id())))
                    continue;

                try {
                    if (log.isDebugEnabled())
                        log.debug("Recovery reconnect [rmtNode=" + recoveryDesc.node().id() + ']');

                    GridCommunicationClient client = reserveClient(node);

                    client.release();
                }
                catch (IgniteCheckedException e) {
                    if (recoveryDesc.nodeAlive(getSpiContext().node(node.id()))) {
                        if (log.isDebugEnabled())
                            log.debug("Recovery reconnect failed, will retry " +
                                "[rmtNode=" + recoveryDesc.node().id() + ", err=" + e + ']');

                        addReconnectRequest(recoveryDesc);
                    }
                    else {
                        if (log.isDebugEnabled())
                            log.debug("Recovery reconnect failed, " +
                                "node left [rmtNode=" + recoveryDesc.node().id() + ", err=" + e + ']');

                        onException("Recovery reconnect failed, node left [rmtNode=" + recoveryDesc.node().id() + "]",
                            e);
                    }

                }
            }
        }

        /**
         * @param recoverySnd Recovery send data.
         */
        void addReconnectRequest(GridNioRecoveryDescriptor recoverySnd) {
            boolean add = q.add(recoverySnd);

            assert add;
        }
    }

    /**
     *
     */
    private static class ConnectFuture extends GridFutureAdapter<GridCommunicationClient> {
        /** */
        private static final long serialVersionUID = 0L;

        // No-op.
    }

    /**
     *
     */
    private static class HandshakeTimeoutObject<T> {
        /** */
        private static final AtomicLong idGen = new AtomicLong();

        /** */
        private final long id = idGen.incrementAndGet();

        /** */
        private final T obj;

        /** */
        private final long endTime;

        /** */
        private final AtomicBoolean done = new AtomicBoolean();

        /**
         * @param obj Client.
         * @param endTime End time.
         */
        private HandshakeTimeoutObject(T obj, long endTime) {
            assert obj != null;
            assert obj instanceof GridCommunicationClient || obj instanceof SelectableChannel;
            assert endTime > 0;

            this.obj = obj;
            this.endTime = endTime;
        }

        /**
         * @return {@code True} if object has not yet been timed out.
         */
        boolean cancel() {
            return done.compareAndSet(false, true);
        }

        /**
         * @return {@code True} if object has not yet been canceled.
         */
        boolean onTimeout() {
            if (done.compareAndSet(false, true)) {
                // Close socket - timeout occurred.
                if (obj instanceof GridCommunicationClient)
                    ((GridCommunicationClient)obj).forceClose();
                else
                    U.closeQuiet((AbstractInterruptibleChannel)obj);

                return true;
            }

            return false;
        }

        /**
         * @return End time.
         */
        long endTime() {
            return endTime;
        }

        /**
         * @return ID.
         */
        long id() {
            return id;
        }

        /** {@inheritDoc} */
        @Override public String toString() {
            return S.toString(HandshakeTimeoutObject.class, this);
        }
    }

    /**
     *
     */
    private class HandshakeClosure extends IgniteInClosure2X<InputStream, OutputStream> {
        /** */
        private static final long serialVersionUID = 0L;

        /** */
        private final UUID rmtNodeId;

        /**
         * @param rmtNodeId Remote node ID.
         */
        private HandshakeClosure(UUID rmtNodeId) {
            this.rmtNodeId = rmtNodeId;
        }

        /** {@inheritDoc} */
        @SuppressWarnings("ThrowFromFinallyBlock")
        @Override public void applyx(InputStream in, OutputStream out) throws IgniteCheckedException {
            try {
                // Handshake.
                byte[] b = new byte[17];

                int n = 0;

                while (n < 17) {
                    int cnt = in.read(b, n, 17 - n);

                    if (cnt < 0)
                        throw new IgniteCheckedException("Failed to get remote node ID (end of stream reached)");

                    n += cnt;
                }

                // First 4 bytes are for length.
                UUID id = U.bytesToUuid(b, 1);

                if (!rmtNodeId.equals(id))
                    throw new IgniteCheckedException("Remote node ID is not as expected [expected=" + rmtNodeId +
                        ", rcvd=" + id + ']');
                else if (log.isDebugEnabled())
                    log.debug("Received remote node ID: " + id);
            }
            catch (SocketTimeoutException e) {
                throw new IgniteCheckedException("Failed to perform handshake due to timeout (consider increasing " +
                    "'connectionTimeout' configuration property).", e);
            }
            catch (IOException e) {
                throw new IgniteCheckedException("Failed to perform handshake.", e);
            }

            try {
                out.write(U.IGNITE_HEADER);
                out.write(NODE_ID_MSG_TYPE);
                out.write(nodeIdMsg.nodeIdBytes);

                out.flush();

                if (log.isDebugEnabled())
                    log.debug("Sent local node ID [locNodeId=" + ignite.configuration().getNodeId() + ", rmtNodeId="
                        + rmtNodeId + ']');
            }
            catch (IOException e) {
                throw new IgniteCheckedException("Failed to perform handshake.", e);
            }
        }
    }

    /**
     * Handshake message.
     */
    @SuppressWarnings("PublicInnerClass")
    public static class HandshakeMessage implements Message {
        /** */
        private static final long serialVersionUID = 0L;

        /** */
        private UUID nodeId;

        /** */
        private long rcvCnt;

        /** */
        private long connectCnt;

        /**
         * Default constructor required by {@link Message}.
         */
        public HandshakeMessage() {
            // No-op.
        }

        /**
         * @param nodeId Node ID.
         * @param connectCnt Connect count.
         * @param rcvCnt Number of received messages.
         */
        public HandshakeMessage(UUID nodeId, long connectCnt, long rcvCnt) {
            assert nodeId != null;
            assert rcvCnt >= 0 : rcvCnt;

            this.nodeId = nodeId;
            this.connectCnt = connectCnt;
            this.rcvCnt = rcvCnt;
        }

        /**
         * @return Connect count.
         */
        public long connectCount() {
            return connectCnt;
        }

        /**
         * @return Number of received messages.
         */
        public long received() {
            return rcvCnt;
        }

        /**
         * @return Node ID.
         */
        public UUID nodeId() {
            return nodeId;
        }

        /** {@inheritDoc} */
        @Override public boolean writeTo(ByteBuffer buf, MessageWriter writer) {
            if (buf.remaining() < 33)
                return false;

            buf.put(HANDSHAKE_MSG_TYPE);

            byte[] bytes = U.uuidToBytes(nodeId);

            assert bytes.length == 16 : bytes.length;

            buf.put(bytes);

            buf.putLong(rcvCnt);

            buf.putLong(connectCnt);

            return true;
        }

        /** {@inheritDoc} */
        @Override public boolean readFrom(ByteBuffer buf, MessageReader reader) {
            if (buf.remaining() < 32)
                return false;

            byte[] nodeIdBytes = new byte[16];

            buf.get(nodeIdBytes);

            nodeId = U.bytesToUuid(nodeIdBytes, 0);

            rcvCnt = buf.getLong();

            connectCnt = buf.getLong();

            return true;
        }

        /** {@inheritDoc} */
        @Override public byte directType() {
            return HANDSHAKE_MSG_TYPE;
        }

        /** {@inheritDoc} */
        @Override public byte fieldsCount() {
            throw new UnsupportedOperationException();
        }

        /** {@inheritDoc} */
        @Override public String toString() {
            return S.toString(HandshakeMessage.class, this);
        }
    }

    /**
     * Recovery acknowledgment message.
     */
    @SuppressWarnings("PublicInnerClass")
    public static class RecoveryLastReceivedMessage implements Message {
        /** */
        private static final long serialVersionUID = 0L;

        /** */
        private long rcvCnt;

        /**
         * Default constructor required by {@link Message}.
         */
        public RecoveryLastReceivedMessage() {
            // No-op.
        }

        /**
         * @param rcvCnt Number of received messages.
         */
        public RecoveryLastReceivedMessage(long rcvCnt) {
            this.rcvCnt = rcvCnt;
        }

        /**
         * @return Number of received messages.
         */
        public long received() {
            return rcvCnt;
        }

        /** {@inheritDoc} */
        @Override public boolean writeTo(ByteBuffer buf, MessageWriter writer) {
            if (buf.remaining() < 9)
                return false;

            buf.put(RECOVERY_LAST_ID_MSG_TYPE);

            buf.putLong(rcvCnt);

            return true;
        }

        /** {@inheritDoc} */
        @Override public boolean readFrom(ByteBuffer buf, MessageReader reader) {
            if (buf.remaining() < 8)
                return false;

            rcvCnt = buf.getLong();

            return true;
        }

        /** {@inheritDoc} */
        @Override public byte directType() {
            return RECOVERY_LAST_ID_MSG_TYPE;
        }

        /** {@inheritDoc} */
        @Override public byte fieldsCount() {
            return 0;
        }

        /** {@inheritDoc} */
        @Override public String toString() {
            return S.toString(RecoveryLastReceivedMessage.class, this);
        }
    }

    /**
     * Node ID message.
     */
    @SuppressWarnings("PublicInnerClass")
    public static class NodeIdMessage implements Message {
        /** */
        private static final long serialVersionUID = 0L;

        /** */
        private byte[] nodeIdBytes;

        /** */
        private byte[] nodeIdBytesWithType;

        /** */
        public NodeIdMessage() {
            // No-op.
        }

        /**
         * @param nodeId Node ID.
         */
        private NodeIdMessage(UUID nodeId) {
            nodeIdBytes = U.uuidToBytes(nodeId);

            nodeIdBytesWithType = new byte[nodeIdBytes.length + 1];

            nodeIdBytesWithType[0] = NODE_ID_MSG_TYPE;

            System.arraycopy(nodeIdBytes, 0, nodeIdBytesWithType, 1, nodeIdBytes.length);
        }

        /** {@inheritDoc} */
        @Override public boolean writeTo(ByteBuffer buf, MessageWriter writer) {
            assert nodeIdBytes.length == 16;

            if (buf.remaining() < 17)
                return false;

            buf.put(NODE_ID_MSG_TYPE);
            buf.put(nodeIdBytes);

            return true;
        }

        /** {@inheritDoc} */
        @Override public boolean readFrom(ByteBuffer buf, MessageReader reader) {
            if (buf.remaining() < 16)
                return false;

            nodeIdBytes = new byte[16];

            buf.get(nodeIdBytes);

            return true;
        }

        /** {@inheritDoc} */
        @Override public byte directType() {
            return NODE_ID_MSG_TYPE;
        }

        /** {@inheritDoc} */
        @Override public byte fieldsCount() {
            return 0;
        }

        /** {@inheritDoc} */
        @Override public String toString() {
            return S.toString(NodeIdMessage.class, this);
        }
    }
}
