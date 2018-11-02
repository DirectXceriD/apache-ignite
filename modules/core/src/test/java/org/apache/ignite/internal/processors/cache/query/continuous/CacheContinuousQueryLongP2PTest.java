package org.apache.ignite.internal.processors.cache.query.continuous;

import org.apache.ignite.IgniteException;
import org.apache.ignite.IgniteLogger;
import org.apache.ignite.cluster.ClusterNode;
import org.apache.ignite.internal.managers.communication.GridIoMessage;
import org.apache.ignite.internal.managers.deployment.GridDeploymentResponse;
import org.apache.ignite.lang.IgniteInClosure;
import org.apache.ignite.plugin.extensions.communication.Message;
import org.apache.ignite.resources.LoggerResource;
import org.apache.ignite.spi.IgniteSpiException;
import org.apache.ignite.spi.communication.CommunicationSpi;
import org.apache.ignite.spi.communication.tcp.TcpCommunicationSpi;

/**
 *
 */
public class CacheContinuousQueryLongP2PTest extends CacheContinuousQueryOperationP2PTest {
    /** */
    private static final int DELAY = 300;

    /** {@inheritDoc} */
    @Override protected CommunicationSpi communicationSpi() {
        return new P2PDelayingCommunicationSpi();
    }

    /**
     * TcpCommunicationSpi
     */
    private static class P2PDelayingCommunicationSpi extends TcpCommunicationSpi {
        /** */
        @LoggerResource
        private IgniteLogger log;

        /** {@inheritDoc} */
        @Override public void sendMessage(ClusterNode node, Message msg, IgniteInClosure<IgniteException> ackC)
            throws IgniteSpiException {
            if (isDeploymentResponse((GridIoMessage) msg)) {
                log.info(">>> Delaying deployment message: " + msg);

                try {
                    Thread.sleep(DELAY);
                }
                catch (InterruptedException e) {
                    e.printStackTrace();
                }
            }

            super.sendMessage(node, msg, ackC);
        }

        /**
         * Checks if it is a p2p deployment response.
         *
         * @param msg Message to check.
         * @return {@code True} if this is a p2p response.
         */
        private boolean isDeploymentResponse(GridIoMessage msg) {
            Object origMsg = msg.message();

            return origMsg instanceof GridDeploymentResponse;
        }
    }
}
