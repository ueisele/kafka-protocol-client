package net.uweeisele.kafka.protocol.client.node;

import org.apache.kafka.common.Node;

public class ConstantNodeProvider implements NodeProvider {

    private final ClusterMetadata clusterMetadata;

    private final int nodeId;

    public ConstantNodeProvider(ClusterMetadata clusterMetadata, int nodeId) {
        this.clusterMetadata = clusterMetadata;
        this.nodeId = nodeId;
    }

    @Override
    public Node provide() {
        if (clusterMetadata.isReady() && clusterMetadata.nodeById(nodeId) != null) {
            return clusterMetadata.nodeById(nodeId);
        }
        clusterMetadata.requestUpdate();
        return null;
    }
}
