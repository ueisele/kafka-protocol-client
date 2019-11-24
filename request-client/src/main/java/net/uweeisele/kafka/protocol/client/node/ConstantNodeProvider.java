package net.uweeisele.kafka.protocol.client.node;

import org.apache.kafka.common.Node;

import java.util.function.Supplier;

public class ConstantNodeProvider implements Supplier<Node> {

    private final ClusterMetadata clusterMetadata;

    private final int nodeId;

    public ConstantNodeProvider(ClusterMetadata clusterMetadata, int nodeId) {
        this.clusterMetadata = clusterMetadata;
        this.nodeId = nodeId;
    }

    @Override
    public Node get() {
        if (clusterMetadata.isReady() && clusterMetadata.nodeById(nodeId) != null) {
            return clusterMetadata.nodeById(nodeId);
        }
        clusterMetadata.requestUpdate();
        return null;
    }
}
