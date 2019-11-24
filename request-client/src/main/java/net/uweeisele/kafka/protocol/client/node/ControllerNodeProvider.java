package net.uweeisele.kafka.protocol.client.node;

import org.apache.kafka.common.Node;

import java.util.function.Supplier;

public class ControllerNodeProvider implements Supplier<Node> {

    private final ClusterMetadata clusterMetadata;

    public ControllerNodeProvider(ClusterMetadata clusterMetadata) {
        this.clusterMetadata = clusterMetadata;
    }

    @Override
    public Node get() {
        if (clusterMetadata.isReady() && clusterMetadata.controller() != null) {
            return clusterMetadata.controller();
        }
        clusterMetadata.requestUpdate();
        return null;
    }
}
