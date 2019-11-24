package net.uweeisele.kafka.protocol.client.node;

import org.apache.kafka.common.Node;

public class ControllerNodeProvider implements NodeProvider {

    private final ClusterMetadata clusterMetadata;

    public ControllerNodeProvider(ClusterMetadata clusterMetadata) {
        this.clusterMetadata = clusterMetadata;
    }

    @Override
    public Node provide() {
        if (clusterMetadata.isReady() && clusterMetadata.controller() != null) {
            return clusterMetadata.controller();
        }
        clusterMetadata.requestUpdate();
        return null;
    }
}
