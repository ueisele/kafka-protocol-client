package net.uweeisele.kafka.protocol.client.node;

import org.apache.kafka.common.Node;

import java.util.List;
import java.util.Random;
import java.util.function.Supplier;

public class RandomNodeProvider implements Supplier<Node> {

    private final ClusterMetadata clusterMetadata;

    private final Random randIndex;

    public RandomNodeProvider(ClusterMetadata clusterMetadata) {
        this(clusterMetadata, new Random());
    }

    public RandomNodeProvider(ClusterMetadata clusterMetadata, Random randIndex) {
        this.clusterMetadata = clusterMetadata;
        this.randIndex = randIndex;
    }

    @Override
    public Node get() {
        if (clusterMetadata.isReady() && !clusterMetadata.nodes().isEmpty()) {
            List<Node> nodes = this.clusterMetadata.nodes();
            int index = this.randIndex.nextInt(nodes.size());
            return nodes.get(index);
        }
        clusterMetadata.requestUpdate();
        return null;
    }
}
