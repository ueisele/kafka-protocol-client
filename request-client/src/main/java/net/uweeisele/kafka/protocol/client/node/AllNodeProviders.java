package net.uweeisele.kafka.protocol.client.node;

import org.apache.kafka.common.Node;

import java.util.List;
import java.util.function.Supplier;

import static java.util.stream.Collectors.toList;

public class AllNodeProviders implements Supplier<List<? extends Supplier<Node>>> {

    private final ClusterMetadata clusterMetadata;

    public AllNodeProviders(ClusterMetadata clusterMetadata) {
        this.clusterMetadata = clusterMetadata;
    }

    @Override
    public List<? extends Supplier<Node>> get() {
        return clusterMetadata.nodes().stream()
                .map(node -> (Supplier<Node>) () -> node)
                .collect(toList());
    }
}
