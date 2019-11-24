package net.uweeisele.kafka.protocol.client.node;

import java.util.List;
import java.util.function.Supplier;

import static java.util.Collections.emptyList;
import static java.util.stream.Collectors.toList;

public class AllNodeProviders implements Supplier<List<? extends NodeProvider>> {

    private final ClusterMetadata clusterMetadata;

    public AllNodeProviders(ClusterMetadata clusterMetadata) {
        this.clusterMetadata = clusterMetadata;
    }

    @Override
    public List<? extends NodeProvider> get() {
        if (clusterMetadata.isReady() && !clusterMetadata.nodes().isEmpty()) {
            return clusterMetadata.nodes().stream()
                    .map(node -> (NodeProvider) () -> node)
                    .collect(toList());
        }
        clusterMetadata.requestUpdate();
        return emptyList();
    }
}
