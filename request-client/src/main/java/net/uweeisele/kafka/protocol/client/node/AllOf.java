package net.uweeisele.kafka.protocol.client.node;

import java.util.Arrays;
import java.util.List;
import java.util.function.Supplier;

public class AllOf implements Supplier<List<? extends NodeProvider>> {

    private final List<? extends NodeProvider> nodeProviders;

    public AllOf(NodeProvider... nodeProviders) {
        this(Arrays.asList(nodeProviders));
    }

    public AllOf(List<? extends NodeProvider> nodeProviders) {
        this.nodeProviders = nodeProviders;
    }

    @Override
    public List<? extends NodeProvider> get() {
        return nodeProviders;
    }
}
