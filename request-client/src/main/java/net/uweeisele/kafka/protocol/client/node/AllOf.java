package net.uweeisele.kafka.protocol.client.node;

import org.apache.kafka.common.Node;

import java.util.Arrays;
import java.util.List;
import java.util.function.Supplier;

public class AllOf implements Supplier<List<? extends Supplier<Node>>> {

    private final List<? extends Supplier<Node>> nodeProviders;

    @SafeVarargs
    public AllOf(Supplier<Node>... nodeProviders) {
        this(Arrays.asList(nodeProviders));
    }

    public AllOf(List<? extends Supplier<Node>> nodeProviders) {
        this.nodeProviders = nodeProviders;
    }

    @Override
    public List<? extends Supplier<Node>> get() {
        return nodeProviders;
    }
}
