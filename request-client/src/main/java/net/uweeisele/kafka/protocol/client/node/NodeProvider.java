package net.uweeisele.kafka.protocol.client.node;

import org.apache.kafka.common.Node;

@FunctionalInterface
public interface NodeProvider {
    Node provide();
}
