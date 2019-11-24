package net.uweeisele.kafka.protocol.client.node;

import org.apache.kafka.common.Node;
import org.apache.kafka.common.TopicPartition;

import java.util.List;

public interface ClusterMetadata {

    Node controller();

    Node nodeById(int nodeId);

    Node leaderFor(TopicPartition topicPartition);

    List<Node> nodes();

    boolean isReady();

    ClusterMetadata requestUpdate();
}
