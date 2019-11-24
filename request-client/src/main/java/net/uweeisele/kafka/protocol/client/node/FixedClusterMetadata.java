package net.uweeisele.kafka.protocol.client.node;

import org.apache.kafka.common.Cluster;
import org.apache.kafka.common.Node;
import org.apache.kafka.common.TopicPartition;

import java.util.List;

import static java.lang.String.format;
import static java.util.Objects.requireNonNull;

public class FixedClusterMetadata implements ClusterMetadata {

    private final Cluster cluster;

    public FixedClusterMetadata(Cluster cluster) {
        this.cluster = requireNonNull(cluster, "cluster must not be null");
    }

    @Override
    public Node controller() {
        return cluster.controller();
    }

    @Override
    public Node nodeById(int nodeId) {
        return cluster.nodeById(nodeId);
    }

    @Override
    public Node leaderFor(TopicPartition topicPartition) {
        return cluster.leaderFor(topicPartition);
    }

    @Override
    public List<Node> nodes() {
        return cluster.nodes();
    }

    @Override
    public boolean isReady() {
        return true;
    }

    @Override
    public ClusterMetadata requestUpdate() {
        throw new UnsupportedOperationException(format("This cluster metadata is fixed and cannot be updated. The cluster contains the nodes %s", cluster.nodes()));
    }
}
