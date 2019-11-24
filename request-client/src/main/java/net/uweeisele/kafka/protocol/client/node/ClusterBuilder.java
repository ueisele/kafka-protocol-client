package net.uweeisele.kafka.protocol.client.node;

import org.apache.kafka.clients.ClientDnsLookup;
import org.apache.kafka.common.Cluster;
import org.apache.kafka.common.Node;

import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.function.Supplier;

import static java.util.Arrays.asList;
import static org.apache.kafka.clients.ClientUtils.parseAndValidateAddresses;

public class ClusterBuilder implements Supplier<Cluster> {

    private List<Node> nodes = new ArrayList<>();

    public ClusterBuilder addUrls(ClientDnsLookup clientDnsLookup, String... urls) {
        return addUrls(clientDnsLookup, asList(urls));
    }

    public ClusterBuilder addUrls(ClientDnsLookup clientDnsLookup, List<String> urls) {
        return addInetAddresses(parseAndValidateAddresses(urls, clientDnsLookup));
    }

    public ClusterBuilder addInetAddresses(InetSocketAddress... addresses) {
        return addInetAddresses(asList(addresses));
    }

    public ClusterBuilder addInetAddresses(List<InetSocketAddress> addresses) {
        List<Node> nodes = new ArrayList<>();
        int nodeId = 1;
        for (InetSocketAddress address : addresses)
            nodes.add(new Node(nodeId++, address.getHostString(), address.getPort()));
        return addNodes(nodes);
    }

    public ClusterBuilder addNodes(Node... nodes) {
        return addNodes(asList(nodes));
    }

    public ClusterBuilder addNodes(List<Node> nodes) {
        this.nodes.addAll(nodes);
        return this;
    }

    @Override
    public Cluster get() {
        return new Cluster(null, nodes, new ArrayList<>(0),
                Collections.emptySet(), Collections.emptySet(), Collections.emptySet(), null);
    }
}
