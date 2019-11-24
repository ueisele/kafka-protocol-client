package net.uweeisele.kafka.protocol.client.node;

import org.apache.kafka.clients.ClientDnsLookup;
import org.apache.kafka.common.Cluster;

import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.List;
import java.util.function.Supplier;

import static java.util.Arrays.asList;
import static org.apache.kafka.clients.ClientUtils.parseAndValidateAddresses;

public class BootstrapClusterBuilder implements Supplier<Cluster> {

    private List<InetSocketAddress> addresses = new ArrayList<>();

    public BootstrapClusterBuilder addUrls(ClientDnsLookup clientDnsLookup, String... urls) {
        return addUrls(clientDnsLookup, asList(urls));
    }

    public BootstrapClusterBuilder addUrls(ClientDnsLookup clientDnsLookup, List<String> urls) {
        return addInetAddresses(parseAndValidateAddresses(urls, clientDnsLookup));
    }

    public BootstrapClusterBuilder addInetAddresses(InetSocketAddress... addresses) {
        return addInetAddresses(asList(addresses));
    }

    public BootstrapClusterBuilder addInetAddresses(List<InetSocketAddress> addresses) {
        this.addresses = addresses;
        return this;
    }

    @Override
    public Cluster get() {
        return Cluster.bootstrap(addresses);
    }
}
