package net.uweeisele.kafka.protocol.client;

import net.uweeisele.kafka.protocol.client.node.*;
import org.apache.commons.lang3.tuple.ImmutablePair;
import org.apache.kafka.clients.ClientDnsLookup;
import org.apache.kafka.common.KafkaFuture;
import org.apache.kafka.common.Node;
import org.apache.kafka.common.requests.MetadataRequest;
import org.apache.kafka.common.requests.MetadataResponse;
import org.apache.kafka.common.utils.LogContext;
import org.junit.jupiter.api.Test;

import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.function.Supplier;

class MyTest {

    @Test
    void fixedCluster() throws InterruptedException, ExecutionException, TimeoutException {
        KafkaRequestClient requestClient = KafkaRequestClient.create();

        ClusterMetadata clusterMetadata = new FixedClusterMetadata(new ClusterBuilder().addNodes(new Node(-1, "riplf-dev-smoky03-kafka01", 9092)).get());
        Supplier<Node> nodeProvider = new ConstantNodeProvider(clusterMetadata, -1);

        KafkaFuture<ImmutablePair<Node, MetadataResponse>> response = requestClient.request(MetadataRequest.Builder.allTopics(), MetadataResponse.class, nodeProvider);
        System.out.println(response.get(10, TimeUnit.SECONDS).getLeft());;
        System.out.println(response.get(10, TimeUnit.SECONDS).getRight().cluster());;
    }

    @Test
    void bootstrappedCluster() throws InterruptedException, ExecutionException, TimeoutException {
        KafkaRequestClient requestClient = KafkaRequestClient.create();

        ClusterMetadata clusterMetadata = new BootstrappedClusterMetadata(new LogContext(), requestClient, 100L, 60000, new BootstrapClusterBuilder().addUrls(ClientDnsLookup.DEFAULT, "riplf-dev-smoky03-kafka01:9092"));
        Supplier<Node> nodeProvider = new RandomNodeProvider(clusterMetadata);

        KafkaFuture<ImmutablePair<Node, MetadataResponse>> response = requestClient.request(MetadataRequest.Builder.allTopics(), MetadataResponse.class, nodeProvider);
        System.out.println(response.get(10, TimeUnit.SECONDS).getLeft());;
        System.out.println(response.get(10, TimeUnit.SECONDS).getRight().cluster());;
    }
}