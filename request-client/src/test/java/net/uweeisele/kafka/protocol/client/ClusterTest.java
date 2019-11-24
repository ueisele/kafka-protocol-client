package net.uweeisele.kafka.protocol.client;

import net.uweeisele.kafka.protocol.client.node.*;
import org.apache.commons.lang3.tuple.ImmutablePair;
import org.apache.kafka.common.Node;
import org.apache.kafka.common.config.SaslConfigs;
import org.apache.kafka.common.requests.MetadataRequest;
import org.apache.kafka.common.requests.MetadataResponse;
import org.junit.jupiter.api.Test;

import java.util.Properties;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

class ClusterTest {

    @Test
    void fixedCluster() throws InterruptedException, ExecutionException, TimeoutException {
        try(KafkaRequestClient requestClient = KafkaRequestClient.create()) {
            ClusterMetadata clusterMetadata = new FixedClusterMetadata(new ClusterBuilder().addNodes(new Node(-1, "localhost", 9092)).get());
            NodeProvider nodeProvider = new ConstantNodeProvider(clusterMetadata, -1);

            CompletableFuture<ImmutablePair<Node, MetadataResponse>> response = requestClient.request(MetadataRequest.Builder.allTopics(), nodeProvider, new FutureResponseHandler<>(MetadataResponse.class));
            System.out.println(response.get(10, TimeUnit.SECONDS).getLeft());
            System.out.println(response.get(10, TimeUnit.SECONDS).getRight().cluster());
        }
    }

    @Test
    void bootstrappedCluster() throws InterruptedException, ExecutionException, TimeoutException {
        Properties properties = new Properties();
        properties.setProperty(BootstrappedClusterMetadataConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        properties.setProperty(BootstrappedClusterMetadataConfig.RETRY_BACKOFF_MS_CONFIG, "100");
        properties.setProperty(BootstrappedClusterMetadataConfig.METADATA_MAX_AGE_CONFIG, "10000");

        try(KafkaRequestClient requestClient = KafkaRequestClient.create(properties);
            ClusterMetadata clusterMetadata = BootstrappedClusterMetadata.create(requestClient, properties)) {
            NodeProvider nodeProvider = new RandomNodeProvider(clusterMetadata);

            CompletableFuture<ImmutablePair<Node, MetadataResponse>> response = requestClient.request(MetadataRequest.Builder.allTopics(), nodeProvider, new FutureResponseHandler<>(MetadataResponse.class));
            System.out.println(response.get(10, TimeUnit.SECONDS).getLeft());
            System.out.println(response.get(10, TimeUnit.SECONDS).getRight().cluster());
        }
    }

}