package net.uweeisele.kafka.protocol.client.node;

import net.uweeisele.kafka.protocol.client.KafkaRequestClient;
import net.uweeisele.kafka.protocol.client.ResponseHandler;
import org.apache.commons.lang3.concurrent.BasicThreadFactory;
import org.apache.kafka.clients.ClientDnsLookup;
import org.apache.kafka.common.Cluster;
import org.apache.kafka.common.Node;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.errors.AuthenticationException;
import org.apache.kafka.common.requests.AbstractResponse;
import org.apache.kafka.common.requests.MetadataRequest;
import org.apache.kafka.common.requests.MetadataResponse;
import org.apache.kafka.common.utils.LogContext;
import org.apache.kafka.common.utils.Time;
import org.slf4j.Logger;

import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.*;

public class BootstrappedClusterMetadata implements ClusterMetadata {

    private final Logger log;

    private final KafkaRequestClient client;

    /**
     * The minimum amount of time that we should wait between subsequent updates.
     */
    private final long updateBackoffMs;

    /**
     * The minimum amount of time that we should wait before triggering an
     * automatic update.
     */
    private final long updateIntervalMs;

    private final ScheduledFuture<?> updateTask;

    private final Time time;

    /**
     * The current update state.
     */
    private State state = State.QUIESCENT;

    /**
     * The time in wall-clock milliseconds when we last did an update.
     */
    private long lastUpdateMs = 0;

    /**
     * The time in wall-clock milliseconds when we last attempted to do an update.
     */
    private long lastUpdateAttemptMs = 0;

    private Cluster cluster;

    /**
     * If we got an authorization exception when we last attempted to fetch
     * metadata, this is it; null, otherwise.
     */
    private AuthenticationException authException = null;

    private enum State {
        QUIESCENT,
        UPDATE_REQUESTED,
        UPDATE_PENDING
    }

    public static BootstrappedClusterMetadata create(KafkaRequestClient client, Properties properties) {
        return createInternal(client, new BootstrappedClusterMetadataConfig(properties));
    }

    public static BootstrappedClusterMetadata create(KafkaRequestClient client, Map<String, Object> conf) {
        return createInternal(client, new BootstrappedClusterMetadataConfig(conf));
    }

    static BootstrappedClusterMetadata createInternal(KafkaRequestClient client, BootstrappedClusterMetadataConfig config) {
        return new BootstrappedClusterMetadata(
                client,
                config.getLong(BootstrappedClusterMetadataConfig.RETRY_BACKOFF_MS_CONFIG),
                config.getLong(BootstrappedClusterMetadataConfig.METADATA_MAX_AGE_CONFIG),
                ClientDnsLookup.forConfig(config.getString(BootstrappedClusterMetadataConfig.CLIENT_DNS_LOOKUP_CONFIG)),
                config.getList(BootstrappedClusterMetadataConfig.BOOTSTRAP_SERVERS_CONFIG)
        );
    }

    public BootstrappedClusterMetadata(KafkaRequestClient client, long updateBackoffMs, long updateIntervalMs, ClientDnsLookup clientDnsLookup, List<String> bootstrapServers) {
        this(client, updateBackoffMs, updateIntervalMs, new BootstrapClusterBuilder().addUrls(clientDnsLookup, bootstrapServers));
    }

    public BootstrappedClusterMetadata(KafkaRequestClient client, long updateBackoffMs, long updateIntervalMs, BootstrapClusterBuilder bootstrapClusterBuilder) {
        this(client, updateBackoffMs, updateIntervalMs, bootstrapClusterBuilder, Executors.newSingleThreadScheduledExecutor(daemonThreadFactory()));
    }

    public BootstrappedClusterMetadata(KafkaRequestClient client, long updateBackoffMs, long updateIntervalMs, BootstrapClusterBuilder bootstrapClusterBuilder, ScheduledExecutorService scheduledExecutorService) {
        this(client, updateBackoffMs, updateIntervalMs, bootstrapClusterBuilder, scheduledExecutorService, createLogContext(), Time.SYSTEM);
    }

    public BootstrappedClusterMetadata(KafkaRequestClient client, long updateBackoffMs, long updateIntervalMs, BootstrapClusterBuilder bootstrapClusterBuilder, ScheduledExecutorService scheduledExecutorService, LogContext logContext, Time time) {
        this.client = client;
        this.updateBackoffMs = updateBackoffMs;
        this.updateIntervalMs = updateIntervalMs;
        update(bootstrapClusterBuilder.get(), time.milliseconds());
        this.log = logContext.logger(BootstrappedClusterMetadata.class);
        this.time = time;
        this.log.debug("Bootstrapped cluster metadata initialized");
        this.updateTask = scheduledExecutorService.scheduleWithFixedDelay(this::runUpdateCycle, updateBackoffMs, updateBackoffMs, TimeUnit.MILLISECONDS);

    }

    static ThreadFactory daemonThreadFactory() {
        return new BasicThreadFactory.Builder().daemon(true).build();
    }

    static LogContext createLogContext() {
        return new LogContext("[BootstrappedClusterMetadata] ");
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
        if (authException != null) {
            log.debug("Metadata is not usable: failed to get metadata.", authException);
            throw authException;
        }
        if (cluster.nodes().isEmpty()) {
            log.trace("Metadata is not ready: bootstrap nodes have not been " +
                    "initialized yet.");
            return false;
        }
        if (cluster.isBootstrapConfigured()) {
            log.trace("Metadata is not ready: we have not fetched metadata from " +
                    "the bootstrap nodes yet.");
            return false;
        }
        log.trace("Metadata is ready to use.");
        return true;
    }

    @Override
    public BootstrappedClusterMetadata requestUpdate() {
        if (state == State.QUIESCENT) {
            state = State.UPDATE_REQUESTED;
            log.debug("Requesting metadata update.");
        }
        return this;
    }

    @Override
    public void close() {
        log.debug("Closing update task.");
        updateTask.cancel(false);
    }

    private void runUpdateCycle() {
        long now = time.milliseconds();
        long updateDelayMs = updateDelayMs(now);
        if (updateDelayMs == 0) {
            transitionToUpdatePending(now);
            client.send(MetadataRequest.Builder.allTopics(), new RandomNodeProvider(new FixedClusterMetadata(cluster)), new ResponseHandler() {
                @Override
                public void handleResponse(AbstractResponse abstractResponse, Node node) {
                    update(((MetadataResponse)abstractResponse).cluster(), now);
                }

                @Override
                public void handleFailure(Throwable throwable, Node node) {
                    updateFailed(throwable);
                }
            });
        }
    }

    /**
     * Determine if the AdminClient should fetch new metadata.
     */
    private long updateDelayMs(long now) {
        switch (state) {
            case QUIESCENT:
                // Calculate the time remaining until the next periodic update.
                // We want to avoid making many metadata requests in a short amount of time,
                // so there is a metadata refresh backoff period.
                return Math.max(delayBeforeNextAttemptMs(now), delayBeforeNextExpireMs(now));
            case UPDATE_REQUESTED:
                // Respect the backoff, even if an update has been requested
                return delayBeforeNextAttemptMs(now);
            default:
                // An update is already pending, so we don't need to initiate another one.
                return Long.MAX_VALUE;
        }
    }

    private long delayBeforeNextExpireMs(long now) {
        long timeSinceUpdate = now - lastUpdateMs;
        return Math.max(0, updateIntervalMs - timeSinceUpdate);
    }

    private long delayBeforeNextAttemptMs(long now) {
        long timeSinceAttempt = now - lastUpdateAttemptMs;
        return Math.max(0, updateBackoffMs - timeSinceAttempt);
    }

    /**
     * Transition into the UPDATE_PENDING state.  Updates lastUpdateAttemptMs.
     */
    private void transitionToUpdatePending(long now) {
        this.state = State.UPDATE_PENDING;
        this.lastUpdateAttemptMs = now;
    }

    private void updateFailed(Throwable exception) {
        // We depend on pending calls to request another metadata update
        this.state = State.QUIESCENT;

        if (exception instanceof AuthenticationException) {
            log.warn("Metadata update failed due to authentication error", exception);
            this.authException = (AuthenticationException) exception;
        } else {
            log.info("Metadata update failed", exception);
        }
    }

    /**
     * Receive new metadata, and transition into the QUIESCENT state.
     * Updates lastUpdateMs, cluster, and authException.
     */
    private void update(Cluster cluster, long now) {
        if (cluster.isBootstrapConfigured()) {
            log.debug("Setting bootstrap cluster metadata {}.", cluster);
        } else {
            log.debug("Updating cluster metadata to {}", cluster);
            this.lastUpdateMs = now;
        }

        this.state = State.QUIESCENT;
        this.authException = null;

        if (!cluster.nodes().isEmpty()) {
            this.cluster = cluster;
        }
    }
}
