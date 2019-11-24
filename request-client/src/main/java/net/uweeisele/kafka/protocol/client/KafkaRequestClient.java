package net.uweeisele.kafka.protocol.client;


import net.uweeisele.kafka.protocol.client.node.NodeProvider;
import org.apache.kafka.clients.*;
import org.apache.kafka.common.KafkaException;
import org.apache.kafka.common.Metric;
import org.apache.kafka.common.MetricName;
import org.apache.kafka.common.Node;
import org.apache.kafka.common.errors.*;
import org.apache.kafka.common.metrics.*;
import org.apache.kafka.common.network.ChannelBuilder;
import org.apache.kafka.common.network.Selector;
import org.apache.kafka.common.requests.AbstractRequest;
import org.apache.kafka.common.requests.AbstractResponse;
import org.apache.kafka.common.requests.MetadataResponse;
import org.apache.kafka.common.requests.RequestHeader;
import org.apache.kafka.common.utils.AppInfoParser;
import org.apache.kafka.common.utils.KafkaThread;
import org.apache.kafka.common.utils.LogContext;
import org.apache.kafka.common.utils.Time;
import org.slf4j.Logger;

import java.time.Duration;
import java.util.*;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Predicate;

import static java.util.Collections.emptyList;
import static java.util.Collections.unmodifiableMap;
import static org.apache.kafka.common.utils.Utils.closeQuietly;

public class KafkaRequestClient implements AutoCloseable {

    /**
     * The next integer to use to name a KafkaRequestClient which the user hasn't specified an explicit name for.
     */
    private static final AtomicInteger KAFKA_REQUEST_CLIENT_ID_SEQUENCE = new AtomicInteger(1);

    /**
     * The prefix to use for the JMX metrics for this class
     */
    private static final String JMX_PREFIX = "kafka.request.client";

    /**
     * An invalid shutdown time which indicates that a shutdown has not yet been performed.
     */
    private static final long INVALID_SHUTDOWN_TIME = -1;

    /**
     * Thread name prefix for admin client network thread
     */
    static final String NETWORK_THREAD_PREFIX = "kafka-request-client-thread";

    private final Logger log;

    /**
     * The default timeout to use for an operation.
     */
    private final int defaultTimeoutMs;

    /**
     * The name of this KafkaRequestClient instance.
     */
    private final String clientId;

    /**
     * Provides the time.
     */
    private final Time time;

    /**
     * The metrics for this KafkaAdminClient.
     */
    private final Metrics metrics;

    /**
     * The network client to use.
     */
    private final KafkaClient client;

    /**
     * The runnable used in the service thread for this request client.
     */
    private final RequestClientRunnable runnable;

    /**
     * The network service thread for this request client.
     */
    private final Thread thread;

    /**
     * During a close operation, this is the time at which we will time out all pending operations
     * and force the RPC thread to exit. If the admin client is not closing, this will be 0.
     */
    private final AtomicLong hardShutdownTimeMs = new AtomicLong(INVALID_SHUTDOWN_TIME);

    /**
     * A factory which creates TimeoutProcessors for the RPC thread.
     */
    private final TimeoutProcessorFactory timeoutProcessorFactory;

    private final int maxRetries;

    private final long retryBackoffMs;

    public static KafkaRequestClient create() {
        return create(new Properties());
    }

    /**
     * Create a new AdminClient with the given configuration.
     *
     * @param props The configuration.
     * @return The new KafkaAdminClient.
     */
    public static KafkaRequestClient create(Properties props) {
        return createInternal(new RequestClientConfig(props), null);
    }

    /**
     * Create a new AdminClient with the given configuration.
     *
     * @param conf The configuration.
     * @return The new KafkaAdminClient.
     */
    public static KafkaRequestClient create(Map<String, Object> conf) {
        return createInternal(new RequestClientConfig(conf), null);
    }

    static KafkaRequestClient createInternal(RequestClientConfig config, TimeoutProcessorFactory timeoutProcessorFactory) {
        Metrics metrics = null;
        NetworkClient networkClient = null;
        Time time = Time.SYSTEM;
        String clientId = generateClientId(config);
        ChannelBuilder channelBuilder = null;
        Selector selector = null;
        ApiVersions apiVersions = new ApiVersions();
        LogContext logContext = createLogContext(clientId);

        try {
            List<MetricsReporter> reporters = config.getConfiguredInstances(RequestClientConfig.METRIC_REPORTER_CLASSES_CONFIG,
                    MetricsReporter.class,
                    Collections.singletonMap(RequestClientConfig.CLIENT_ID_CONFIG, clientId));
            Map<String, String> metricTags = Collections.singletonMap("client-id", clientId);
            MetricConfig metricConfig = new MetricConfig().samples(config.getInt(RequestClientConfig.METRICS_NUM_SAMPLES_CONFIG))
                    .timeWindow(config.getLong(RequestClientConfig.METRICS_SAMPLE_WINDOW_MS_CONFIG), TimeUnit.MILLISECONDS)
                    .recordLevel(Sensor.RecordingLevel.forName(config.getString(RequestClientConfig.METRICS_RECORDING_LEVEL_CONFIG)))
                    .tags(metricTags);
            reporters.add(new JmxReporter(JMX_PREFIX));
            metrics = new Metrics(metricConfig, reporters, time);
            String metricGrpPrefix = "request-client";
            channelBuilder = ClientUtils.createChannelBuilder(config, time);
            selector = new Selector(config.getLong(RequestClientConfig.CONNECTIONS_MAX_IDLE_MS_CONFIG),
                    metrics, time, metricGrpPrefix, channelBuilder, logContext);
            networkClient = new NetworkClient(
                    selector,
                    new NoOpMetadataUpdater(),
                    clientId,
                    1,
                    config.getLong(RequestClientConfig.RECONNECT_BACKOFF_MS_CONFIG),
                    config.getLong(RequestClientConfig.RECONNECT_BACKOFF_MAX_MS_CONFIG),
                    config.getInt(RequestClientConfig.SEND_BUFFER_CONFIG),
                    config.getInt(RequestClientConfig.RECEIVE_BUFFER_CONFIG),
                    (int) TimeUnit.HOURS.toMillis(1),
                    ClientDnsLookup.forConfig(config.getString(RequestClientConfig.CLIENT_DNS_LOOKUP_CONFIG)),
                    time,
                    true,
                    apiVersions,
                    logContext);
            return new KafkaRequestClient(config, clientId, time, metrics, networkClient,
                    timeoutProcessorFactory, logContext);
        } catch (Throwable exc) {
            closeQuietly(metrics, "Metrics");
            closeQuietly(networkClient, "NetworkClient");
            closeQuietly(selector, "Selector");
            closeQuietly(channelBuilder, "ChannelBuilder");
            throw new KafkaException("Failed to create new KafkaRequestClient", exc);
        }
    }

    /**
     * Get or create a list value from a map.
     *
     * @param map   The map to get or create the element from.
     * @param key   The key.
     * @param <K>   The key type.
     * @param <V>   The value type.
     * @return      The list value.
     */
    static <K, V> List<V> getOrCreateListValue(Map<K, List<V>> map, K key) {
        List<V> list = map.get(key);
        if (list != null)
            return list;
        list = new LinkedList<>();
        map.put(key, list);
        return list;
    }

    /**
     * Get the current time remaining before a deadline as an integer.
     *
     * @param now           The current time in milliseconds.
     * @param deadlineMs    The deadline time in milliseconds.
     * @return              The time delta in milliseconds.
     */
    static int calcTimeoutMsRemainingAsInt(long now, long deadlineMs) {
        long deltaMs = deadlineMs - now;
        if (deltaMs > Integer.MAX_VALUE)
            deltaMs = Integer.MAX_VALUE;
        else if (deltaMs < Integer.MIN_VALUE)
            deltaMs = Integer.MIN_VALUE;
        return (int) deltaMs;
    }

    /**
     * Generate the client id based on the configuration.
     *
     * @param config    The configuration
     *
     * @return          The client id
     */
    static String generateClientId(RequestClientConfig config) {
        String clientId = config.getString(RequestClientConfig.CLIENT_ID_CONFIG);
        if (!clientId.isEmpty())
            return clientId;
        return "kafkarequestclient-" + KAFKA_REQUEST_CLIENT_ID_SEQUENCE.getAndIncrement();
    }

    /**
     * Pretty-print an exception.
     *
     * @param throwable     The exception.
     *
     * @return              A compact human-readable string.
     */
    static String prettyPrintException(Throwable throwable) {
        if (throwable == null)
            return "Null exception.";
        if (throwable.getMessage() != null) {
            return throwable.getClass().getSimpleName() + ": " + throwable.getMessage();
        }
        return throwable.getClass().getSimpleName();
    }

    static LogContext createLogContext(String clientId) {
        return new LogContext("[KafkaRequestClient clientId=" + clientId + "] ");
    }

    private KafkaRequestClient(RequestClientConfig config,
                             String clientId,
                             Time time,
                             Metrics metrics,
                             KafkaClient client,
                             TimeoutProcessorFactory timeoutProcessorFactory,
                             LogContext logContext) {
        this.defaultTimeoutMs = config.getInt(RequestClientConfig.REQUEST_TIMEOUT_MS_CONFIG);
        this.clientId = clientId;
        this.log = logContext.logger(KafkaRequestClient.class);
        this.time = time;
        this.metrics = metrics;
        this.client = client;
        this.runnable = new RequestClientRunnable();
        String threadName = NETWORK_THREAD_PREFIX + " | " + clientId;
        this.thread = new KafkaThread(threadName, runnable, true);
        this.timeoutProcessorFactory = (timeoutProcessorFactory == null) ?
                new TimeoutProcessorFactory() : timeoutProcessorFactory;
        this.maxRetries = config.getInt(RequestClientConfig.RETRIES_CONFIG);
        this.retryBackoffMs = config.getLong(RequestClientConfig.RETRY_BACKOFF_MS_CONFIG);
        AppInfoParser.registerAppInfo(JMX_PREFIX, clientId, metrics, time.milliseconds());
        log.debug("Kafka request client initialized");
        thread.start();
    }

    public <T, V extends NamedRequestBuilder & ResultResponseHandler<T> & NodeProvider> T request(V requestBuilderResponseHandlerNodeProvider) {
        return request(requestBuilderResponseHandlerNodeProvider, requestBuilderResponseHandlerNodeProvider, requestBuilderResponseHandlerNodeProvider);
    }

    public <T, V extends NamedRequestBuilder & ResultResponseHandler<T>> T request(V requestBuilderResponseHandler, NodeProvider nodeProvider) {
        return request(requestBuilderResponseHandler, nodeProvider, requestBuilderResponseHandler);
    }

    public <T> T request(NamedRequestBuilder requestBuilder, NodeProvider nodeProvider, ResultResponseHandler<T> responseHandler) {
        return request(requestBuilder.requestName(), requestBuilder, nodeProvider, responseHandler, requestBuilder.timeout());
    }

    public <T> T request(AbstractRequest.Builder<?> requestBuilder, NodeProvider nodeProvider, ResultResponseHandler<T> responseHandler) {
        return request(requestBuilder, nodeProvider, responseHandler, null);
    }

    public <T> T request(AbstractRequest.Builder<?> requestBuilder, NodeProvider nodeProvider, ResultResponseHandler<T> responseHandler, Integer timeoutMs) {
        return request(requestBuilder.apiKey().name, (t) -> requestBuilder, nodeProvider, responseHandler, timeoutMs);
    }

    public <T> T request(String requestName, RequestBuilder requestBuilder, NodeProvider nodeProvider, ResultResponseHandler<T> responseHandler) {
        return request(requestName, requestBuilder, nodeProvider, responseHandler, null);
    }

    public <T> T request(String requestName, RequestBuilder requestBuilder, NodeProvider nodeProvider, ResultResponseHandler<T> responseHandler, Integer timeoutMs) {
        send(requestName, requestBuilder, nodeProvider, responseHandler, timeoutMs);
        return responseHandler.get();
    }

    public void send(NamedRequestBuilder requestBuilder, NodeProvider nodeProvider, ResponseHandler responseHandler) {
        send(requestBuilder.requestName(), requestBuilder, nodeProvider, responseHandler, requestBuilder.timeout());
    }

    public void send(AbstractRequest.Builder<?> requestBuilder, NodeProvider nodeProvider, ResponseHandler responseHandler) {
        send(requestBuilder, nodeProvider, responseHandler, null);
    }

    public void send(AbstractRequest.Builder<?> requestBuilder, NodeProvider nodeProvider, ResponseHandler responseHandler, Integer timeoutMs) {
        send(requestBuilder.apiKey().name, (t) -> requestBuilder, nodeProvider, responseHandler, timeoutMs);
    }

    public void send(String requestName, RequestBuilder requestBuilder, NodeProvider nodeProvider, ResponseHandler responseHandler) {
        send(requestName, requestBuilder, nodeProvider, responseHandler, null);
    }

    public void send(String requestName, RequestBuilder requestBuilder, NodeProvider nodeProvider, ResponseHandler responseHandler, Integer timeoutMs) {
        final long now = time.milliseconds();
        runnable.call(new Call(requestName, calcDeadlineMs(now, timeoutMs), nodeProvider) {
            @Override
            AbstractRequest.Builder<?> createRequest(int timeoutMs) {
                return requestBuilder.requestBuilder(timeoutMs);
            }

            @Override
            void handleResponse(AbstractResponse abstractResponse) {
                responseHandler.handleResponse(abstractResponse, curNode());
            }

            @Override
            void handleFailure(Throwable throwable) {
                responseHandler.handleFailure(throwable, curNode());
            }

            @Override
            boolean handleUnsupportedVersionException(UnsupportedVersionException exception) {
                return requestBuilder.handleUnsupportedVersionException(exception);
            }
        }, now);
    }

    public Map<MetricName, ? extends Metric> metrics() {
        return unmodifiableMap(this.metrics.metrics());
    }

    @Override
    public void close() {
        close(Duration.ofMillis(Long.MAX_VALUE));
    }

    public void close(Duration timeout) {
        long waitTimeMs = timeout.toMillis();
        if (waitTimeMs < 0)
            throw new IllegalArgumentException("The timeout cannot be negative.");
        waitTimeMs = Math.min(TimeUnit.DAYS.toMillis(365), waitTimeMs); // Limit the timeout to a year.
        long now = time.milliseconds();
        long newHardShutdownTimeMs = now + waitTimeMs;
        long prev = INVALID_SHUTDOWN_TIME;
        while (true) {
            if (hardShutdownTimeMs.compareAndSet(prev, newHardShutdownTimeMs)) {
                if (prev == INVALID_SHUTDOWN_TIME) {
                    log.debug("Initiating close operation.");
                } else {
                    log.debug("Moving hard shutdown time forward.");
                }
                client.wakeup(); // Wake the thread, if it is blocked inside poll().
                break;
            }
            prev = hardShutdownTimeMs.get();
            if (prev < newHardShutdownTimeMs) {
                log.debug("Hard shutdown time is already earlier than requested.");
                newHardShutdownTimeMs = prev;
                break;
            }
        }
        if (log.isDebugEnabled()) {
            long deltaMs = Math.max(0, newHardShutdownTimeMs - time.milliseconds());
            log.debug("Waiting for the I/O thread to exit. Hard shutdown in {} ms.", deltaMs);
        }
        try {
            // Wait for the thread to be joined.
            thread.join();

            AppInfoParser.unregisterAppInfo(JMX_PREFIX, clientId, metrics);

            log.debug("Kafka request client closed.");
        } catch (InterruptedException e) {
            log.debug("Interrupted while joining I/O thread", e);
            Thread.currentThread().interrupt();
        }
    }

    Time time() {
        return time;
    }

    /**
     * Get the deadline for a particular call.
     *
     * @param now               The current time in milliseconds.
     * @param optionTimeoutMs   The timeout option given by the user.
     *
     * @return                  The deadline in milliseconds.
     */
    private long calcDeadlineMs(long now, Integer optionTimeoutMs) {
        if (optionTimeoutMs != null)
            return now + Math.max(0, optionTimeoutMs);
        return now + defaultTimeoutMs;
    }

    //for testing
    int numPendingCalls() {
        return runnable.pendingCalls.size();
    }

    abstract class Call {
        private final String callName;
        private final long deadlineMs;
        private final NodeProvider nodeProvider;
        private int tries = 0;
        private boolean aborted = false;
        private Node curNode = null;
        private long nextAllowedTryMs = 0;

        Call(String callName, long deadlineMs, NodeProvider nodeProvider) {
            this.callName = callName;
            this.deadlineMs = deadlineMs;
            this.nodeProvider = nodeProvider;
        }

        protected Node curNode() {
            return curNode;
        }

        /**
         * Handle a failure.
         *
         * Depending on what the exception is and how many times we have already tried, we may choose to
         * fail the Call, or retry it. It is important to print the stack traces here in some cases,
         * since they are not necessarily preserved in ApiVersionException objects.
         *
         * @param now           The current time in milliseconds.
         * @param throwable     The failure exception.
         */
        final void fail(long now, Throwable throwable) {
            if (aborted) {
                // If the call was aborted while in flight due to a timeout, deliver a
                // TimeoutException. In this case, we do not get any more retries - the call has
                // failed. We increment tries anyway in order to display an accurate log message.
                tries++;
                if (log.isDebugEnabled()) {
                    log.debug("{} aborted at {} after {} attempt(s)", this, now, tries,
                            new Exception(prettyPrintException(throwable)));
                }
                handleFailure(new TimeoutException("Aborted due to timeout."));
                return;
            }
            // If this is an UnsupportedVersionException that we can retry, do so. Note that a
            // protocol downgrade will not count against the total number of retries we get for
            // this RPC. That is why 'tries' is not incremented.
            if ((throwable instanceof UnsupportedVersionException) &&
                    handleUnsupportedVersionException((UnsupportedVersionException) throwable)) {
                log.debug("{} attempting protocol downgrade and then retry.", this);
                runnable.enqueue(this, now);
                return;
            }
            tries++;
            nextAllowedTryMs = now + retryBackoffMs;

            // If the call has timed out, fail.
            if (calcTimeoutMsRemainingAsInt(now, deadlineMs) < 0) {
                if (log.isDebugEnabled()) {
                    log.debug("{} timed out at {} after {} attempt(s)", this, now, tries,
                            new Exception(prettyPrintException(throwable)));
                }
                handleFailure(throwable);
                return;
            }
            // If the exception is not retryable, fail.
            if (!(throwable instanceof RetriableException)) {
                if (log.isDebugEnabled()) {
                    log.debug("{} failed with non-retriable exception after {} attempt(s)", this, tries,
                            new Exception(prettyPrintException(throwable)));
                }
                handleFailure(throwable);
                return;
            }
            // If we are out of retries, fail.
            if (tries > maxRetries) {
                if (log.isDebugEnabled()) {
                    log.debug("{} failed after {} attempt(s)", this, tries,
                            new Exception(prettyPrintException(throwable)));
                }
                handleFailure(throwable);
                return;
            }
            if (log.isDebugEnabled()) {
                log.debug("{} failed: {}. Beginning retry #{}",
                        this, prettyPrintException(throwable), tries);
            }
            runnable.enqueue(this, now);
        }

        /**
         * Create an AbstractRequest.Builder for this Call.
         *
         * @param timeoutMs The timeout in milliseconds.
         *
         * @return          The AbstractRequest builder.
         */
        abstract AbstractRequest.Builder createRequest(int timeoutMs);

        /**
         * Process the call response.
         *
         * @param abstractResponse  The AbstractResponse.
         *
         */
        abstract void handleResponse(AbstractResponse abstractResponse);

        /**
         * Handle a failure. This will only be called if the failure exception was not
         * retryable, or if we hit a timeout.
         *
         * @param throwable     The exception.
         */
        abstract void handleFailure(Throwable throwable);

        /**
         * Handle an UnsupportedVersionException.
         *
         * @param exception     The exception.
         *
         * @return              True if the exception can be handled; false otherwise.
         */
        boolean handleUnsupportedVersionException(UnsupportedVersionException exception) {
            return false;
        }

        @Override
        public String toString() {
            return "Call(callName=" + callName + ", deadlineMs=" + deadlineMs + ")";
        }

    }

    static class TimeoutProcessorFactory {
        TimeoutProcessor create(long now) {
            return new TimeoutProcessor(now);
        }
    }

    static class TimeoutProcessor {
        /**
         * The current time in milliseconds.
         */
        private final long now;

        /**
         * The number of milliseconds until the next timeout.
         */
        private int nextTimeoutMs;

        /**
         * Create a new timeout processor.
         *
         * @param now           The current time in milliseconds since the epoch.
         */
        TimeoutProcessor(long now) {
            this.now = now;
            this.nextTimeoutMs = Integer.MAX_VALUE;
        }

        /**
         * Check for calls which have timed out.
         * Timed out calls will be removed and failed.
         * The remaining milliseconds until the next timeout will be updated.
         *
         * @param calls         The collection of calls.
         *
         * @return              The number of calls which were timed out.
         */
        int handleTimeouts(Collection<Call> calls, String msg) {
            int numTimedOut = 0;
            for (Iterator<Call> iter = calls.iterator(); iter.hasNext(); ) {
                Call call = iter.next();
                int remainingMs = calcTimeoutMsRemainingAsInt(now, call.deadlineMs);
                if (remainingMs < 0) {
                    call.fail(now, new TimeoutException(msg));
                    iter.remove();
                    numTimedOut++;
                } else {
                    nextTimeoutMs = Math.min(nextTimeoutMs, remainingMs);
                }
            }
            return numTimedOut;
        }

        /**
         * Check whether a call should be timed out.
         * The remaining milliseconds until the next timeout will be updated.
         *
         * @param call      The call.
         *
         * @return          True if the call should be timed out.
         */
        boolean callHasExpired(Call call) {
            int remainingMs = calcTimeoutMsRemainingAsInt(now, call.deadlineMs);
            if (remainingMs < 0)
                return true;
            nextTimeoutMs = Math.min(nextTimeoutMs, remainingMs);
            return false;
        }

        int nextTimeoutMs() {
            return nextTimeoutMs;
        }
    }

    private final class RequestClientRunnable implements Runnable {
        /**
         * Calls which have not yet been assigned to a node.
         * Only accessed from this thread.
         */
        private final ArrayList<Call> pendingCalls = new ArrayList<>();

        /**
         * Maps nodes to calls that we want to send.
         * Only accessed from this thread.
         */
        private final Map<Node, List<Call>> callsToSend = new HashMap<>();

        /**
         * Maps node ID strings to calls that have been sent.
         * Only accessed from this thread.
         */
        private final Map<String, List<Call>> callsInFlight = new HashMap<>();

        /**
         * Maps correlation IDs to calls that have been sent.
         * Only accessed from this thread.
         */
        private final Map<Integer, Call> correlationIdToCalls = new HashMap<>();

        /**
         * Pending calls. Protected by the object monitor.
         * This will be null only if the thread has shut down.
         */
        private List<Call> newCalls = new LinkedList<>();

        /**
         * Time out the elements in the pendingCalls list which are expired.
         *
         * @param processor     The timeout processor.
         */
        private void timeoutPendingCalls(TimeoutProcessor processor) {
            int numTimedOut = processor.handleTimeouts(pendingCalls, "Timed out waiting for a node assignment.");
            if (numTimedOut > 0)
                log.debug("Timed out {} pending calls.", numTimedOut);
        }

        /**
         * Time out calls which have been assigned to nodes.
         *
         * @param processor     The timeout processor.
         */
        private int timeoutCallsToSend(TimeoutProcessor processor) {
            int numTimedOut = 0;
            for (List<Call> callList : callsToSend.values()) {
                numTimedOut += processor.handleTimeouts(callList,
                        "Timed out waiting to send the call.");
            }
            if (numTimedOut > 0)
                log.debug("Timed out {} call(s) with assigned nodes.", numTimedOut);
            return numTimedOut;
        }

        /**
         * Drain all the calls from newCalls into pendingCalls.
         *
         * This function holds the lock for the minimum amount of time, to avoid blocking
         * users of AdminClient who will also take the lock to add new calls.
         */
        private synchronized void drainNewCalls() {
            if (!newCalls.isEmpty()) {
                pendingCalls.addAll(newCalls);
                newCalls.clear();
            }
        }

        /**
         * Choose nodes for the calls in the pendingCalls list.
         *
         * @param now           The current time in milliseconds.
         * @return              The minimum time until a call is ready to be retried if any of the pending
         *                      calls are backing off after a failure
         */
        private long maybeDrainPendingCalls(long now) {
            long pollTimeout = Long.MAX_VALUE;
            log.trace("Trying to choose nodes for {} at {}", pendingCalls, now);

            Iterator<Call> pendingIter = pendingCalls.iterator();
            while (pendingIter.hasNext()) {
                Call call = pendingIter.next();

                // If the call is being retried, await the proper backoff before finding the node
                if (now < call.nextAllowedTryMs) {
                    pollTimeout = Math.min(pollTimeout, call.nextAllowedTryMs - now);
                } else if (maybeDrainPendingCall(call, now)) {
                    pendingIter.remove();
                }
            }
            return pollTimeout;
        }

        /**
         * Check whether a pending call can be assigned a node. Return true if the pending call was either
         * transferred to the callsToSend collection or if the call was failed. Return false if it
         * should remain pending.
         */
        private boolean maybeDrainPendingCall(Call call, long now) {
            try {
                Node node = call.nodeProvider.provide();
                if (node != null) {
                    log.trace("Assigned {} to node {}", call, node);
                    call.curNode = node;
                    getOrCreateListValue(callsToSend, node).add(call);
                    return true;
                } else {
                    log.trace("Unable to assign {} to a node.", call);
                    return false;
                }
            } catch (Throwable t) {
                // Handle authentication errors while choosing nodes.
                log.debug("Unable to choose node for {}", call, t);
                call.fail(now, t);
                return true;
            }
        }

        /**
         * Send the calls which are ready.
         *
         * @param now                   The current time in milliseconds.
         * @return                      The minimum timeout we need for poll().
         */
        private long sendEligibleCalls(long now) {
            long pollTimeout = Long.MAX_VALUE;
            for (Iterator<Map.Entry<Node, List<Call>>> iter = callsToSend.entrySet().iterator(); iter.hasNext(); ) {
                Map.Entry<Node, List<Call>> entry = iter.next();
                List<Call> calls = entry.getValue();
                if (calls.isEmpty()) {
                    iter.remove();
                    continue;
                }
                Node node = entry.getKey();
                if (!client.ready(node, now)) {
                    long nodeTimeout = client.pollDelayMs(node, now);
                    pollTimeout = Math.min(pollTimeout, nodeTimeout);
                    log.trace("Client is not ready to send to {}. Must delay {} ms", node, nodeTimeout);
                    continue;
                }
                Call call = calls.remove(0);
                int timeoutMs = calcTimeoutMsRemainingAsInt(now, call.deadlineMs);
                AbstractRequest.Builder<?> requestBuilder;
                try {
                    requestBuilder = call.createRequest(timeoutMs);
                } catch (Throwable throwable) {
                    call.fail(now, new KafkaException(String.format(
                            "Internal error sending %s to %s.", call.callName, node)));
                    continue;
                }
                ClientRequest clientRequest = client.newClientRequest(node.idString(), requestBuilder, now, true);
                log.trace("Sending {} to {}. correlationId={}", requestBuilder, node, clientRequest.correlationId());
                client.send(clientRequest, now);
                getOrCreateListValue(callsInFlight, node.idString()).add(call);
                correlationIdToCalls.put(clientRequest.correlationId(), call);
            }
            return pollTimeout;
        }

        /**
         * Time out expired calls that are in flight.
         *
         * Calls that are in flight may have been partially or completely sent over the wire. They may
         * even be in the process of being processed by the remote server. At the moment, our only option
         * to time them out is to close the entire connection.
         *
         * @param processor         The timeout processor.
         */
        private void timeoutCallsInFlight(TimeoutProcessor processor) {
            int numTimedOut = 0;
            for (Map.Entry<String, List<Call>> entry : callsInFlight.entrySet()) {
                List<Call> contexts = entry.getValue();
                if (contexts.isEmpty())
                    continue;
                String nodeId = entry.getKey();
                // We assume that the first element in the list is the earliest. So it should be the
                // only one we need to check the timeout for.
                Call call = contexts.get(0);
                if (processor.callHasExpired(call)) {
                    if (call.aborted) {
                        log.warn("Aborted call {} is still in callsInFlight.", call);
                    } else {
                        log.debug("Closing connection to {} to time out {}", nodeId, call);
                        call.aborted = true;
                        client.disconnect(nodeId);
                        numTimedOut++;
                        // We don't remove anything from the callsInFlight data structure. Because the connection
                        // has been closed, the calls should be returned by the next client#poll(),
                        // and handled at that point.
                    }
                }
            }
            if (numTimedOut > 0)
                log.debug("Timed out {} call(s) in flight.", numTimedOut);
        }

        /**
         * Handle responses from the server.
         *
         * @param now                   The current time in milliseconds.
         * @param responses             The latest responses from KafkaClient.
         **/
        private void handleResponses(long now, List<ClientResponse> responses) {
            for (ClientResponse response : responses) {
                int correlationId = response.requestHeader().correlationId();

                Call call = correlationIdToCalls.get(correlationId);
                if (call == null) {
                    // If the server returns information about a correlation ID we didn't use yet,
                    // an internal server error has occurred. Close the connection and log an error message.
                    log.error("Internal server error on {}: server returned information about unknown " +
                                    "correlation ID {}, requestHeader = {}", response.destination(), correlationId,
                            response.requestHeader());
                    client.disconnect(response.destination());
                    continue;
                }

                // Stop tracking this call.
                correlationIdToCalls.remove(correlationId);
                List<Call> calls = callsInFlight.get(response.destination());
                if ((calls == null) || (!calls.remove(call))) {
                    log.error("Internal server error on {}: ignoring call {} in correlationIdToCall " +
                            "that did not exist in callsInFlight", response.destination(), call);
                    continue;
                }

                // Handle the result of the call. This may involve retrying the call, if we got a
                // retryible exception.
                if (response.versionMismatch() != null) {
                    call.fail(now, response.versionMismatch());
                } else if (response.wasDisconnected()) {
                    AuthenticationException authException = client.authenticationException(call.curNode());
                    if (authException != null) {
                        call.fail(now, authException);
                    } else {
                        call.fail(now, new DisconnectException(String.format(
                                "Cancelled %s request with correlation id %s due to node %s being disconnected",
                                call.callName, correlationId, response.destination())));
                    }
                } else {
                    try {
                        call.handleResponse(response.responseBody());
                        if (log.isTraceEnabled())
                            log.trace("{} got response {}", call,
                                    response.responseBody().toString(response.requestHeader().apiVersion()));
                    } catch (Throwable t) {
                        if (log.isTraceEnabled())
                            log.trace("{} handleResponse failed with {}", call, prettyPrintException(t));
                        call.fail(now, t);
                    }
                }
            }
        }

        /**
         * Unassign calls that have not yet been sent based on some predicate. For example, this
         * is used to reassign the calls that have been assigned to a disconnected node.
         *
         * @param shouldUnassign Condition for reassignment. If the predicate is true, then the calls will
         *                       be put back in the pendingCalls collection and they will be reassigned
         */
        private void unassignUnsentCalls(Predicate<Node> shouldUnassign) {
            for (Iterator<Map.Entry<Node, List<Call>>> iter = callsToSend.entrySet().iterator(); iter.hasNext(); ) {
                Map.Entry<Node, List<Call>> entry = iter.next();
                Node node = entry.getKey();
                List<Call> awaitingCalls = entry.getValue();

                if (awaitingCalls.isEmpty()) {
                    iter.remove();
                } else if (shouldUnassign.test(node)) {
                    pendingCalls.addAll(awaitingCalls);
                    iter.remove();
                }
            }
        }

        private boolean hasActiveCalls(Collection<Call> calls) {
            return !calls.isEmpty();
        }

        /**
         * Return true if there are currently active external calls.
         */
        private boolean hasActiveCalls() {
            if (hasActiveCalls(pendingCalls)) {
                return true;
            }
            for (List<Call> callList : callsToSend.values()) {
                if (hasActiveCalls(callList)) {
                    return true;
                }
            }
            return hasActiveCalls(correlationIdToCalls.values());
        }

        private boolean threadShouldExit(long now, long curHardShutdownTimeMs) {
            if (!hasActiveCalls()) {
                log.trace("All work has been completed, and the I/O thread is now exiting.");
                return true;
            }
            if (now >= curHardShutdownTimeMs) {
                log.info("Forcing a hard I/O thread shutdown. Requests in progress will be aborted.");
                return true;
            }
            log.debug("Hard shutdown in {} ms.", curHardShutdownTimeMs - now);
            return false;
        }

        @Override
        public void run() {
            long now = time.milliseconds();
            log.trace("Thread starting");
            while (true) {
                // Copy newCalls into pendingCalls.
                drainNewCalls();

                // Check if the AdminClient thread should shut down.
                long curHardShutdownTimeMs = hardShutdownTimeMs.get();
                if ((curHardShutdownTimeMs != INVALID_SHUTDOWN_TIME) && threadShouldExit(now, curHardShutdownTimeMs))
                    break;

                // Handle timeouts.
                TimeoutProcessor timeoutProcessor = timeoutProcessorFactory.create(now);
                timeoutPendingCalls(timeoutProcessor);
                timeoutCallsToSend(timeoutProcessor);
                timeoutCallsInFlight(timeoutProcessor);

                long pollTimeout = Math.min(1200000, timeoutProcessor.nextTimeoutMs());
                if (curHardShutdownTimeMs != INVALID_SHUTDOWN_TIME) {
                    pollTimeout = Math.min(pollTimeout, curHardShutdownTimeMs - now);
                }

                // Choose nodes for our pending calls.
                pollTimeout = Math.min(pollTimeout, maybeDrainPendingCalls(now));
                pollTimeout = Math.min(pollTimeout, sendEligibleCalls(now));

                // Ensure that we use a small poll timeout if there are pending calls which need to be sent
                if (!pendingCalls.isEmpty())
                    pollTimeout = Math.min(pollTimeout, retryBackoffMs);

                // Wait for network responses.
                log.trace("Entering KafkaClient#poll(timeout={})", pollTimeout);
                List<ClientResponse> responses = client.poll(pollTimeout, now);
                log.trace("KafkaClient#poll retrieved {} response(s)", responses.size());

                // unassign calls to disconnected nodes
                unassignUnsentCalls(client::connectionFailed);

                // Update the current time and handle the latest responses.
                now = time.milliseconds();
                handleResponses(now, responses);
            }
            int numTimedOut = 0;
            TimeoutProcessor timeoutProcessor = new TimeoutProcessor(Long.MAX_VALUE);
            synchronized (this) {
                numTimedOut += timeoutProcessor.handleTimeouts(newCalls, "The AdminClient thread has exited.");
                newCalls = null;
            }
            numTimedOut += timeoutProcessor.handleTimeouts(pendingCalls, "The AdminClient thread has exited.");
            numTimedOut += timeoutCallsToSend(timeoutProcessor);
            numTimedOut += timeoutProcessor.handleTimeouts(correlationIdToCalls.values(),
                    "The AdminClient thread has exited.");
            if (numTimedOut > 0) {
                log.debug("Timed out {} remaining operation(s).", numTimedOut);
            }
            closeQuietly(client, "KafkaClient");
            closeQuietly(metrics, "Metrics");
            log.debug("Exiting AdminClientRunnable thread.");
        }

        /**
         * Queue a call for sending.
         *
         * If the AdminClient thread has exited, this will fail. Otherwise, it will succeed (even
         * if the AdminClient is shutting down). This function should called when retrying an
         * existing call.
         *
         * @param call      The new call object.
         * @param now       The current time in milliseconds.
         */
        void enqueue(Call call, long now) {
            if (log.isDebugEnabled()) {
                log.debug("Queueing {} with a timeout {} ms from now.", call, call.deadlineMs - now);
            }
            boolean accepted = false;
            synchronized (this) {
                if (newCalls != null) {
                    newCalls.add(call);
                    accepted = true;
                }
            }
            if (accepted) {
                client.wakeup(); // wake the thread if it is in poll()
            } else {
                log.debug("The AdminClient thread has exited. Timing out {}.", call);
                call.fail(Long.MAX_VALUE, new TimeoutException("The AdminClient thread has exited."));
            }
        }

        /**
         * Initiate a new call.
         *
         * This will fail if the AdminClient is scheduled to shut down.
         *
         * @param call      The new call object.
         * @param now       The current time in milliseconds.
         */
        void call(Call call, long now) {
            if (hardShutdownTimeMs.get() != INVALID_SHUTDOWN_TIME) {
                log.debug("The AdminClient is not accepting new calls. Timing out {}.", call);
                call.fail(Long.MAX_VALUE, new TimeoutException("The AdminClient thread is not accepting new calls."));
            } else {
                enqueue(call, now);
            }
        }
    }

    private static class NoOpMetadataUpdater implements MetadataUpdater {

        @Override
        public List<Node> fetchNodes() {
            return emptyList();
        }

        @Override
        public boolean isUpdateDue(long now) {
            return false;
        }

        @Override
        public long maybeUpdate(long now) {
            return Long.MAX_VALUE;
        }

        @Override
        public void handleDisconnection(String destination) {
        }

        @Override
        public void handleFatalException(KafkaException fatalException) {
        }

        @Override
        public void handleCompletedMetadataResponse(RequestHeader requestHeader, long now, MetadataResponse metadataResponse) {
        }

        @Override
        public void requestUpdate() {
        }

        @Override
        public void close() {
        }
    }

}
