package net.uweeisele.kafka.protocol.client;

import org.apache.kafka.common.Node;
import org.apache.kafka.common.requests.AbstractResponse;

public interface ResponseHandler {

    /**
     * Process the call response.
     *
     * @param abstractResponse  The AbstractResponse.
     *
     */
    void handleResponse(AbstractResponse abstractResponse, Node node);

    /**
     * Handle a failure. This will only be called if the failure exception was not
     * retryable, or if we hit a timeout.
     *
     * @param throwable     The exception.
     */
    void handleFailure(Throwable throwable, Node node);

}
