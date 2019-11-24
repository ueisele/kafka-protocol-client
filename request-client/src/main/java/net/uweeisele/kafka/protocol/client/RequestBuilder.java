package net.uweeisele.kafka.protocol.client;

import org.apache.kafka.common.errors.UnsupportedVersionException;
import org.apache.kafka.common.requests.AbstractRequest;

@FunctionalInterface
public interface RequestBuilder {

    /**
     * Create an AbstractRequest.Builder for this Call.
     *
     * @param timeoutMs The timeout in milliseconds.
     *
     * @return          The AbstractRequest builder.
     */
    AbstractRequest.Builder<?> requestBuilder(int timeoutMs);

    /**
     * Handle an UnsupportedVersionException.
     *
     * @param exception     The exception.
     *
     * @return              True if the exception can be handled; false otherwise.
     */
    default boolean handleUnsupportedVersionException(UnsupportedVersionException exception) {
        return false;
    }
}
