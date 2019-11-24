package net.uweeisele.kafka.protocol.client;

import org.apache.kafka.common.requests.AbstractRequest;
import org.apache.kafka.common.requests.AbstractResponse;

public interface RequestResponseDefinition<R extends AbstractResponse> {

    AbstractRequest.Builder<?> requestBuilder();

    Class<R> responseType();
}
