package net.uweeisele.kafka.protocol.client;

public interface NamedRequestBuilder extends RequestBuilder {

    String requestName();

    default Integer timeout() {
        return null;
    }
}
