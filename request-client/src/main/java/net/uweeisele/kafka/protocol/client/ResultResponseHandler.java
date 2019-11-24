package net.uweeisele.kafka.protocol.client;

import java.util.function.Supplier;

public interface ResultResponseHandler<T> extends ResponseHandler, Supplier<T> {
}
