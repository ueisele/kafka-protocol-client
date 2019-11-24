package net.uweeisele.kafka.protocol.client;

import org.apache.commons.lang3.tuple.ImmutablePair;
import org.apache.kafka.common.Node;
import org.apache.kafka.common.requests.AbstractResponse;

import java.util.concurrent.CompletableFuture;

public class FutureResponseHandler<T extends AbstractResponse> implements ResultResponseHandler<CompletableFuture<ImmutablePair<Node, T>>> {

    private final Class<T> responseType;

    private final CompletableFuture<ImmutablePair<Node, T>> responseFuture;

    public FutureResponseHandler(Class<T> responseType) {
        this(responseType, new CompletableFuture<>());
    }

    public FutureResponseHandler(Class<T> responseType, CompletableFuture<ImmutablePair<Node, T>> responseFuture) {
        this.responseType = responseType;
        this.responseFuture = responseFuture;
    }

    @Override
    public CompletableFuture<ImmutablePair<Node, T>> get() {
        return responseFuture;
    }

    @Override
    public void handleResponse(AbstractResponse abstractResponse, Node node) {
        responseFuture.complete(ImmutablePair.of(node, responseType.cast(abstractResponse)));
    }

    @Override
    public void handleFailure(Throwable throwable, Node node) {
        responseFuture.completeExceptionally(throwable);
    }
}
