package we.conflate.consumer;

import we.conflate.model.FxSpot;

public interface TickConsumer {
    void consume(FxSpot tick) throws InterruptedException;
}