package we.conflate.processor;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import we.conflate.consumer.TickConsumer;
import we.conflate.model.FxSpot;

import java.time.Instant;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.IntStream;

class ConflatorTest {

    ScheduledExecutorService simulator;
    Conflator conflator;

    @BeforeEach
    void setUp() {
        simulator = Executors.newScheduledThreadPool(3);
        conflator = new Conflator();
    }

    @AfterEach
    void tearDown() {
        simulator.shutdown();
        conflator.shutdown();
    }

    @Test
    void test() throws InterruptedException {
        // given consumer
        AtomicInteger consumedCounter = new AtomicInteger(0);
        TickConsumer consumer = _ -> {
            int i = consumedCounter.addAndGet(1);
            if (i % 100_000 == 0) {
                TimeUnit.MILLISECONDS.sleep(50);
            }
        };
        conflator.startConsumer(consumer);

        // given producer
        AtomicInteger producedCounter = new AtomicInteger(0);
        IntStream.range(1, 1000).forEach(pairId -> simulator.scheduleAtFixedRate(() -> {
            producedCounter.addAndGet(1);
            conflator.submit(new FxSpot(
                    "ccypair" + pairId,
                    Math.random() * 10_000,
                    Math.random() * 10_000,
                    Instant.now()
            ));
        }, 0, 1, TimeUnit.MILLISECONDS));

        // then warm-up & reset
        TimeUnit.SECONDS.sleep(10);
        producedCounter.set(0);
        consumedCounter.set(0);

        // and then the actual test
        AtomicInteger soFarConsumedCounter = new AtomicInteger();
        AtomicInteger soFarProducedCounter = new AtomicInteger();
        IntStream.range(1, 11)
                .forEach(_ -> {
                    try {
                        TimeUnit.SECONDS.sleep(1);
                    } catch (InterruptedException e) {
                        Thread.currentThread().interrupt();
                    }
                    int totalConsumed = consumedCounter.get();
                    int totalProduced = producedCounter.get();
                    int lastBatchConsumed = totalConsumed - soFarConsumedCounter.get();
                    int lastBatchProduced = totalProduced - soFarProducedCounter.get();
                    System.out.printf("Produced: %s tick/s, Consumed: %s tick/s, Discarded: %s ticks/s%n",
                            lastBatchProduced, lastBatchConsumed, lastBatchProduced - lastBatchConsumed);
                    soFarConsumedCounter.set(totalConsumed);
                    soFarProducedCounter.set(totalProduced);
                });
    }
}