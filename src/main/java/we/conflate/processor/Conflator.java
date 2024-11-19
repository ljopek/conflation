package we.conflate.processor;

import we.conflate.consumer.TickConsumer;
import we.conflate.model.FxSpot;

import java.time.Duration;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicBoolean;

public class Conflator {
    private final Map<String, FxSpot> latestPrices;
    private final BlockingQueue<String> updatedPairs;
    private final AtomicBoolean running;

    private final ExecutorService producerExecutor;
    private final ExecutorService consumerExecutor;

    private static final int MAX_QUEUE_SIZE = 1000;
    private static final int BATCH_SIZE = 100;
    private static final Duration POLL_TIMEOUT = Duration.ofMillis(1);

    public Conflator() {
        this.latestPrices = new ConcurrentHashMap<>();
        this.updatedPairs = new LinkedBlockingQueue<>(MAX_QUEUE_SIZE);
        this.running = new AtomicBoolean(true);
        this.producerExecutor = Executors.newFixedThreadPool(
                Runtime.getRuntime().availableProcessors()
        );
        this.consumerExecutor = Executors.newSingleThreadExecutor();
    }

    public void submit(FxSpot update) {
        if (!running.get()) {
            throw new IllegalStateException("Conflator is shutdown");
        }

        producerExecutor.execute(() -> {
            latestPrices.put(update.currencyPair(), update);
            updatedPairs.offer(update.currencyPair());
        });
    }

    public void startConsumer(TickConsumer consumer) {
        consumerExecutor.submit(() -> {
            Set<String> batch = ConcurrentHashMap.newKeySet(BATCH_SIZE);

            while (running.get()) {
                try {
                    if (collectBatch(batch)) {
                        processBatch(consumer, batch);
                        batch.clear();
                    }
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    break;
                }
            }
        });
    }

    private boolean collectBatch(Set<String> batch) throws InterruptedException {
        String first = updatedPairs.poll(POLL_TIMEOUT.toMillis(), TimeUnit.MILLISECONDS);
        if (first == null) {
            return false;
        }
        batch.add(first);

        while (batch.size() < BATCH_SIZE) {
            String next = updatedPairs.poll();
            if (next == null) {
                break;
            }
            batch.add(next);
        }

        return true;
    }

    private void processBatch(TickConsumer consumer, Set<String> batch) {
        batch.stream()
                .map(latestPrices::get)
                .filter(Objects::nonNull)
                .forEach(update -> {
                    try {
                        consumer.consume(update);
                    } catch (InterruptedException e) {
                        Thread.currentThread().interrupt();
                    } catch (Exception e) {
                        System.err.println("Error processing " + update + ": " + e.getMessage());
                    }
                });
    }

    public void shutdown() {
        running.set(false);
        producerExecutor.shutdown();
        consumerExecutor.shutdown();

        try {
            if (!producerExecutor.awaitTermination(5, TimeUnit.SECONDS)) {
                producerExecutor.shutdownNow();
            }
            if (!consumerExecutor.awaitTermination(5, TimeUnit.SECONDS)) {
                consumerExecutor.shutdownNow();
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            producerExecutor.shutdownNow();
            consumerExecutor.shutdownNow();
        }
    }
}