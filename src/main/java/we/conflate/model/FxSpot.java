package we.conflate.model;

import java.time.Instant;

public record FxSpot  (
        String currencyPair,
        double bid,
        double ask,
        Instant timestamp
) {
}
