package pl.allegro.tech.hermes.common.message.wrapper;

import org.slf4j.Logger;

import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Function;

class AggregatingLogger<T> {

    private final Map<T, LogAggregate> entries = new ConcurrentHashMap<>();
    private final Logger logger;
    private final Function<T, String> toMessageTransformer;

    AggregatingLogger(Logger logger, Function<T, String> toMessageTransformer) {
        this.logger = logger;
        this.toMessageTransformer = toMessageTransformer;
    }

    void mark(T entry) {
        mark(entry, null);
    }

    void mark(T entry, Throwable ex) {
        entries.merge(entry, new LogAggregate(ex),
                (currentAggregate, emptyAggregate) -> LogAggregate.incrementCount(currentAggregate,
                        Optional.ofNullable(ex)
                                .orElse(currentAggregate.lastException)));
    }

    void report() {
        entries.keySet().forEach(entry -> {
            entries.computeIfPresent(entry, (key, aggregate) -> {
                String message = String.format("%s [occurrences=%d]", toMessageTransformer.apply(key), aggregate.logsCount);
                if (aggregate.lastException != null) {
                    logger.error(message, aggregate.lastException);
                } else {
                    logger.error(message);
                }
                return null;
            });
        });
    }

    private static class LogAggregate {

        private final int logsCount;
        private final Throwable lastException;

        private LogAggregate(Throwable lastException) {
            this(1, lastException);
        }

        private LogAggregate(int logsCount, Throwable lastException) {
            this.logsCount = logsCount;
            this.lastException = lastException;
        }

        private static LogAggregate incrementCount(LogAggregate currentAggregate, Throwable lastException) {
            return new LogAggregate(currentAggregate.logsCount + 1, lastException);
        }
    }
}
