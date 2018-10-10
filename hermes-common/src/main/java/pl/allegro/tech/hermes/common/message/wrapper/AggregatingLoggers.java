package pl.allegro.tech.hermes.common.message.wrapper;

import org.slf4j.Logger;

import java.util.ArrayList;
import java.util.List;
import java.util.function.Function;

class AggregatingLoggers {

    private final Logger logger;

    private List<AggregatingLogger> loggers = new ArrayList<>();

    public AggregatingLoggers(Logger logger) {
        this.logger = logger;
    }

    <T> AggregatingLogger<T> addLogger(Function<T, String> toMessageTransformer) {
        AggregatingLogger<T> aggregatingLogger = new AggregatingLogger<>(logger, toMessageTransformer);
        loggers.add(aggregatingLogger);
        return aggregatingLogger;
    }

    void report() {
        for (AggregatingLogger logger: loggers) {
            logger.report();
        }
    }
}
