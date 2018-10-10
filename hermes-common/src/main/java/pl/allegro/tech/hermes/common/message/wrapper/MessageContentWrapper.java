package pl.allegro.tech.hermes.common.message.wrapper;

import com.codahale.metrics.Counter;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import org.apache.avro.Schema;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import pl.allegro.tech.hermes.api.Topic;
import pl.allegro.tech.hermes.common.config.ConfigFactory;
import pl.allegro.tech.hermes.common.config.Configs;
import pl.allegro.tech.hermes.schema.CompiledSchema;
import pl.allegro.tech.hermes.schema.SchemaRepository;
import pl.allegro.tech.hermes.schema.SchemaVersion;

import javax.inject.Inject;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;

public class MessageContentWrapper {

    private static final Logger logger = LoggerFactory.getLogger(MessageContentWrapper.class);

    private final JsonMessageContentWrapper jsonMessageContentWrapper;
    private final AvroMessageContentWrapper avroMessageContentWrapper;
    private final SchemaRepository schemaRepository;
    private final SchemaOnlineChecksRateLimiter schemaOnlineChecksRateLimiter;
    private final AggregatingLoggers aggregatingLoggers;
    private final AggregatingLogger<TopicAndSchemaVersionEntry> failedMatchSchemaVersionLog;

    private final Counter deserializationWithMissedSchemaVersionInPayload;
    private final Counter deserializationErrorsForSchemaVersionAwarePayload;
    private final Counter deserializationErrorsForAnySchemaVersion;
    private final Counter deserializationErrorsForAnyOnlineSchemaVersion;

    @Inject
    public MessageContentWrapper(JsonMessageContentWrapper jsonMessageContentWrapper,
                                 AvroMessageContentWrapper avroMessageContentWrapper,
                                 SchemaRepository schemaRepository,
                                 SchemaOnlineChecksRateLimiter schemaOnlineChecksRateLimiter,
                                 DeserializationMetrics deserializationMetrics,
                                 ConfigFactory configFactory) {
        this.jsonMessageContentWrapper = jsonMessageContentWrapper;
        this.avroMessageContentWrapper = avroMessageContentWrapper;
        this.schemaRepository = schemaRepository;
        this.schemaOnlineChecksRateLimiter = schemaOnlineChecksRateLimiter;

        this.aggregatingLoggers = new AggregatingLoggers(logger);
        this.failedMatchSchemaVersionLog = aggregatingLoggers.addLogger(entry ->
                String.format("Failed to match schema for message for topic %s, schema version %d, fallback to previous.",
                        entry.topicName, entry.schemaVersion));
        startAggregatingLoggersReporting(configFactory);

        deserializationErrorsForSchemaVersionAwarePayload = deserializationMetrics.errorsForSchemaVersionAwarePayload();
        deserializationErrorsForAnySchemaVersion = deserializationMetrics.errorsForAnySchemaVersion();
        deserializationErrorsForAnyOnlineSchemaVersion = deserializationMetrics.errorsForAnyOnlineSchemaVersion();
        deserializationWithMissedSchemaVersionInPayload = deserializationMetrics.missedSchemaVersionInPayload();
    }

    private void startAggregatingLoggersReporting(ConfigFactory configFactory) {
        ThreadFactory threadFactory = new ThreadFactoryBuilder()
                .setNameFormat("message-content-wrapper-aggregating-logger-%d")
                .build();
        Executors.newSingleThreadScheduledExecutor(threadFactory).scheduleAtFixedRate(aggregatingLoggers::report, 0L,
                configFactory.getLongProperty(Configs.MESSAGE_CONTENT_WRAPPER_AGGREGATING_LOGGERS_REPORTING_INTERVAL),
                TimeUnit.MILLISECONDS);
    }

    public UnwrappedMessageContent unwrapJson(byte[] data) {
        return jsonMessageContentWrapper.unwrapContent(data);
    }

    public UnwrappedMessageContent unwrapAvro(byte[] data, Topic topic) {
        return isPayloadAwareOfSchemaVersion(data, topic) ? deserializeSchemaVersionAwarePayload(data, topic) :
                tryDeserializingUsingAnySchemaVersion(data, topic);
    }

    private boolean isPayloadAwareOfSchemaVersion(byte[] data, Topic topic) {
        if (topic.isSchemaVersionAwareSerializationEnabled()) {
            if (SchemaAwareSerDe.startsWithMagicByte(data)) {
                return true;
            }
            deserializationWithMissedSchemaVersionInPayload.inc();
        }
        return false;
    }

    private UnwrappedMessageContent deserializeSchemaVersionAwarePayload(byte[] data, Topic topic) {
        try {
            SchemaAwarePayload payload = SchemaAwareSerDe.deserialize(data);
            return avroMessageContentWrapper.unwrapContent(payload.getPayload(),
                    schemaRepository.getAvroSchema(topic, payload.getSchemaVersion()));
        } catch (Exception ex) {
            logger.warn("Could not deserialize schema version aware payload for topic {}. Trying to deserialize using any schema version",
                    topic.getQualifiedName(), ex);
            deserializationErrorsForSchemaVersionAwarePayload.inc();
            return tryDeserializingUsingAnySchemaVersion(data, topic);
        }
    }

    // try-harding to find proper schema
    private UnwrappedMessageContent tryDeserializingUsingAnySchemaVersion(byte[] data, Topic topic) {
        try {
            return tryDeserializingUsingAnySchemaVersion(data, topic, false);
        } catch (Exception ex) {
            logger.info("Trying to find schema online for message for topic {}", topic.getQualifiedName());
            return tryDeserializingUsingAnySchemaVersion(data, topic, true);
        }
    }

    private UnwrappedMessageContent tryDeserializingUsingAnySchemaVersion(byte[] data, Topic topic, boolean online) {
        if (online) {
            limitSchemaRepositoryOnlineCallsRate(topic);
        }
        List<SchemaVersion> versions = schemaRepository.getVersions(topic, online);
        for (SchemaVersion version : versions) {
            try {
                CompiledSchema<Schema> schema = online ? schemaRepository.getKnownAvroSchemaVersion(topic, version) :
                        schemaRepository.getAvroSchema(topic, version);
                return avroMessageContentWrapper.unwrapContent(data, schema);
            } catch (Exception ex) {
                failedMatchSchemaVersionLog.mark(new TopicAndSchemaVersionEntry(topic.getQualifiedName(), version.value()), ex);
            }
        }
        logger.error("Could not match schema {} for message of topic {} {}",
                online ? "online" : "from cache", topic.getQualifiedName(), SchemaVersion.toString(versions));
        deserializationErrorsCounterForAnySchemaVersion(online).inc();
        throw new SchemaMissingException(topic);
    }

    private void limitSchemaRepositoryOnlineCallsRate(Topic topic) {
        if (!schemaOnlineChecksRateLimiter.tryAcquireOnlineCheckPermit()) {
            logger.error("Could not match schema online for message of topic {} " +
                    "due to too many schema repository requests", topic.getQualifiedName());
            throw new SchemaMissingException(topic);
        }
    }

    private Counter deserializationErrorsCounterForAnySchemaVersion(boolean online) {
        return online ? deserializationErrorsForAnyOnlineSchemaVersion : deserializationErrorsForAnySchemaVersion;
    }

    public byte[] wrapAvro(byte[] data, String id, long timestamp, Topic topic, CompiledSchema<Schema> schema, Map<String, String> externalMetadata) {
        byte[] wrapped = avroMessageContentWrapper.wrapContent(data, id, timestamp, schema.getSchema(), externalMetadata);
        return topic.isSchemaVersionAwareSerializationEnabled() ? SchemaAwareSerDe.serialize(schema.getVersion(), wrapped) : wrapped;
    }

    public byte[] wrapJson(byte[] data, String id, long timestamp, Map<String, String> externalMetadata) {
        return jsonMessageContentWrapper.wrapContent(data, id, timestamp, externalMetadata);
    }

    private static class TopicAndSchemaVersionEntry {
        private final String topicName;
        private final int schemaVersion;

        private TopicAndSchemaVersionEntry(String topicName, int schemaVersion) {
            this.topicName = topicName;
            this.schemaVersion = schemaVersion;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            TopicAndSchemaVersionEntry that = (TopicAndSchemaVersionEntry) o;
            return schemaVersion == that.schemaVersion &&
                    Objects.equals(topicName, that.topicName);
        }

        @Override
        public int hashCode() {
            return Objects.hash(topicName, schemaVersion);
        }
    }
}
