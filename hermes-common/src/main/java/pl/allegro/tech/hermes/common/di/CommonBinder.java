package pl.allegro.tech.hermes.common.di;

import com.yammer.metrics.core.HealthCheckRegistry;
import org.apache.avro.Schema;
import org.glassfish.hk2.api.TypeLiteral;
import pl.allegro.tech.hermes.common.broker.BrokerStorage;
import pl.allegro.tech.hermes.common.broker.ZookeeperBrokerStorage;
import pl.allegro.tech.hermes.common.di.factories.GraphiteWebTargetFactory;
import pl.allegro.tech.hermes.common.di.factories.GroupRepositoryFactory;
import pl.allegro.tech.hermes.common.di.factories.MessagePreviewRepositoryFactory;
import pl.allegro.tech.hermes.common.di.factories.ModelAwareZookeeperNotifyingCacheFactory;
import pl.allegro.tech.hermes.common.di.factories.OAuthProviderRepositoryFactory;
import pl.allegro.tech.hermes.common.di.factories.ObjectMapperFactory;
import pl.allegro.tech.hermes.common.di.factories.SimpleConsumerPoolFactory;
import pl.allegro.tech.hermes.common.di.factories.SubscriptionOffsetChangeIndicatorFactory;
import pl.allegro.tech.hermes.common.di.factories.SubscriptionRepositoryFactory;
import pl.allegro.tech.hermes.common.di.factories.TopicRepositoryFactory;
import pl.allegro.tech.hermes.common.di.factories.ZookeeperMetricsFactory;
import pl.allegro.tech.hermes.common.di.factories.ZookeeperPathsFactory;
import pl.allegro.tech.hermes.common.kafka.KafkaNamesMapperFactory;
import pl.allegro.tech.hermes.common.message.wrapper.AvroMessageContentWrapper;
import pl.allegro.tech.hermes.common.message.wrapper.DeserializationMetrics;
import pl.allegro.tech.hermes.common.message.wrapper.JsonMessageContentWrapper;
import pl.allegro.tech.hermes.common.message.wrapper.MessageContentWrapper;
import pl.allegro.tech.hermes.common.message.wrapper.SchemaOnlineChecksRateLimiter;
import pl.allegro.tech.hermes.common.message.wrapper.SchemaOnlineChecksWaitingRateLimiter;
import pl.allegro.tech.hermes.common.schema.AvroCompiledSchemaRepositoryFactory;
import pl.allegro.tech.hermes.common.schema.RawSchemaClientFactory;
import pl.allegro.tech.hermes.common.schema.SchemaRepositoryFactory;
import pl.allegro.tech.hermes.common.schema.SchemaVersionsRepositoryFactory;
import pl.allegro.tech.hermes.domain.notifications.InternalNotificationsBus;
import pl.allegro.tech.hermes.infrastructure.zookeeper.notifications.ZookeeperInternalNotificationBus;
import pl.allegro.tech.hermes.schema.CompiledSchemaRepository;

import javax.inject.Singleton;

public class CommonBinder extends AbstractBinder {

    @Override
    protected void configure() {
        bind(ZookeeperBrokerStorage.class).to(BrokerStorage.class).in(Singleton.class);
        bind(ZookeeperBrokerStorage.class).to(BrokerStorage.class).in(Singleton.class);
        bindSingletonFactory(RawSchemaClientFactory.class);
        bindSingletonFactory(SchemaVersionsRepositoryFactory.class);
        bindFactory(AvroCompiledSchemaRepositoryFactory.class).in(Singleton.class).to(new TypeLiteral<CompiledSchemaRepository<Schema>>() {});
        bindSingletonFactory(SchemaRepositoryFactory.class);

        bindSingleton(HealthCheckRegistry.class);
        bindSingleton(MessageContentWrapper.class);
        bindSingleton(DeserializationMetrics.class);
        bindSingleton(JsonMessageContentWrapper.class);
        bindSingleton(AvroMessageContentWrapper.class);
        bind(SchemaOnlineChecksWaitingRateLimiter.class).in(Singleton.class).to(SchemaOnlineChecksRateLimiter.class);

        bindSingletonFactory(GraphiteWebTargetFactory.class);
        bindSingletonFactory(ObjectMapperFactory.class);
        bindSingletonFactory(ZookeeperMetricsFactory.class);
        bindSingletonFactory(ZookeeperPathsFactory.class);
        bindSingletonFactory(GroupRepositoryFactory.class);
        bindSingletonFactory(TopicRepositoryFactory.class);
        bindSingletonFactory(SubscriptionRepositoryFactory.class);
        bindSingletonFactory(SimpleConsumerPoolFactory.class);
        bindSingletonFactory(SubscriptionOffsetChangeIndicatorFactory.class);
        bindSingletonFactory(KafkaNamesMapperFactory.class);
        bindSingletonFactory(MessagePreviewRepositoryFactory.class);

        bind(ZookeeperInternalNotificationBus.class).to(InternalNotificationsBus.class);
        bindSingletonFactory(ModelAwareZookeeperNotifyingCacheFactory.class);
        bindSingletonFactory(OAuthProviderRepositoryFactory.class);
    }
}
