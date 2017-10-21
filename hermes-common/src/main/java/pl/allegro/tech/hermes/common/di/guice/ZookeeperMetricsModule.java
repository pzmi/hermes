package pl.allegro.tech.hermes.common.di.guice;

import com.codahale.metrics.MetricRegistry;
import com.google.inject.AbstractModule;
import pl.allegro.tech.hermes.common.config.ConfigFactory;
import pl.allegro.tech.hermes.common.config.Configs;
import pl.allegro.tech.hermes.common.metric.counter.CounterStorage;
import pl.allegro.tech.hermes.common.metric.counter.zookeeper.ZookeeperCounterReporter;
import pl.allegro.tech.hermes.common.metric.counter.zookeeper.ZookeeperCounterStorage;
import pl.allegro.tech.hermes.domain.subscription.SubscriptionRepository;
import pl.allegro.tech.hermes.infrastructure.zookeeper.counter.DistributedEphemeralCounter;
import pl.allegro.tech.hermes.infrastructure.zookeeper.counter.SharedCounter;
import pl.allegro.tech.hermes.metrics.PathsCompiler;

import javax.inject.Singleton;
import java.util.concurrent.TimeUnit;

/**
 * Target package: common.infrastructure.zookeeper
 * <p>
 * Depends on:
 * * ConfigurationModule
 * * MetricsModule
 * * ZookeeperModule
 * <p>
 * TODO:
 * * use this module, for now for simplicity (no need to touch SubscriptionRepository) it is NOOP
 */
public class ZookeeperMetricsModule extends AbstractModule {

    @Override
    protected void configure() {
    }

    //    @Provides
    @Singleton
    CounterStorage counterStorage(
            ConfigFactory configFactory, MetricRegistry metricRegistry, SharedCounter sharedCounter,
            DistributedEphemeralCounter distributedCounter, SubscriptionRepository subscriptionRepository,
            PathsCompiler pathsCompiler
    ) {
        ZookeeperCounterStorage counterStorage = new ZookeeperCounterStorage(
                sharedCounter, distributedCounter, subscriptionRepository, pathsCompiler, configFactory
        );

        if (configFactory.getBooleanProperty(Configs.METRICS_ZOOKEEPER_REPORTER)) {
            new ZookeeperCounterReporter(metricRegistry, counterStorage, configFactory).start(
                    configFactory.getIntProperty(Configs.REPORT_PERIOD),
                    TimeUnit.SECONDS
            );
        }

        return counterStorage;
    }
}
