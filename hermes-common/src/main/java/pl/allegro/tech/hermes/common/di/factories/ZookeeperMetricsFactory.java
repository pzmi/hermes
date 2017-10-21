package pl.allegro.tech.hermes.common.di.factories;

import com.codahale.metrics.MetricRegistry;
import org.glassfish.hk2.api.Factory;
import pl.allegro.tech.hermes.common.config.ConfigFactory;
import pl.allegro.tech.hermes.common.config.Configs;
import pl.allegro.tech.hermes.common.metric.counter.CounterStorage;
import pl.allegro.tech.hermes.common.metric.counter.zookeeper.ZookeeperCounterReporter;
import pl.allegro.tech.hermes.common.metric.counter.zookeeper.ZookeeperCounterStorage;
import pl.allegro.tech.hermes.domain.subscription.SubscriptionRepository;
import pl.allegro.tech.hermes.infrastructure.zookeeper.counter.DistributedEphemeralCounter;
import pl.allegro.tech.hermes.infrastructure.zookeeper.counter.SharedCounter;
import pl.allegro.tech.hermes.metrics.PathsCompiler;

import javax.inject.Inject;
import java.util.concurrent.TimeUnit;

public class ZookeeperMetricsFactory implements Factory<CounterStorage> {

    private final SharedCounter sharedCounter;
    private final DistributedEphemeralCounter distributedCounter;
    private final SubscriptionRepository subscriptionRepository;
    private final PathsCompiler pathsCompiler;
    private final ConfigFactory configFactory;
    private final MetricRegistry metricRegistry;

    private ZookeeperCounterReporter reporter;

    @Inject
    public ZookeeperMetricsFactory(SharedCounter sharedCounter, DistributedEphemeralCounter distributedCounter,
                                   SubscriptionRepository subscriptionRepository,
                                   PathsCompiler pathsCompiler, ConfigFactory configFactory, MetricRegistry metricRegistry) {
        this.sharedCounter = sharedCounter;
        this.distributedCounter = distributedCounter;
        this.subscriptionRepository = subscriptionRepository;
        this.pathsCompiler = pathsCompiler;
        this.configFactory = configFactory;
        this.metricRegistry = metricRegistry;
    }

    @Override
    public CounterStorage provide() {
        CounterStorage counterStorage = new ZookeeperCounterStorage(
                sharedCounter, distributedCounter, subscriptionRepository, pathsCompiler, configFactory
        );


        if (configFactory.getBooleanProperty(Configs.METRICS_ZOOKEEPER_REPORTER)) {
            reporter = new ZookeeperCounterReporter(metricRegistry, counterStorage, configFactory);
            reporter.start(
                    configFactory.getIntProperty(Configs.REPORT_PERIOD),
                    TimeUnit.SECONDS
            );
        }

        return counterStorage;
    }

    @Override
    public void dispose(CounterStorage instance) {
        reporter.stop();
    }
}
