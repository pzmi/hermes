package pl.allegro.tech.hermes.common.di.guice;

import com.codahale.metrics.MetricRegistry;
import com.google.inject.AbstractModule;
import com.google.inject.Provides;
import org.apache.curator.framework.CuratorFramework;
import pl.allegro.tech.hermes.common.config.ConfigFactory;
import pl.allegro.tech.hermes.common.config.Configs;
import pl.allegro.tech.hermes.common.di.CuratorType;
import pl.allegro.tech.hermes.common.di.factories.MetricRegistryFactory;
import pl.allegro.tech.hermes.common.metric.HermesMetrics;
import pl.allegro.tech.hermes.common.util.HostnameResolver;
import pl.allegro.tech.hermes.infrastructure.zookeeper.counter.DistributedEphemeralCounter;
import pl.allegro.tech.hermes.infrastructure.zookeeper.counter.SharedCounter;
import pl.allegro.tech.hermes.metrics.PathsCompiler;

import javax.inject.Named;
import javax.inject.Singleton;

/**
 * Target package: common.infrastructure.metrics
 * <p>
 * Depends on:
 * * ConfigurationModule
 * * NetworkUtilsModule
 */
public class MetricsModule extends AbstractModule {

    /**
     * Module name in metrics path: stats.hermes.{module prefix} (e.x. producer).
     */
    private final String modulePrefix;

    public MetricsModule(String modulePrefix) {
        this.modulePrefix = modulePrefix;
    }

    @Override
    protected void configure() {
        bind(HermesMetrics.class).in(Singleton.class);
    }

    @Provides
    @Singleton
    PathsCompiler pathsCompiler(HostnameResolver hostnameResolver) {
        return new PathsCompiler(hostnameResolver.resolve());
    }

    @Provides
    @Singleton
    SharedCounter sharedCounter(@Named(CuratorType.HERMES) CuratorFramework curator, ConfigFactory config) {
        return new SharedCounter(curator,
                config.getIntProperty(Configs.METRICS_COUNTER_EXPIRE_AFTER_ACCESS),
                config.getIntProperty(Configs.ZOOKEEPER_BASE_SLEEP_TIME),
                config.getIntProperty(Configs.ZOOKEEPER_MAX_RETRIES)
        );
    }

    @Provides
    @Singleton
    DistributedEphemeralCounter distributedEphemeralCounter(@Named(CuratorType.HERMES) CuratorFramework curator) {
        return new DistributedEphemeralCounter(curator);
    }

    @Provides
    @Singleton
    MetricRegistry metricRegistry(ConfigFactory config, HostnameResolver hostnameResolver) {
        return new MetricRegistryFactory(config, hostnameResolver, modulePrefix).provide();
    }
}
