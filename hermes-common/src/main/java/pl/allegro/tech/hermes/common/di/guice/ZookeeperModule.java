package pl.allegro.tech.hermes.common.di.guice;

import com.google.inject.AbstractModule;
import com.google.inject.Provides;
import com.google.inject.name.Named;
import org.apache.curator.framework.CuratorFramework;
import pl.allegro.tech.hermes.common.config.ConfigFactory;
import pl.allegro.tech.hermes.common.config.Configs;
import pl.allegro.tech.hermes.common.di.CuratorType;
import pl.allegro.tech.hermes.common.di.factories.CuratorClientFactory;

import javax.inject.Singleton;
import java.util.Optional;

/**
 * Target package: common.infrastructure.zookeeper
 * <p>
 * Depends on:
 * * ConfigurationModule
 * <p>
 * TODO:
 * * make CuratorFactory non-blocking (start Curator after DI context was created)
 */
public class ZookeeperModule extends AbstractModule {

    @Override
    protected void configure() {
    }

    @Provides
    @Singleton
    @Named(CuratorType.HERMES)
    CuratorFramework hermesCurator(ConfigFactory configFactory) {
        CuratorClientFactory clientFactory = new CuratorClientFactory(configFactory);

        String connectString = configFactory.getStringProperty(Configs.ZOOKEEPER_CONNECT_STRING);
        Optional<CuratorClientFactory.ZookeeperAuthorization> authorization = Optional.empty();

        if (configFactory.getBooleanProperty(Configs.ZOOKEEPER_AUTHORIZATION_ENABLED)) {
            authorization = Optional.of(new CuratorClientFactory.ZookeeperAuthorization(
                    configFactory.getStringProperty(Configs.ZOOKEEPER_AUTHORIZATION_SCHEME),
                    configFactory.getStringProperty(Configs.ZOOKEEPER_AUTHORIZATION_USER),
                    configFactory.getStringProperty(Configs.ZOOKEEPER_AUTHORIZATION_PASSWORD))
            );
        }

        return clientFactory.provide(connectString, authorization);
    }

    @Provides
    @Singleton
    @Named(CuratorType.KAFKA)
    CuratorFramework kafkaCurator(ConfigFactory configFactory) {
        CuratorClientFactory clientFactory = new CuratorClientFactory(configFactory);

        String connectString = configFactory.getStringProperty(Configs.KAFKA_ZOOKEEPER_CONNECT_STRING);
        return clientFactory.provide(connectString);
    }
}
