package pl.allegro.tech.hermes.common.di.guice;

import com.google.inject.AbstractModule;
import pl.allegro.tech.hermes.common.config.ConfigFactory;

import javax.inject.Singleton;

/**
 * Target package: common.infrastructure.config
 */
public class ConfigurationModule extends AbstractModule {

    @Override
    protected void configure() {
        bind(ConfigFactory.class).in(Singleton.class);
    }
}
