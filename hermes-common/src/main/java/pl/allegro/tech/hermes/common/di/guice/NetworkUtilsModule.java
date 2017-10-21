package pl.allegro.tech.hermes.common.di.guice;

import com.google.inject.AbstractModule;
import com.google.inject.Provides;
import pl.allegro.tech.hermes.common.util.HostnameResolver;
import pl.allegro.tech.hermes.common.util.InetAddressHostnameResolver;

import javax.inject.Singleton;

/**
 * Target package: common.infrastructure.network
 */
public class NetworkUtilsModule extends AbstractModule {

    @Override
    protected void configure() {
    }

    @Provides
    @Singleton
    HostnameResolver hostnameResolver() {
        return new InetAddressHostnameResolver();
    }
}
