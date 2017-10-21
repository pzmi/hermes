package pl.allegro.tech.hermes.common.di.guice;

import com.google.inject.AbstractModule;
import com.google.inject.Provides;

import javax.inject.Singleton;
import java.time.Clock;

/**
 * Target package: common.infrastructure.time
 */
public class TimeModule extends AbstractModule {


    @Override
    protected void configure() {
    }

    @Provides
    @Singleton
    Clock clock() {
        return Clock.systemDefaultZone();
    }
}
