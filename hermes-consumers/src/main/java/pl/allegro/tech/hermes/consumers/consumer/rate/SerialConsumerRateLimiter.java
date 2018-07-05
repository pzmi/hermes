package pl.allegro.tech.hermes.consumers.consumer.rate;

import com.codahale.metrics.Timer;
import com.google.common.util.concurrent.RateLimiter;
import pl.allegro.tech.hermes.api.Subscription;
import pl.allegro.tech.hermes.common.config.ConfigFactory;
import pl.allegro.tech.hermes.common.metric.HermesMetrics;
import pl.allegro.tech.hermes.consumers.consumer.idleTime.ExponentiallyGrowingIdleTimeCalculator;
import pl.allegro.tech.hermes.consumers.consumer.idleTime.IdleTimeCalculator;
import pl.allegro.tech.hermes.consumers.consumer.rate.calculator.OutputRateCalculationResult;
import pl.allegro.tech.hermes.consumers.consumer.rate.calculator.OutputRateCalculator;
import pl.allegro.tech.hermes.consumers.consumer.rate.calculator.OutputRateCalculatorFactory;

import java.time.Clock;
import java.util.Objects;

import static pl.allegro.tech.hermes.common.config.Configs.CONSUMER_RECEIVER_INITIAL_IDLE_TIME;
import static pl.allegro.tech.hermes.common.config.Configs.CONSUMER_RECEIVER_MAX_IDLE_TIME;
import static pl.allegro.tech.hermes.common.config.Configs.CONSUMER_RECEIVER_WAIT_BETWEEN_UNSUCCESSFUL_POLLS;
import static pl.allegro.tech.hermes.common.metric.Timers.CONSUMER_IDLE_TIME;

public class SerialConsumerRateLimiter implements ConsumerRateLimiter {

    private Subscription subscription;

    private final HermesMetrics hermesMetrics;

    private final ConsumerRateLimitSupervisor rateLimitSupervisor;

    private final RateLimiter rateLimiter;

    private final RateLimiter filterRateLimiter;

    private final OutputRateCalculator outputRateCalculator;

    private final SendCounters sendCounters;

    private final IdleTimeCalculator idleTimeCalculator;

    private final boolean waitBetweenUnsuccessfulPolls;

    private OutputRateCalculator.Mode currentMode;

    public SerialConsumerRateLimiter(Subscription subscription,
                                     ConfigFactory configFactory,
                                     OutputRateCalculatorFactory outputRateCalculatorFactory,
                                     HermesMetrics hermesMetrics,
                                     ConsumerRateLimitSupervisor rateLimitSupervisor,
                                     Clock clock) {
        this.subscription = subscription;
        this.hermesMetrics = hermesMetrics;
        this.rateLimitSupervisor = rateLimitSupervisor;
        this.sendCounters = new SendCounters(clock);
        this.outputRateCalculator = outputRateCalculatorFactory.createCalculator(subscription, sendCounters);
        this.currentMode = OutputRateCalculator.Mode.NORMAL;
        this.rateLimiter = RateLimiter.create(calculateInitialRate().rate());
        this.filterRateLimiter = RateLimiter.create(subscription.getSerialSubscriptionPolicy().getRate());
        this.idleTimeCalculator = new ExponentiallyGrowingIdleTimeCalculator(
                configFactory.getIntProperty(CONSUMER_RECEIVER_INITIAL_IDLE_TIME),
                configFactory.getIntProperty(CONSUMER_RECEIVER_MAX_IDLE_TIME)
        );
        this.waitBetweenUnsuccessfulPolls = configFactory.getBooleanProperty(CONSUMER_RECEIVER_WAIT_BETWEEN_UNSUCCESSFUL_POLLS);
    }

    @Override
    public void initialize() {
        outputRateCalculator.start();
        adjustConsumerRate();
        hermesMetrics.registerOutputRateGauge(
                subscription.getTopicName(), subscription.getName(), rateLimiter::getRate);
        rateLimitSupervisor.register(this);
    }

    @Override
    public void shutdown() {
        hermesMetrics.unregisterOutputRateGauge(subscription.getTopicName(), subscription.getName());
        rateLimitSupervisor.unregister(this);
        outputRateCalculator.shutdown();
    }

    @Override
    public void acquire() {
        rateLimiter.acquire();
        sendCounters.incrementAttempted();
    }

    @Override
    public void acquireFiltered() {
        filterRateLimiter.acquire();
    }

    @Override
    public void adjustConsumerRate() {
        OutputRateCalculationResult result = recalculate();
        rateLimiter.setRate(result.rate());
        currentMode = result.mode();
        sendCounters.reset();
    }

    private OutputRateCalculationResult calculateInitialRate() {
        return outputRateCalculator.recalculateRate(sendCounters, currentMode, 0.0);
    }

    private OutputRateCalculationResult recalculate() {
        return outputRateCalculator.recalculateRate(sendCounters, currentMode, rateLimiter.getRate());
    }

    @Override
    public void updateSubscription(Subscription newSubscription) {
        this.subscription = newSubscription;
        this.filterRateLimiter.setRate(newSubscription.getSerialSubscriptionPolicy().getRate());
        this.outputRateCalculator.updateSubscription(newSubscription);
    }

    @Override
    public void registerSuccessfulSending() {
        sendCounters.incrementSuccesses();
    }

    @Override
    public void registerFailedSending() {
        sendCounters.incrementFailures();
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }

        if (o == null || getClass() != o.getClass()) {
            return false;
        }

        SerialConsumerRateLimiter that = (SerialConsumerRateLimiter) o;

        return Objects.equals(subscription.getQualifiedName(), that.subscription.getQualifiedName());
    }

    @Override
    public int hashCode() {
        return Objects.hashCode(subscription.getQualifiedName());
    }

    @Override
    public void awaitUntilNextPoll() {
        if (waitBetweenUnsuccessfulPolls) {
            try (Timer.Context ctx = hermesMetrics.timer(CONSUMER_IDLE_TIME,
                                                         subscription.getTopicName(),
                                                         subscription.getName()).time()) {
                Thread.sleep(idleTimeCalculator.increaseIdleTime());
            } catch (InterruptedException ex) {
                Thread.currentThread().interrupt();
            }
        }
    }

    @Override
    public void registerSuccessfulPoll() {
        idleTimeCalculator.reset();
    }
}
