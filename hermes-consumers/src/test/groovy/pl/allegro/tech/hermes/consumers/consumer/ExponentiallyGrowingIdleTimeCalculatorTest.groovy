package pl.allegro.tech.hermes.consumers.consumer

import pl.allegro.tech.hermes.consumers.consumer.idleTime.ExponentiallyGrowingIdleTimeCalculator
import pl.allegro.tech.hermes.consumers.consumer.idleTime.IdleTimeCalculator
import spock.lang.Specification


class ExponentiallyGrowingIdleTimeCalculatorTest extends Specification {

    def initialIdleTime = 10
    def maxIdleTime = 1000
    def base = 2

    IdleTimeCalculator calculator

    def setup() {
        this.calculator = new ExponentiallyGrowingIdleTimeCalculator(base, initialIdleTime, maxIdleTime)
    }

    def "initial-idle-time is returned first"() {
        expect:
        calculator.idleTime == initialIdleTime
    }

    def "each consecutive idle-time is multiplied by base"() {
        expect:
        (0..6).each {
            assert calculator.increaseIdleTime() == initialIdleTime * (base ** it)
        }
    }

    def "idle-time should be lower or equal than max-idle-time"() {
        expect:
        (0..10).each {
            assert calculator.increaseIdleTime() <= maxIdleTime
        }
    }

    def "initial-idle-time is returned after reset"() {
        given:
        (0..3).each { calculator.increaseIdleTime() }

        when:
        calculator.reset()

        then:
        calculator.idleTime == initialIdleTime
    }
}
