package pl.allegro.tech.hermes.consumers.consumer

import spock.lang.Specification


class ProcrastinationTest extends Specification {

    def initialIdleTime = 10
    def maxIdleTime = 1000
    Procrastinator procrastinator

    def setup() {
        this.procrastinator = new Procrastinator(initialIdleTime, maxIdleTime)
    }

    def "initial idle time is returned first"() {
        expect:
        procrastinator.idleTime == initialIdleTime
    }

    def "each consecutive procrastination is multiplied by 2"() {
        expect:
        (0..6).each {
            assert procrastinator.idleTime == initialIdleTime * (2 ** it)
        }
    }

    def "procrastination resets when actual work is reported"() {
        given:
        (0..3).each { procrastinator.idleTime }

        when:
        procrastinator.reportWork()

        then:
        procrastinator.idleTime == initialIdleTime
    }

    def "idle time should be lower than max idle time"() {
        expect:
        (0..10).each {
            assert procrastinator.idleTime < maxIdleTime
        }
    }
}
