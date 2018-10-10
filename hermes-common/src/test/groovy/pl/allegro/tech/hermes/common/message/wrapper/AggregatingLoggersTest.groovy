package pl.allegro.tech.hermes.common.message.wrapper

import org.slf4j.Logger
import spock.lang.Specification

import java.util.concurrent.CountDownLatch
import java.util.concurrent.Executors
import java.util.concurrent.TimeUnit

class AggregatingLoggersTest extends Specification {

    def "should report aggregated counters for defined loggers"() {
        given:
        def targetLogger = Mock(Logger)
        def loggers = new AggregatingLoggers(targetLogger)

        def valueLogger = loggers.addLogger({ int value -> "Invalid value: $value"})
        def nameLogger = loggers.addLogger({ String name -> "Invalid name: $name"})

        when:
        3.times {
            nameLogger.mark("John")
        }
        7.times {
            nameLogger.mark("Snow")
        }
        10.times {
            valueLogger.mark(1)
        }
        20.times {
            valueLogger.mark(3)
        }

        and:
        loggers.report()

        then:
        1 * targetLogger.error("Invalid value: 1 [occurrences=10]")
        1 * targetLogger.error("Invalid value: 3 [occurrences=20]")
        1 * targetLogger.error("Invalid name: John [occurrences=3]")
        1 * targetLogger.error("Invalid name: Snow [occurrences=7]")
    }

    def "should not report when there are no logs"() {
        given:
        def targetLogger = Mock(Logger)
        def loggers = new AggregatingLoggers(targetLogger)
        def logger = loggers.addLogger({"Got $it"})

        when:
        loggers.report()

        then:
        0 * targetLogger.error(_, _)
        0 * targetLogger.error(_)

        when:
        logger.mark("hello")

        and:
        loggers.report()

        then:
        1 * targetLogger.error("Got hello [occurrences=1]")

        when:
        loggers.report()

        then:
        0 * targetLogger.error(_, _)
        0 * targetLogger.error(_)
    }

    def "should log last exception"() {
        given:
        def targetLogger = Mock(Logger)
        def loggers = new AggregatingLoggers(targetLogger)
        def logger = loggers.addLogger({"Got $it"})

        def forgottenException = new RuntimeException("An exception that won't get logged")
        def exception = new RuntimeException("An error")

        when: "last call does not log exception"
        logger.mark(22, forgottenException)
        logger.mark(22, exception)
        logger.mark(22) // last call does not log exception

        and:
        loggers.report()

        then: "exception is logged"
        1 * targetLogger.error("Got 22 [occurrences=3]", exception)
    }

    def "should allow multithreaded access"() {
        given:
        def targetLogger = Mock(Logger)
        def loggers = new AggregatingLoggers(targetLogger)
        def logger = loggers.addLogger({"Got $it"})
        def threadsCount = 5
        def latch = new CountDownLatch(threadsCount)
        def logsPerThread = 1_000_000
        def executor = Executors.newFixedThreadPool(threadsCount)

        when:
        threadsCount.times {
            executor.submit({
                logsPerThread.times {
                    logger.mark("A")
                    logger.mark("B")
                }
                latch.countDown()
            })
        }

        and:
        latch.await()
        loggers.report()

        then:
        1 * targetLogger.error("Got A [occurrences=${threadsCount * logsPerThread}]")
        1 * targetLogger.error("Got B [occurrences=${threadsCount * logsPerThread}]")

    }

    def "should allow multithreaded access while reporting simultaneously"() {
        given:
        def countRegexp = /Got A \[occurrences\=(\d+)\]/
        def count = 0
        def loggerCalls = 0
        def targetLogger = [error: {msg ->
            loggerCalls++
            def matcher = (msg =~ countRegexp) // we need to parse the number of occurrences
            assert matcher.matches()
            count += matcher.group(1) as Integer
        }] as Logger

        def loggers = new AggregatingLoggers(targetLogger)
        def logger = loggers.addLogger({"Got $it"})
        def threadsCount = 5
        def latch = new CountDownLatch(threadsCount)
        def logsPerThread = 1_000_000
        def executor = Executors.newFixedThreadPool(threadsCount)
        def scheduled = Executors.newSingleThreadScheduledExecutor()

        when: "the reporter reports and clears counters multiple times"
        scheduled.scheduleAtFixedRate({
            loggers.report()
        }, 0, 10, TimeUnit.MILLISECONDS)

        and: "multiple threads log simultaneously"
        threadsCount.times {
            executor.submit({
                logsPerThread.times {
                    logger.mark("A")
                }
                latch.countDown()
            })
        }
        latch.await()
        scheduled.shutdown()

        and: "we make sure everything gets flushed"
        loggers.report()

        then: "we know there were multiple calls to the target logger"
        loggerCalls > 1

        and: "total number of logs is correct"
        count == threadsCount * logsPerThread
    }
}
