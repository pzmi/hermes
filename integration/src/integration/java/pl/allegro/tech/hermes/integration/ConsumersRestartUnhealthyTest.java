package pl.allegro.tech.hermes.integration;

import org.testng.annotations.Test;
import pl.allegro.tech.hermes.api.Topic;
import pl.allegro.tech.hermes.integration.env.CustomKafkaStarter;
import pl.allegro.tech.hermes.test.helper.environment.KafkaStarter;


public class ConsumersRestartUnhealthyTest extends IntegrationTest {

	@Test
	public void restartUnhealthyCorrectness() throws Exception {
		// given

		Topic topic = operations.buildTopic("publishAndConsumeGroup", "topic");
		operations.createSubscription(topic, "subscription", HTTP_ENDPOINT_URL);


		STARTERS.get(KafkaStarter.class).stop();
		STARTERS.get(CustomKafkaStarter.class).stop();
		Thread.sleep(1000 * 180);

		STARTERS.get(KafkaStarter.class).start();
		STARTERS.get(CustomKafkaStarter.class).start();

		Thread.sleep(10000000);
	}
}
