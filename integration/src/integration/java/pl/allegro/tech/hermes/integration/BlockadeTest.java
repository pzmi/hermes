package pl.allegro.tech.hermes.integration;

import org.junit.After;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.Test;
import pl.allegro.tech.hermes.api.ContentType;
import pl.allegro.tech.hermes.api.PatchData;
import pl.allegro.tech.hermes.api.Topic;
import pl.allegro.tech.hermes.test.helper.avro.AvroUserSchemaLoader;

import javax.ws.rs.core.Response;

import static javax.ws.rs.core.Response.Status.FORBIDDEN;
import static pl.allegro.tech.hermes.api.ContentType.JSON;
import static pl.allegro.tech.hermes.api.PatchData.patchData;
import static pl.allegro.tech.hermes.integration.test.HermesAssertions.assertThat;
import static pl.allegro.tech.hermes.test.helper.builder.SubscriptionBuilder.subscription;
import static pl.allegro.tech.hermes.test.helper.builder.TopicBuilder.topic;

public class BlockadeTest extends IntegrationTest {

    @AfterMethod
    public void cleanup() {
        management.blockade().unblockManagement();
//        wait.untilBlockadeRemoved();
    }

    @Test
    public void shouldBlockManagmentChanges() {
        // given
        Topic topic = operations.buildTopic("subscribeGroup", "topic");

        // when
        management.blockade().blockManagement();
        wait.untilBlockadeSet();
        Response response = management.subscription().create(
                topic.getQualifiedName(),
                subscription("subscribeGroup.topic", "subscription").build());

        // then
        assertThat(response).hasStatus(FORBIDDEN);
        management.blockade().unblockManagement();
                wait.untilBlockadeRemoved();

    }

    @Test
    public void shouldUnblockManagementChanges() {
//        // given
//        Topic topic = operations.buildTopic("group", "topic");
//        TestMessage message = TestMessage.of("hello", "world");
//
//        // when
//        management.blacklist().blacklistTopics(Arrays.asList("group.topic"));
//        wait.untilTopicBlacklisted("group.topic");
//
//        Response response = publisher.publish(topic.getQualifiedName(), message.body());
//
//        // then
//        assertThat(response).hasStatus(FORBIDDEN);
    }

    @Test
    public void shouldAllowConfigRetrievalWhenBlocked() {
//        // given
//        Topic topic = operations.buildTopic("group", "topic");
//        TestMessage message = TestMessage.of("hello", "world");
//
//        // when
//        management.blacklist().blacklistTopics(Arrays.asList("group.topic"));
//        wait.untilTopicBlacklisted("group.topic");
//
//        Response response = publisher.publish(topic.getQualifiedName(), message.body());
//
//        // then
//        assertThat(response).hasStatus(FORBIDDEN);
    }
}
