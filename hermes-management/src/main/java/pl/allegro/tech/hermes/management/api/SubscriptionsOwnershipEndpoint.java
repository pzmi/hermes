package pl.allegro.tech.hermes.management.api;

import java.util.List;
import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import org.springframework.beans.factory.annotation.Autowired;
import pl.allegro.tech.hermes.api.OwnerId;
import pl.allegro.tech.hermes.api.Subscription;
import pl.allegro.tech.hermes.management.domain.owner.OwnerSource;
import pl.allegro.tech.hermes.management.domain.owner.OwnerSourceNotFound;
import pl.allegro.tech.hermes.management.domain.owner.OwnerSources;
import pl.allegro.tech.hermes.management.domain.subscription.SubscriptionService;

import static javax.ws.rs.core.MediaType.APPLICATION_JSON;

@Path("subscriptions/owner")
public class SubscriptionsOwnershipEndpoint {

	private final OwnerSources ownerSources;
	private final SubscriptionService subscriptionService;

	@Autowired
	public SubscriptionsOwnershipEndpoint(OwnerSources ownerSources,
	                                      SubscriptionService subscriptionService) {
		this.ownerSources = ownerSources;
		this.subscriptionService = subscriptionService;
	}

	@GET
	@Produces(APPLICATION_JSON)
	@Path("/{ownerSourceName}/{ownerId}")
	public List<Subscription> listForOwner(@PathParam("ownerSourceName") String ownerSourceName, @PathParam("ownerId") String id) {
		OwnerSource ownerSource = ownerSources.getByName(ownerSourceName)
				.orElseThrow(() -> new OwnerSourceNotFound(ownerSourceName));
		if (!ownerSource.exists(id)) {
			throw new OwnerSource.OwnerNotFound(ownerSourceName, id);
		}
		OwnerId ownerId = new OwnerId(ownerSource.name(), id);
		return subscriptionService.listForOwnerId(ownerId);
	}
}
