package pl.allegro.tech.hermes.api.endpoints;

import javax.ws.rs.DELETE;
import javax.ws.rs.GET;
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.core.Response;

@Path("blockades")
public interface BlockadeEndpoint {

    @GET
    Response isManagementBlocked();

    @POST
    @Path("/management")
    Response blockManagement();

    @DELETE
    @Path("/management")
    Response unblockManagement();
}
