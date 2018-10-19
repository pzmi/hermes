package pl.allegro.tech.hermes.management.api;

import com.wordnik.swagger.annotations.ApiOperation;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;
import pl.allegro.tech.hermes.management.api.auth.Roles;
import pl.allegro.tech.hermes.management.domain.blockade.BlockadeService;

import javax.annotation.security.RolesAllowed;
import javax.ws.rs.DELETE;
import javax.ws.rs.GET;
import javax.ws.rs.HttpMethod;
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.core.Response;

@Path("blockades")
@Component
public class BlockadesEndpoint {
private static final Logger LOGGER = LoggerFactory.getLogger(BlockadesEndpoint.class);
    private final BlockadeService blockadeService;

    public BlockadesEndpoint(BlockadeService blockadeService) {
        this.blockadeService = blockadeService;
    }

    @GET
    @ApiOperation(value = "Get set up blockades", httpMethod = HttpMethod.GET)
    public BlockadesResponse isManagementBlocked() {
        return new BlockadesResponse(blockadeService.isBlocked());
    }

    @POST
    @RolesAllowed(Roles.ADMIN)
    @Path("/management")
    @ApiOperation(value = "Create blockade on all management changes", httpMethod = HttpMethod.POST)
    public Response blockManagement() {
        LOGGER.info("Creating blockade");
        blockadeService.block();
        return Response.status(Response.Status.CREATED).build();

    }

    @DELETE
    @RolesAllowed(Roles.ADMIN)
    @Path("/management")
    @ApiOperation(value = "Remove blockade on all management changes", httpMethod = HttpMethod.POST)
    public Response unblockManagement() {
        LOGGER.info("Removing blockade");

        blockadeService.unblock();
        return Response.status(Response.Status.OK).build();
    }
}
