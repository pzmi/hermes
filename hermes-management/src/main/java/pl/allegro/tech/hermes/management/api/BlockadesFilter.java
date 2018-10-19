package pl.allegro.tech.hermes.management.api;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import pl.allegro.tech.hermes.management.domain.blockade.BlockadeService;

import javax.annotation.Priority;
import javax.ws.rs.container.ContainerRequestContext;
import javax.ws.rs.container.ContainerRequestFilter;
import javax.ws.rs.core.Response;
import javax.ws.rs.ext.Provider;
import java.util.regex.Pattern;

@Provider
@Priority(999)
public class BlockadesFilter implements ContainerRequestFilter {
    private static final Pattern PATH_WHITELIST = Pattern.compile("^/blockades");
    private static final Pattern METHOD_WHITELIST = Pattern.compile("GET");

    private static final Logger LOGGER = LoggerFactory.getLogger(BlockadesFilter.class);

    private final BlockadeService blockadeService;

    @Autowired
    public BlockadesFilter(BlockadeService blockadeService) {
        this.blockadeService = blockadeService;
    }

    @Override
    public void filter(ContainerRequestContext requestContext) {
        LOGGER.info("Checking blockade filter");
        if (shouldReject(requestContext)) {
            LOGGER.info("Blockade rejected");

            requestContext.abortWith(Response.status(Response.Status.FORBIDDEN).build());
        }
    }

    private boolean shouldReject(ContainerRequestContext request) {
        return blockadeService.isBlocked() && !isRequestWhitelisted(request);
    }

    private static boolean isRequestWhitelisted(ContainerRequestContext request) {
        String path = request.getUriInfo().getPath();
        String method = request.getMethod();
        return METHOD_WHITELIST.matcher(method).matches() || PATH_WHITELIST.matcher(path).matches();
    }
}
