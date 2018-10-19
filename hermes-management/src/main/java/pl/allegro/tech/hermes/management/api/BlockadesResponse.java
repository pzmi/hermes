package pl.allegro.tech.hermes.management.api;

public class BlockadesResponse {
    private final boolean management;

    public BlockadesResponse(boolean management) {
        this.management = management;
    }

    public boolean isManagement() {
        return management;
    }
}
