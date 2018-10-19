package pl.allegro.tech.hermes.management.domain.blockade;

public interface BlockadeRepository {
    void block();
    boolean isBlocked();
    void unblock();
}
