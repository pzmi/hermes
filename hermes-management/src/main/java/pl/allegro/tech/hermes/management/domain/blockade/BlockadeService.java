package pl.allegro.tech.hermes.management.domain.blockade;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

@Component
public class BlockadeService {

    private BlockadeRepository blockadeRepository;

    @Autowired
    public BlockadeService(BlockadeRepository blockadeRepository) {
        this.blockadeRepository = blockadeRepository;
    }

    public void block() {
        blockadeRepository.block();
    }

    public void unblock() {
        blockadeRepository.unblock();
    }

    public boolean isBlocked() {
        return blockadeRepository.isBlocked();
    }
}
