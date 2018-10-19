package pl.allegro.tech.hermes.management.domain.blockade;

import org.apache.zookeeper.KeeperException;
import pl.allegro.tech.hermes.api.ErrorCode;
import pl.allegro.tech.hermes.common.exception.HermesException;

public class BlockadeAlreadyExistsException extends HermesException {
    public BlockadeAlreadyExistsException(KeeperException.NodeExistsException ex) {
        super(ex);
    }

    @Override
    public ErrorCode getCode() {
        return ErrorCode.BLOCKADE_ALREADY_EXISTS;
    }
}
