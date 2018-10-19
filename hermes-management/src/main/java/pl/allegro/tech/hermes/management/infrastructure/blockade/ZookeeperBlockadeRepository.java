package pl.allegro.tech.hermes.management.infrastructure.blockade;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.curator.framework.CuratorFramework;
import org.apache.zookeeper.KeeperException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import pl.allegro.tech.hermes.common.exception.InternalProcessingException;
import pl.allegro.tech.hermes.infrastructure.zookeeper.ZookeeperBasedRepository;
import pl.allegro.tech.hermes.infrastructure.zookeeper.ZookeeperPaths;
import pl.allegro.tech.hermes.management.domain.blockade.BlockadeAlreadyExistsException;
import pl.allegro.tech.hermes.management.domain.blockade.BlockadeRepository;

public class ZookeeperBlockadeRepository extends ZookeeperBasedRepository implements BlockadeRepository {
    private static final Logger logger = LoggerFactory.getLogger(ZookeeperBlockadeRepository.class);

    public ZookeeperBlockadeRepository(CuratorFramework zookeeper, ObjectMapper mapper, ZookeeperPaths paths) {
        super(zookeeper, mapper, paths);
    }

    public void block() {
        ensureConnected();

        String blockadesPath = paths.blockadesPath();
        logger.info("Creating blockade on path {}", blockadesPath);

        try {
            zookeeper.inTransaction()
                    .create().forPath(blockadesPath)
                    .and().commit();
        } catch (KeeperException.NodeExistsException ex) {
            throw new BlockadeAlreadyExistsException(ex);
        } catch (Exception ex) {
            throw new InternalProcessingException(ex);
        }
    }

    @Override
    public boolean isBlocked() {
        ensureConnected();

        return pathExists(paths.blockadesPath());
    }

    @Override
    public void unblock() {
        ensureConnected();

        String blockadesPath = paths.blockadesPath();
        logger.info("Removing blockade on path {}", blockadesPath);

        remove(blockadesPath);
    }
}
