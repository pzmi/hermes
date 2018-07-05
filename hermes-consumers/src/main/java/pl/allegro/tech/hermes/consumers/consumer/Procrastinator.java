package pl.allegro.tech.hermes.consumers.consumer;

import static com.google.common.base.Preconditions.checkArgument;

public class Procrastinator {

    private long initialIdleTime;
    private long maxIdleTime;
    private long nextIdleTime;

    public Procrastinator(long initialIdleTime, long maxIdleTime) {
        checkArgument(initialIdleTime > 0, "initialIdleTime should be greater than zero");
        checkArgument(maxIdleTime > 0, "maxIdleTime should be greater than zero");
        checkArgument(initialIdleTime <= maxIdleTime, "maxIdleTime should be grater or equal initialIdleTime");

        this.initialIdleTime = initialIdleTime;
        this.maxIdleTime = maxIdleTime;
        this.nextIdleTime = initialIdleTime;
    }

    public long getIdleTime() {
        long currentIdleTime = nextIdleTime;
        if (nextIdleTime * 2 < maxIdleTime) {
            nextIdleTime *= 2;
        }
        return currentIdleTime;
    }

    public void reportWork() {
        this.nextIdleTime = initialIdleTime;
    }
}
