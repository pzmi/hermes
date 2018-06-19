package pl.allegro.tech.hermes.frontend.server.handlers;

import io.netty.channel.ChannelInitializer;
import io.netty.channel.socket.SocketChannel;
import io.netty.handler.codec.http.HttpObjectAggregator;
import io.netty.handler.codec.http.HttpServerCodec;
import pl.allegro.tech.hermes.frontend.cache.topic.TopicsCache;
import pl.allegro.tech.hermes.frontend.producer.kafka.Producers;

import javax.inject.Inject;

public class HttpHandlersPipeline extends ChannelInitializer<SocketChannel> {

    private final Producers producers;
    private final TopicsCache topicsCache;

    @Inject
    public HttpHandlersPipeline(Producers producers, TopicsCache topicsCache) {
        this.producers = producers;
        this.topicsCache = topicsCache;
    }

    @Override
    protected void initChannel(SocketChannel ch) {
        ch.pipeline()
        .addLast(new HttpServerCodec())
        .addLast(new HttpObjectAggregator(Integer.MAX_VALUE)) //TODO provide max request size
        .addLast(new PublishingHandler(producers, topicsCache));
    }
}
