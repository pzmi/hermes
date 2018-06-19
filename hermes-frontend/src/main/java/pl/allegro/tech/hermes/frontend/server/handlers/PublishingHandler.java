package pl.allegro.tech.hermes.frontend.server.handlers;

import io.netty.channel.ChannelFutureListener;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandlerAdapter;
import io.netty.handler.codec.http.DefaultFullHttpResponse;
import io.netty.handler.codec.http.FullHttpRequest;
import io.netty.handler.codec.http.FullHttpResponse;
import io.netty.handler.codec.http.HttpUtil;
import io.netty.util.AsciiString;
import org.apache.kafka.clients.producer.ProducerRecord;
import pl.allegro.tech.hermes.frontend.cache.topic.TopicsCache;
import pl.allegro.tech.hermes.frontend.metric.CachedTopic;
import pl.allegro.tech.hermes.frontend.producer.kafka.Producers;

import static io.netty.handler.codec.http.HttpResponseStatus.OK;
import static io.netty.handler.codec.http.HttpVersion.HTTP_1_1;


class PublishingHandler extends ChannelInboundHandlerAdapter {
    private final Producers producers;
    private final TopicsCache topicsCache;

    private static final String TOPICS_PATH_PREFIX = "/topics/";

    private static final AsciiString CONTENT_TYPE = AsciiString.cached("Content-Type");
    private static final AsciiString CONTENT_LENGTH = AsciiString.cached("Content-Length");
    private static final AsciiString CONNECTION = AsciiString.cached("Connection");
    private static final AsciiString KEEP_ALIVE = AsciiString.cached("keep-alive");

    public PublishingHandler(Producers producers, TopicsCache topicsCache) {
        this.producers = producers;
        this.topicsCache = topicsCache;
    }

    @Override
    public void channelRead(ChannelHandlerContext ctx, Object msg) {
        if (msg instanceof FullHttpRequest) {
            FullHttpRequest request = (FullHttpRequest) msg;

            boolean keepAlive = HttpUtil.isKeepAlive(request);

            byte[] bytes = new byte[request.content().readableBytes()];
            request.content().readBytes(bytes);

            CachedTopic topic = topicsCache.getTopic(getTopicName(request.uri()))
                    .orElseThrow(() -> new RuntimeException("Topic not found")); // TODO

            ProducerRecord<byte[], byte[]> producerRecord = new ProducerRecord<>(topic.getQualifiedName(), bytes);

            request.release();

            producers.get(topic.getTopic()).send(producerRecord, (metadata, exception) -> {
                        if (exception != null) {
                            throw new RuntimeException("Exception rethrowed from callback", exception); // TODO handle better
                        } else {
                            ctx.executor().execute(() -> {
                                FullHttpResponse response = new DefaultFullHttpResponse(HTTP_1_1, OK);
                                response.headers().set(CONTENT_TYPE, "application/json");
                                response.headers().setInt(CONTENT_LENGTH, 0);

                                if (!keepAlive) {
                                    ctx.writeAndFlush(response).addListener(ChannelFutureListener.CLOSE);
                                } else {
                                    response.headers().set(CONNECTION, KEEP_ALIVE);
                                    ctx.writeAndFlush(response);
                                }
                            });
                        }
            });
        }
        // TODO handle case where msg is not type of FullHttpRequest
    }

    private String getTopicName(String path) {
        if (path.startsWith(TOPICS_PATH_PREFIX)) {
            return path.substring(TOPICS_PATH_PREFIX.length());
        }
        throw new RuntimeException("Unknown path"); // TODO implement
    }
}
