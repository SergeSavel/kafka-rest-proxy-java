package pro.savel.kafka;

import io.netty.channel.ChannelHandler;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.SimpleChannelInboundHandler;
import io.netty.handler.codec.http.FullHttpRequest;
import pro.savel.kafka.common.HttpUtils;
import pro.savel.kafka.producer.ProducerRequestDecoder;

import java.io.IOException;

@ChannelHandler.Sharable
public class RequestDecoder extends SimpleChannelInboundHandler<FullHttpRequest> {

    private static final ProducerRequestDecoder producerDecoder = new ProducerRequestDecoder();

    @Override
    protected void channelRead0(ChannelHandlerContext ctx, FullHttpRequest httpRequest) throws IOException {
        var uri = httpRequest.uri();
        if (uri.startsWith(ProducerRequestDecoder.URI_PREFIX)) {
            producerDecoder.decode(ctx, httpRequest);
        }
        else {
            HttpUtils.writeNotFound(ctx, httpRequest.protocolVersion());
        }
    }
}
