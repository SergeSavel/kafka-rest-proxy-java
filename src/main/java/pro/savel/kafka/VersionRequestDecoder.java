package pro.savel.kafka;

import io.netty.channel.ChannelHandler;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandlerAdapter;
import io.netty.handler.codec.http.FullHttpRequest;
import io.netty.handler.codec.http.HttpMethod;
import io.netty.util.ReferenceCountUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import pro.savel.kafka.common.HttpUtils;
import pro.savel.kafka.common.Utils;
import pro.savel.kafka.common.exceptions.BadRequestException;

@ChannelHandler.Sharable
public class VersionRequestDecoder extends ChannelInboundHandlerAdapter {

    public static final String URI_PREFIX = "/version";
    private static final Logger logger = LoggerFactory.getLogger(VersionRequestDecoder.class);

    @Override
    public void channelRead(ChannelHandlerContext ctx, Object msg) {
        if (msg instanceof FullHttpRequest httpRequest && httpRequest.uri().startsWith(URI_PREFIX)) {
            try {
                decode(ctx, httpRequest);
            } catch (BadRequestException e) {
                HttpUtils.writeBadRequestAndClose(ctx, httpRequest.protocolVersion(), Utils.combineErrorMessage(e));
            } catch (Exception e) {
                logger.error("An unexpected error occurred while decoding version request.", e);
                HttpUtils.writeInternalServerErrorAndClose(ctx, httpRequest.protocolVersion(), Utils.combineErrorMessage(e));
            } finally {
                ReferenceCountUtil.release(msg);
            }
        } else {
            ctx.fireChannelRead(msg);
        }
    }

    private void decode(ChannelHandlerContext ctx, FullHttpRequest httpRequest) throws BadRequestException {
        var pathMethod = httpRequest.uri().substring(URI_PREFIX.length());
        if (pathMethod.isEmpty()) {
            decodeRoot(ctx, httpRequest);
        } else {
            HttpUtils.writeNotFoundAndClose(ctx, httpRequest.protocolVersion());
        }
    }

    private void decodeRoot(ChannelHandlerContext ctx, FullHttpRequest httpRequest) throws BadRequestException {
        if (httpRequest.method() == HttpMethod.GET) {
            var pkg = VersionRequestDecoder.class.getPackage();
            HttpUtils.writeOkAndClose(ctx, httpRequest.protocolVersion(), pkg.getImplementationVersion());
        } else {
            throw new BadRequestException("Unsupported HTTP method.");
        }
    }
}
