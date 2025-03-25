package pro.savel.kafka;

import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.SimpleChannelInboundHandler;
import pro.savel.kafka.common.contract.RequestBearer;

public class RequestProcessor extends SimpleChannelInboundHandler<RequestBearer> {

    @Override
    protected void channelRead0(ChannelHandlerContext ctx, RequestBearer bearer) throws Exception {
        var a = 1;
    }
}
