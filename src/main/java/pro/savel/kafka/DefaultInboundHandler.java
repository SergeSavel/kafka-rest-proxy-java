// Copyright 2025 Sergey Savelev (serge@savel.pro)
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package pro.savel.kafka;

import io.netty.channel.ChannelHandler;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandlerAdapter;
import io.netty.handler.codec.http.HttpContent;
import io.netty.handler.codec.http.HttpResponseStatus;
import io.netty.handler.timeout.ReadTimeoutException;
import io.netty.handler.timeout.WriteTimeoutException;
import io.netty.util.ReferenceCountUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import pro.savel.kafka.common.HttpUtils;

@ChannelHandler.Sharable
public class DefaultInboundHandler extends ChannelInboundHandlerAdapter {

    private static final Logger logger = LoggerFactory.getLogger(DefaultInboundHandler.class);

    @Override
    public void channelRead(ChannelHandlerContext ctx, Object msg) {
        try {
            if (msg instanceof HttpContent) {
                // Leftover content of a request already handled or rejected upstream.
                // Dropping it must not close the channel, otherwise an in-flight or subsequent
                // response on the same connection gets truncated.
                return;
            }
            logger.error("Unexpected request type: {}", msg.getClass().getName());
            ctx.close();
        } finally {
            ReferenceCountUtil.release(msg);
        }
    }

    @Override
    public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) {
        if (cause instanceof ReadTimeoutException) {
            HttpUtils.writeHttpResponseAndClose(ctx, HttpResponseStatus.REQUEST_TIMEOUT, null);
        } else if (cause instanceof WriteTimeoutException) {
            HttpUtils.writeHttpResponseAndClose(ctx, HttpResponseStatus.GATEWAY_TIMEOUT, null);
        } else {
            logger.error("Unhandled pipeline exception.", cause);
            ctx.close();
        }
    }
}
