// Copyright 2026 Sergey Savelev (serge@savel.pro)
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
import io.netty.handler.codec.http.FullHttpRequest;
import io.netty.handler.codec.http.HttpMethod;
import io.netty.util.ReferenceCountUtil;
import pro.savel.kafka.common.HttpUtils;

@ChannelHandler.Sharable
public class HealthRequestDecoder extends ChannelInboundHandlerAdapter {

    private static final String URI = "/health";

    @Override
    public void channelRead(ChannelHandlerContext ctx, Object msg) {
        if (msg instanceof FullHttpRequest httpRequest && httpRequest.uri().equals(URI)) {
            try {
                if (httpRequest.method() == HttpMethod.GET) {
                    HttpUtils.writeOkAndClose(ctx, "{\"status\":\"UP\"}");
                } else {
                    HttpUtils.writeMethodNotAllowedAndClose(ctx, null);
                }
            } finally {
                ReferenceCountUtil.release(msg);
            }
        } else {
            ctx.fireChannelRead(msg);
        }
    }
}
