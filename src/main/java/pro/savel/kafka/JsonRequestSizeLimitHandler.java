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

import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandlerAdapter;
import io.netty.handler.codec.http.HttpContent;
import io.netty.handler.codec.http.HttpHeaderNames;
import io.netty.handler.codec.http.HttpRequest;
import io.netty.handler.codec.http.HttpResponseStatus;
import io.netty.handler.codec.http.HttpUtil;
import io.netty.handler.codec.http.LastHttpContent;
import io.netty.util.ReferenceCountUtil;
import pro.savel.kafka.common.HttpUtils;

public class JsonRequestSizeLimitHandler extends ChannelInboundHandlerAdapter {

    private final int maxContentLength;
    private boolean jsonRequest;
    private boolean discarding;
    private long receivedBytes;

    public JsonRequestSizeLimitHandler(int maxContentLength) {
        if (maxContentLength <= 0)
            throw new IllegalArgumentException("maxContentLength must be greater than 0");
        this.maxContentLength = maxContentLength;
    }

    @Override
    public void channelRead(ChannelHandlerContext ctx, Object msg) {
        if (discarding) {
            ReferenceCountUtil.release(msg);
            return;
        }

        if (msg instanceof HttpRequest request) {
            jsonRequest = HttpUtils.isJson(request.headers().get(HttpHeaderNames.CONTENT_TYPE));
            receivedBytes = 0;
            if (jsonRequest && contentLength(request) > maxContentLength) {
                reject(ctx, msg);
                return;
            }
        }

        if (jsonRequest && msg instanceof HttpContent content) {
            receivedBytes += content.content().readableBytes();
            if (receivedBytes > maxContentLength) {
                reject(ctx, msg);
                return;
            }
        }

        ctx.fireChannelRead(msg);
        if (msg instanceof LastHttpContent)
            reset();
    }

    @Override
    public void channelInactive(ChannelHandlerContext ctx) {
        reset();
        ctx.fireChannelInactive();
    }

    private void reject(ChannelHandlerContext ctx, Object msg) {
        discarding = true;
        ReferenceCountUtil.release(msg);
        HttpUtils.writeHttpResponseAndClose(ctx, HttpResponseStatus.REQUEST_ENTITY_TOO_LARGE, null);
    }

    private void reset() {
        jsonRequest = false;
        discarding = false;
        receivedBytes = 0;
    }

    private static long contentLength(HttpRequest request) {
        try {
            return HttpUtil.getContentLength(request, -1);
        } catch (NumberFormatException ignored) {
            return -1;
        }
    }
}
