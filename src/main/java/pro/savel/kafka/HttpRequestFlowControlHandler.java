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

import io.netty.channel.ChannelDuplexHandler;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelPromise;
import io.netty.handler.codec.http.FullHttpRequest;
import io.netty.handler.codec.http.LastHttpContent;
import io.netty.util.ReferenceCountUtil;

import java.util.ArrayDeque;
import java.util.Queue;
import java.util.concurrent.RejectedExecutionException;

public class HttpRequestFlowControlHandler extends ChannelDuplexHandler {

    private final Queue<FullHttpRequest> pendingRequests = new ArrayDeque<>();
    private boolean requestInProgress;

    @Override
    public void channelRead(ChannelHandlerContext ctx, Object msg) {
        if (!(msg instanceof FullHttpRequest request)) {
            ctx.fireChannelRead(msg);
            return;
        }

        if (requestInProgress) {
            pendingRequests.add(request);
            return;
        }

        requestInProgress = true;
        ctx.channel().config().setAutoRead(false);
        ctx.fireChannelRead(request);
    }

    @Override
    public void write(ChannelHandlerContext ctx, Object msg, ChannelPromise promise) {
        if (!(msg instanceof LastHttpContent)) {
            ctx.write(msg, promise);
            return;
        }

        var completionPromise = promise.unvoid();
        completionPromise.addListener(future -> scheduleResponseCompletion(ctx, future.isSuccess()));
        ctx.write(msg, completionPromise);
    }

    @Override
    public void channelInactive(ChannelHandlerContext ctx) {
        requestInProgress = false;
        releasePendingRequests();
        ctx.fireChannelInactive();
    }

    @Override
    public void handlerRemoved(ChannelHandlerContext ctx) {
        var resumeReading = requestInProgress;
        requestInProgress = false;
        releasePendingRequests();
        if (resumeReading && ctx.channel().isActive())
            ctx.channel().config().setAutoRead(true);
    }

    private void scheduleResponseCompletion(ChannelHandlerContext ctx, boolean success) {
        try {
            ctx.executor().execute(() -> completeResponse(ctx, success));
        } catch (RejectedExecutionException ignored) {
            releasePendingRequests();
        }
    }

    private void completeResponse(ChannelHandlerContext ctx, boolean success) {
        if (!success) {
            releasePendingRequests();
            ctx.close();
            return;
        }
        if (!ctx.channel().isActive()) {
            releasePendingRequests();
            return;
        }

        var nextRequest = pendingRequests.poll();
        if (nextRequest != null) {
            ctx.fireChannelRead(nextRequest);
            ctx.fireChannelReadComplete();
        } else {
            requestInProgress = false;
            ctx.channel().config().setAutoRead(true);
        }
    }

    private void releasePendingRequests() {
        FullHttpRequest request;
        while ((request = pendingRequests.poll()) != null)
            ReferenceCountUtil.release(request);
    }
}
