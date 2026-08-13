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
import io.netty.handler.timeout.ReadTimeoutHandler;

import java.util.concurrent.TimeUnit;

/**
 * A read timeout that only counts time the gateway was actually willing to read.
 * <p>
 * {@link HttpRequestFlowControlHandler} clears auto-read for the whole lifetime of a request - a
 * consumer poll blocking on Kafka and the chunked response streamed afterwards included. A plain
 * {@link ReadTimeoutHandler} would count that as client inactivity and kill the connection while
 * its response is still being written.
 */
public class ClientReadTimeoutHandler extends ReadTimeoutHandler {

    public ClientReadTimeoutHandler(long timeout, TimeUnit unit) {
        super(timeout, unit);
    }

    @Override
    public void read(ChannelHandlerContext ctx) {
        // Reached when auto-read is turned back on after a response completes, and once per read
        // cycle otherwise: the point where the gateway starts waiting on the client again.
        resetReadTimeout();
        ctx.read();
    }

    @Override
    protected void readTimedOut(ChannelHandlerContext ctx) throws Exception {
        if (!ctx.channel().config().isAutoRead())
            return;
        super.readTimedOut(ctx);
    }
}
