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
import io.netty.channel.embedded.EmbeddedChannel;
import io.netty.handler.timeout.ReadTimeoutException;
import org.junit.jupiter.api.Test;

import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

class ClientReadTimeoutHandlerTest {

    private static final int TIMEOUT_SECONDS = 10;

    private final ExceptionCapture capture = new ExceptionCapture();

    private EmbeddedChannel newChannel() {
        return new EmbeddedChannel(new ClientReadTimeoutHandler(TIMEOUT_SECONDS, TimeUnit.SECONDS), capture);
    }

    private static void elapse(EmbeddedChannel channel, int seconds) {
        channel.advanceTimeBy(seconds, TimeUnit.SECONDS);
        channel.runScheduledPendingTasks();
    }

    @Test
    void idleClient_timesOut() {
        var channel = newChannel();

        elapse(channel, TIMEOUT_SECONDS + 1);

        assertInstanceOf(ReadTimeoutException.class, capture.cause);
        assertFalse(channel.isOpen());
        channel.finishAndReleaseAll();
    }

    @Test
    void readsSuspendedByFlowControl_doesNotTimeOut() {
        var channel = newChannel();
        // What HttpRequestFlowControlHandler does for the whole lifetime of a request.
        channel.config().setAutoRead(false);

        elapse(channel, TIMEOUT_SECONDS * 3);

        assertNull(capture.cause, "gateway processing time must not be charged to the client");
        assertTrue(channel.isOpen());
        channel.finishAndReleaseAll();
    }

    @Test
    void resumedReads_giveClientFullIdleWindowAgain() {
        var channel = newChannel();
        channel.config().setAutoRead(false);
        elapse(channel, TIMEOUT_SECONDS * 3);

        channel.config().setAutoRead(true);
        elapse(channel, TIMEOUT_SECONDS - 1);

        assertNull(capture.cause, "idle window must restart when the gateway starts waiting on the client again");
        assertTrue(channel.isOpen());

        elapse(channel, 2);

        assertInstanceOf(ReadTimeoutException.class, capture.cause);
        assertFalse(channel.isOpen());
        channel.finishAndReleaseAll();
    }

    private static class ExceptionCapture extends ChannelInboundHandlerAdapter {

        private Throwable cause;

        @Override
        public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) {
            this.cause = cause;
        }
    }
}
