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

import io.netty.channel.embedded.EmbeddedChannel;
import io.netty.handler.timeout.ReadTimeoutException;
import io.netty.handler.timeout.WriteTimeoutException;
import org.junit.jupiter.api.Test;
import pro.savel.kafka.admin.AdminProvider;
import pro.savel.kafka.admin.AdminRequestProcessor;
import pro.savel.kafka.common.SynchronousBlockingTaskExecutor;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;

/**
 * Guards against exceptionCaught being swallowed by a *RequestProcessor before it reaches
 * DefaultInboundHandler's ReadTimeoutException/WriteTimeoutException handling further down the pipeline.
 * A swallowed exception would leave the channel open.
 */
class PipelineExceptionPropagationTest {

    private EmbeddedChannel newChannel() {
        var processor = new AdminRequestProcessor(new SynchronousBlockingTaskExecutor(),
                new AdminProvider(config -> null));
        return new EmbeddedChannel(processor, new DefaultInboundHandler());
    }

    @Test
    void readTimeout_propagatesPastProcessor_toDefaultInboundHandler() {
        var channel = newChannel();

        channel.pipeline().fireExceptionCaught(ReadTimeoutException.INSTANCE);

        assertFalse(channel.isOpen());
        assertNull(channel.readOutbound());
        channel.finishAndReleaseAll();
    }

    @Test
    void writeTimeout_propagatesPastProcessor_toDefaultInboundHandler() {
        var channel = newChannel();

        channel.pipeline().fireExceptionCaught(WriteTimeoutException.INSTANCE);

        assertFalse(channel.isOpen());
        assertNull(channel.readOutbound());
        channel.finishAndReleaseAll();
    }
}
