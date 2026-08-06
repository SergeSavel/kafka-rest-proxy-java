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

package pro.savel.kafka.common;

import io.netty.channel.ChannelInboundHandlerAdapter;
import io.netty.channel.embedded.EmbeddedChannel;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.assertTrue;

class BlockingTaskExecutorTest {

    @Test
    void close_interruptsActiveTasks() throws InterruptedException {
        var executor = new BlockingTaskExecutor();
        var channel = new EmbeddedChannel(new ChannelInboundHandlerAdapter());
        var taskStarted = new CountDownLatch(1);
        var taskInterrupted = new CountDownLatch(1);

        executor.execute(channel.pipeline().firstContext(), () -> {
            taskStarted.countDown();
            try {
                new CountDownLatch(1).await();
            } catch (InterruptedException e) {
                taskInterrupted.countDown();
                throw e;
            }
            return null;
        }, (result, error) -> { });

        assertTrue(taskStarted.await(1, TimeUnit.SECONDS));
        executor.close(ShutdownDeadline.after(Duration.ofSeconds(1)));

        assertTrue(taskInterrupted.await(1, TimeUnit.SECONDS));
        channel.finishAndReleaseAll();
    }
}
