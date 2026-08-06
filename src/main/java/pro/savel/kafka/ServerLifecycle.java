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

import io.netty.channel.Channel;
import io.netty.channel.EventLoopGroup;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import pro.savel.kafka.common.ShutdownDeadline;

import java.time.Duration;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

final class ServerLifecycle implements AutoCloseable {

    private static final Logger logger = LoggerFactory.getLogger(ServerLifecycle.class);

    private final Duration timeout;
    private final List<ShutdownStep> steps;
    private final AtomicBoolean shutdownStarted = new AtomicBoolean();
    private final CountDownLatch shutdownCompleted = new CountDownLatch(1);

    ServerLifecycle(Channel serverChannel, ServerInitializer initializer,
                    EventLoopGroup bossGroup, EventLoopGroup workerGroup, Duration timeout) {
        this(timeout,
                deadline -> closeServerChannel(serverChannel, deadline),
                initializer::close,
                deadline -> shutdownEventLoops(List.of(bossGroup, workerGroup), deadline));
    }

    ServerLifecycle(Duration timeout, ShutdownStep... steps) {
        if (timeout == null || timeout.isNegative() || timeout.isZero())
            throw new IllegalArgumentException("timeout must be greater than 0");
        this.timeout = timeout;
        this.steps = List.of(steps);
    }

    @Override
    public void close() {
        if (!shutdownStarted.compareAndSet(false, true)) {
            awaitShutdownCompletion();
            return;
        }

        var startedAt = System.nanoTime();
        var deadline = ShutdownDeadline.after(timeout);
        logger.info("Server is shutting down...");
        try {
            for (var step : steps) {
                try {
                    step.shutdown(deadline);
                } catch (Throwable e) {
                    logger.error("Shutdown step failed.", e);
                }
            }
            if (deadline.isExpired())
                logger.warn("Server shutdown exceeded its {} second deadline.", timeout.toSeconds());
            else
                logger.info("Shutdown completed in {} ms.",
                        TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - startedAt));
        } finally {
            shutdownCompleted.countDown();
        }
    }

    private void awaitShutdownCompletion() {
        var interrupted = false;
        while (true) {
            try {
                shutdownCompleted.await();
                break;
            } catch (InterruptedException e) {
                interrupted = true;
            }
        }
        if (interrupted)
            Thread.currentThread().interrupt();
    }

    private static void closeServerChannel(Channel channel, ShutdownDeadline deadline) {
        if (channel == null)
            return;
        var closeFuture = channel.close();
        if (!closeFuture.awaitUninterruptibly(deadline.remainingNanos(), TimeUnit.NANOSECONDS))
            logger.warn("Timed out waiting for the server channel to close.");
    }

    private static void shutdownEventLoops(List<EventLoopGroup> groups, ShutdownDeadline deadline) {
        var terminationFutures = groups.stream()
                .map(group -> group.shutdownGracefully(
                        0, Math.max(1, deadline.remainingNanos()), TimeUnit.NANOSECONDS))
                .toList();
        for (var future : terminationFutures) {
            if (!future.awaitUninterruptibly(deadline.remainingNanos(), TimeUnit.NANOSECONDS))
                logger.warn("Timed out waiting for a Netty event loop group to terminate.");
        }
    }

    @FunctionalInterface
    interface ShutdownStep {
        void shutdown(ShutdownDeadline deadline);
    }
}
