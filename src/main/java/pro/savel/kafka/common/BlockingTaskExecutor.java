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

import io.netty.channel.ChannelFutureListener;
import io.netty.channel.ChannelHandlerContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.BiConsumer;

public class BlockingTaskExecutor implements AutoCloseable {

    private static final Logger logger = LoggerFactory.getLogger(BlockingTaskExecutor.class);

    private final ExecutorService executor = Executors.newVirtualThreadPerTaskExecutor();

    public <T> void execute(ChannelHandlerContext ctx, Callable<T> operation, BiConsumer<T, Throwable> completion) {
        var taskReference = new AtomicReference<Future<?>>();
        var closeFuture = ctx.channel().closeFuture();
        ChannelFutureListener closeListener = ignored -> {
            var task = taskReference.get();
            if (task != null)
                task.cancel(true);
        };
        closeFuture.addListener(closeListener);
        try {
            var task = executor.submit(() -> {
                T result = null;
                Throwable error = null;
                try {
                    result = operation.call();
                } catch (Throwable e) {
                    error = e;
                } finally {
                    closeFuture.removeListener(closeListener);
                }
                if (!ctx.channel().isActive())
                    return;
                var result_ = result;
                var error_ = error;
                try {
                    ctx.executor().execute(() -> completion.accept(result_, error_));
                } catch (RejectedExecutionException ignored) {
                    logger.debug("Unable to deliver task result because the event loop has stopped.");
                }
            });
            taskReference.set(task);
            if (!ctx.channel().isActive())
                task.cancel(true);
        } catch (RuntimeException e) {
            closeFuture.removeListener(closeListener);
            throw e;
        }
    }

    @Override
    public void close() {
        close(ShutdownDeadline.after(Duration.ofSeconds(40)));
    }

    public void close(ShutdownDeadline deadline) {
        executor.shutdownNow();
        try {
            if (!executor.awaitTermination(deadline.remainingNanos(), TimeUnit.NANOSECONDS))
                logger.warn("Failed to terminate blocking task executor before shutdown deadline.");
        } catch (InterruptedException e) {
            executor.shutdownNow();
            Thread.currentThread().interrupt();
        }
    }
}
