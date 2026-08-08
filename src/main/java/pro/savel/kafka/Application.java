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

import io.netty.bootstrap.ServerBootstrap;
import io.netty.channel.Channel;
import io.netty.channel.ChannelOption;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.IoHandlerFactory;
import io.netty.channel.MultiThreadIoEventLoopGroup;
import io.netty.channel.ServerChannel;
import io.netty.channel.epoll.Epoll;
import io.netty.channel.epoll.EpollIoHandler;
import io.netty.channel.epoll.EpollServerSocketChannel;
import io.netty.channel.nio.NioIoHandler;
import io.netty.channel.socket.nio.NioServerSocketChannel;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import pro.savel.kafka.common.ShutdownDeadline;

import java.time.Duration;
import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.FutureTask;
import java.util.concurrent.TimeUnit;

public class Application
{
    private static final Logger logger = LoggerFactory.getLogger(Application.class);

    public static void main(String[] args) throws InterruptedException
    {
        var config = ServerConfig.fromSystemProperties();
        var transport = selectTransport(config.epollEnabled());
        var bossGroup = new MultiThreadIoEventLoopGroup(1, transport.ioHandlerFactory());
        var workerGroup = new MultiThreadIoEventLoopGroup(config.workerThreads(), transport.ioHandlerFactory());
        var initializer = new ServerInitializer(config);
        FutureTask<Void> shutdownTask = null;
        Thread shutdownHook = null;

        try
        {
            initializer.initialize();

            var bootstrap = new ServerBootstrap();
            bootstrap.option(ChannelOption.SO_BACKLOG, config.backlog());
            bootstrap.childOption(ChannelOption.TCP_NODELAY, true);
            bootstrap.group(bossGroup, workerGroup)
                    .channel(transport.serverChannelClass())
                    .childHandler(initializer);

            var channel = bootstrap.bind(config.host(), config.port()).sync().channel();
            logger.info("Server started on {}:{} using {} transport and {} worker threads.",
                    config.host(), config.port(), transport.name(), workerGroup.executorCount());

            var shutdownTimeout = Duration.ofSeconds(config.shutdownTimeoutSeconds());
            shutdownTask = new FutureTask<>(() -> {
                shutdown(channel, initializer, bossGroup, workerGroup, shutdownTimeout);
                return null;
            });
            var taskForHook = shutdownTask;
            shutdownHook = new Thread(() -> runShutdown(taskForHook), "kafka-gateway-shutdown");
            Runtime.getRuntime().addShutdownHook(shutdownHook);
            channel.closeFuture().sync();
        }
        finally
        {
            if (shutdownTask == null)
                shutdown(null, initializer, bossGroup, workerGroup,
                        Duration.ofSeconds(config.shutdownTimeoutSeconds()));
            else
                runShutdown(shutdownTask);
            if (shutdownHook != null) {
                try {
                    Runtime.getRuntime().removeShutdownHook(shutdownHook);
                } catch (IllegalStateException ignored) {
                    // JVM shutdown is already in progress.
                }
            }
        }
    }

    private static void runShutdown(FutureTask<Void> shutdownTask) {
        shutdownTask.run();

        var interrupted = false;
        while (true) {
            try {
                shutdownTask.get();
                break;
            } catch (InterruptedException e) {
                interrupted = true;
            } catch (ExecutionException e) {
                logger.error("Server shutdown failed.", e.getCause());
                break;
            }
        }
        if (interrupted)
            Thread.currentThread().interrupt();
    }

    private static void shutdown(Channel serverChannel, ServerInitializer initializer,
                                 EventLoopGroup bossGroup, EventLoopGroup workerGroup, Duration timeout) {
        var startedAt = System.nanoTime();
        var deadline = ShutdownDeadline.after(timeout);
        logger.info("Server is shutting down...");

        runShutdownStep(() -> closeServerChannel(serverChannel, deadline));
        runShutdownStep(() -> initializer.close(deadline));
        runShutdownStep(() -> shutdownEventLoops(List.of(bossGroup, workerGroup), deadline));

        if (deadline.isExpired())
            logger.warn("Server shutdown exceeded its {} second deadline.", timeout.toSeconds());
        else
            logger.info("Shutdown completed in {} ms.",
                    TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - startedAt));
    }

    private static void runShutdownStep(Runnable step) {
        try {
            step.run();
        } catch (Throwable e) {
            logger.error("Shutdown step failed.", e);
        }
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

    private static Transport selectTransport(boolean epollEnabled) {
        if (epollEnabled && isLinux()) {
            if (Epoll.isAvailable())
                return new Transport(EpollIoHandler.newFactory(), EpollServerSocketChannel.class, "epoll");
            logger.warn("Epoll transport is unavailable; falling back to NIO.", Epoll.unavailabilityCause());
        }
        return new Transport(NioIoHandler.newFactory(), NioServerSocketChannel.class, "NIO");
    }

    private static boolean isLinux() {
        return System.getProperty("os.name", "").toLowerCase().contains("linux");
    }

    private record Transport(
            IoHandlerFactory ioHandlerFactory,
            Class<? extends ServerChannel> serverChannelClass,
            String name) {
    }
}
