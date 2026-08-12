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
import io.netty.util.concurrent.Future;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import pro.savel.kafka.common.ShutdownDeadline;

import java.time.Duration;
import java.util.Arrays;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.FutureTask;
import java.util.concurrent.TimeUnit;

public class Application
{
    private static final Logger logger = LoggerFactory.getLogger(Application.class);

    private static volatile Channel serverChannel;

    public static void main(String[] args) throws InterruptedException
    {
        var config = ServerConfig.fromSystemProperties();
        var transport = selectTransport(config.epollEnabled());
        var bossGroup = new MultiThreadIoEventLoopGroup(1, transport.ioHandlerFactory());
        var workerGroup = new MultiThreadIoEventLoopGroup(config.workerThreads(), transport.ioHandlerFactory());
        var initializer = new ServerInitializer(config);
        var shutdownTimeout = Duration.ofSeconds(config.shutdownTimeoutSeconds());
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
            serverChannel = channel;

            logger.info("Server started on {}:{} using {} transport and {} event loop threads.",
                    config.host(), config.port(), transport.name(), workerGroup.executorCount());

            shutdownTask = new FutureTask<>(() -> {
                shutdown(channel, initializer, bossGroup, workerGroup, shutdownTimeout);
                return null;
            });
            var task = shutdownTask;
            shutdownHook = new Thread(task, "kafka-gateway-shutdown");
            Runtime.getRuntime().addShutdownHook(shutdownHook);
            channel.closeFuture().sync();
        }
        finally
        {
            if (shutdownTask != null)
                awaitShutdown(shutdownTask);
            else
                shutdown(null, initializer, bossGroup, workerGroup, shutdownTimeout);
            if (shutdownHook != null)
                try
                {
                    Runtime.getRuntime().removeShutdownHook(shutdownHook);
                }
                catch (IllegalStateException ignored)
                {
                    // JVM shutdown is already in progress.
                }
        }
    }

    /**
     * Stops the running server. Called by Apache Procrun on service stop: closes the server channel and lets
     * {@link #main(String[])} complete the graceful shutdown.
     */
    public static void stop(String[] args)
    {
        var channel = serverChannel;
        if (channel != null)
            channel.close();
    }

    private static void awaitShutdown(FutureTask<Void> shutdownTask) {
        shutdownTask.run();
        try {
            shutdownTask.get();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        } catch (ExecutionException e) {
            logger.error("Server shutdown failed.", e.getCause());
        }
    }

    private static void shutdown(Channel serverChannel, ServerInitializer initializer,
                                 EventLoopGroup bossGroup, EventLoopGroup workerGroup, Duration timeout) {
        var startedAt = System.nanoTime();
        var deadline = ShutdownDeadline.after(timeout);
        logger.info("Server is shutting down...");

        runShutdownStep(() -> awaitClose(serverChannel == null ? null : serverChannel.close(),
                deadline, "the server channel to close"));
        runShutdownStep(() -> initializer.close(deadline));
        runShutdownStep(() -> shutdownEventLoops(deadline, bossGroup, workerGroup));

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

    private static void awaitClose(Future<?> future, ShutdownDeadline deadline, String what) {
        if (future != null && !future.awaitUninterruptibly(deadline.remainingNanos(), TimeUnit.NANOSECONDS))
            logger.warn("Timed out waiting for {}.", what);
    }

    private static void shutdownEventLoops(ShutdownDeadline deadline, EventLoopGroup... groups) {
        var terminationFutures = Arrays.stream(groups)
                .map(group -> group.shutdownGracefully(
                        0, Math.max(1, deadline.remainingNanos()), TimeUnit.NANOSECONDS))
                .toList();
        for (var terminationFuture : terminationFutures)
            awaitClose(terminationFuture, deadline, "a Netty event loop group to terminate");
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
