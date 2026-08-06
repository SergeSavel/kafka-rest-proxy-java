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
import io.netty.channel.ChannelOption;
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

import java.time.Duration;

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
        ServerLifecycle lifecycle = null;
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

            lifecycle = new ServerLifecycle(channel, initializer, bossGroup, workerGroup,
                    Duration.ofSeconds(config.shutdownTimeoutSeconds()));
            var lifecycleForHook = lifecycle;
            shutdownHook = new Thread(lifecycleForHook::close, "kafka-gateway-shutdown");
            Runtime.getRuntime().addShutdownHook(shutdownHook);
            channel.closeFuture().sync();
        }
        finally
        {
            if (lifecycle == null)
                lifecycle = new ServerLifecycle(null, initializer, bossGroup, workerGroup,
                        Duration.ofSeconds(config.shutdownTimeoutSeconds()));
            lifecycle.close();
            if (shutdownHook != null) {
                try {
                    Runtime.getRuntime().removeShutdownHook(shutdownHook);
                } catch (IllegalStateException ignored) {
                    // JVM shutdown is already in progress.
                }
            }
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
