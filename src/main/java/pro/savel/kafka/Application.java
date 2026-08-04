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
import io.netty.channel.MultiThreadIoEventLoopGroup;
import io.netty.channel.nio.NioIoHandler;
import io.netty.channel.socket.nio.NioServerSocketChannel;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.CountDownLatch;

public class Application
{
    private static final Logger logger = LoggerFactory.getLogger(Application.class);

    public static void main(String[] args) throws InterruptedException
    {
        final var host = System.getProperty("host", "0.0.0.0");
        final var port = Integer.parseInt(System.getProperty("port", "8086"));
        var bossGroup = new MultiThreadIoEventLoopGroup(NioIoHandler.newFactory());
        var workerGroup = new MultiThreadIoEventLoopGroup(NioIoHandler.newFactory());
        var shutdownLatch = new CountDownLatch(1);

        try (var initializer = new ServerInitializer())
        {
            initializer.initialize();

            var bootstrap = new ServerBootstrap();
            bootstrap.option(ChannelOption.SO_BACKLOG, 1024);
            bootstrap.childOption(ChannelOption.TCP_NODELAY, true);
            bootstrap.group(bossGroup, workerGroup)
                    .channel(NioServerSocketChannel.class)
                    .childHandler(initializer);

            var channel = bootstrap.bind(host, port).sync().channel();
            logger.info("Server started on {}:{}", host, port);

            Runtime.getRuntime().addShutdownHook(new Thread(() ->
            {
                logger.info("Server is shutting down...");
                bossGroup.shutdownGracefully();
                workerGroup.shutdownGracefully();
                bossGroup.terminationFuture().syncUninterruptibly();
                workerGroup.terminationFuture().syncUninterruptibly();
                logger.info("Shutdown completed.");
                shutdownLatch.countDown();
            }));

            channel.closeFuture().sync();
            shutdownLatch.await();
        }
        finally
        {
            bossGroup.shutdownGracefully();
            workerGroup.shutdownGracefully();
        }
    }
}