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

import io.netty.channel.ChannelHandler;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandlerAdapter;
import io.netty.handler.codec.http.FullHttpRequest;
import io.netty.handler.codec.http.HttpMethod;
import io.netty.handler.codec.http.QueryStringDecoder;
import io.netty.util.ReferenceCountUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import pro.savel.kafka.common.HttpUtils;
import pro.savel.kafka.common.Utils;
import pro.savel.kafka.common.exceptions.BadRequestException;

import java.nio.charset.StandardCharsets;

@ChannelHandler.Sharable
public class VersionRequestDecoder extends ChannelInboundHandlerAdapter {

    public static final String URI_PREFIX = "/version";
    private static final Logger logger = LoggerFactory.getLogger(VersionRequestDecoder.class);

    @Override
    public void channelRead(ChannelHandlerContext ctx, Object msg) {
        if (msg instanceof FullHttpRequest httpRequest && httpRequest.uri().startsWith(URI_PREFIX)) {
            try {
                decode(ctx, httpRequest);
            } catch (BadRequestException e) {
                HttpUtils.writeBadRequestAndClose(ctx, Utils.combineErrorMessage(e));
            } catch (Exception e) {
                logger.error("An unexpected error occurred while decoding version request.", e);
                HttpUtils.writeInternalServerErrorAndClose(ctx, Utils.combineErrorMessage(e));
            } finally {
                ReferenceCountUtil.release(msg);
            }
        } else {
            ctx.fireChannelRead(msg);
        }
    }

    private void decode(ChannelHandlerContext ctx, FullHttpRequest httpRequest) throws BadRequestException {
        var decoder = new QueryStringDecoder(httpRequest.uri(), StandardCharsets.UTF_8, true);
        var pathMethod = decoder.path().substring(URI_PREFIX.length());
        if (pathMethod.isEmpty()) {
            decodeRoot(ctx, httpRequest);
        } else {
            HttpUtils.writeNotFoundAndClose(ctx);
        }
    }

    private void decodeRoot(ChannelHandlerContext ctx, FullHttpRequest httpRequest) throws BadRequestException {
        if (httpRequest.method() == HttpMethod.GET) {
            var version = VersionRequestDecoder.class.getPackage().getImplementationVersion();
            HttpUtils.writeOkAndClose(ctx, version != null ? version : "unknown");
        } else {
            throw new BadRequestException("Unsupported HTTP method.");
        }
    }
}
