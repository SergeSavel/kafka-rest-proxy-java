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

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.netty.channel.ChannelHandler;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandlerAdapter;
import io.netty.handler.codec.http.FullHttpRequest;
import io.netty.handler.codec.http.HttpHeaderNames;
import io.netty.util.ReferenceCountUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import pro.savel.kafka.common.HttpUtils;
import pro.savel.kafka.common.NettyAttributes;
import pro.savel.kafka.common.Utils;
import pro.savel.kafka.common.exceptions.UnauthenticatedException;

import java.io.File;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.Base64;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

@ChannelHandler.Sharable
public class BasicAuthenticationHandler extends ChannelInboundHandlerAdapter {

    private static final Logger logger = LoggerFactory.getLogger(BasicAuthenticationHandler.class);

    private static final String DEFAULT_FILE_NAME = "users.json";

    private final ObjectMapper objectMapper;

    private Map<String, UserWithHash> users;

    public BasicAuthenticationHandler(ObjectMapper objectMapper) {
        this.objectMapper = objectMapper;
    }

    public void initialize() {
        initialize(DEFAULT_FILE_NAME);
    }

    public void initialize(String usersFileName) {

        var file = new File(usersFileName);

        if (!file.exists()) {
            logger.warn("Users file '{}' not found. Authentication is disabled.", usersFileName);
            users = null;
            return;
        }

        List<UserWithPassword> userList;
        try {
            userList = objectMapper.readValue(file, new TypeReference<>() {
            });
        } catch (IOException e) {
            throw new RuntimeException("Unable to read from users file.", e);
        }

        var users_ = new HashMap<String, UserWithHash>();
        userList.forEach(user -> users_.put(user.username().toUpperCase(),
                new UserWithHash(user.username(), hash(user.password()))));
        users = users_;

        logger.info("Users file loaded.");
    }

    @Override
    public void channelRead(ChannelHandlerContext ctx, Object msg) {
        if (msg instanceof FullHttpRequest httpRequest) {
            try {
                authenticate(ctx, httpRequest);
            } catch (UnauthenticatedException e) {
                HttpUtils.writeUnauthorizedAndClose(ctx, httpRequest.protocolVersion(), Utils.combineErrorMessage(e));
            } catch (Exception e) {
                logger.error("An unexpected error occurred while authenticating user.", e);
                HttpUtils.writeInternalServerErrorAndClose(ctx, httpRequest.protocolVersion(), Utils.combineErrorMessage(e));
            } finally {
                ReferenceCountUtil.release(msg);
            }
        } else {
            ctx.fireChannelRead(msg);
        }
    }

    private void authenticate(ChannelHandlerContext ctx, FullHttpRequest request) {

        var users_ = users;

        if (users_ != null) {
            String authHeader = request.headers().get(HttpHeaderNames.AUTHORIZATION);

            if (authHeader == null)
                throw new UnauthenticatedException("Missing Authorization header.");

            if (!authHeader.startsWith("Basic "))
                throw new UnauthenticatedException("Invalid Authorization header.");

            var base64Credentials = authHeader.substring(6);

            byte[] decoded;
            try {
                decoded = Base64.getDecoder().decode(base64Credentials);
            } catch (IllegalArgumentException e) {
                throw new UnauthenticatedException("Invalid Authorization header.");
            }

            var credentialsString = new String(decoded, StandardCharsets.UTF_8);
            var credentialsArray = credentialsString.split(":", 2);

            if (credentialsArray.length != 2)
                throw new UnauthenticatedException("Invalid Authorization header.");

            var user = users_.get(credentialsArray[0].toUpperCase());

            if (user == null)
                throw new UnauthenticatedException("Invalid username or password.");

            if (!MessageDigest.isEqual(hash(credentialsArray[1]), user.passwordHash()))
                throw new UnauthenticatedException("Invalid username or password.");

            ctx.channel().attr(NettyAttributes.USERNAME).set(user.username);
        }

        ctx.fireChannelRead(request.retain());
    }

    private static byte[] hash(String value) {
        try {
            return MessageDigest.getInstance("SHA-256").digest(value.getBytes(StandardCharsets.UTF_8));
        } catch (NoSuchAlgorithmException e) {
            throw new RuntimeException(e);
        }
    }

    private record UserWithPassword(String username, String password) {
    }

    private record UserWithHash(String username, byte[] passwordHash) {
    }
}
