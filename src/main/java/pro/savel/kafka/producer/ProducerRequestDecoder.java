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

package pro.savel.kafka.producer;

import com.fasterxml.jackson.databind.ObjectMapper;
import io.netty.channel.ChannelHandler;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandlerAdapter;
import io.netty.handler.codec.http.FullHttpRequest;
import io.netty.handler.codec.http.HttpMethod;
import io.netty.handler.codec.http.QueryStringDecoder;
import io.netty.util.ReferenceCountUtil;
import jakarta.validation.Validator;
import jakarta.validation.ValidatorFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import pro.savel.kafka.common.HttpUtils;
import pro.savel.kafka.common.JsonUtils;
import pro.savel.kafka.common.RequestBearer;
import pro.savel.kafka.common.Utils;
import pro.savel.kafka.common.exceptions.BadRequestException;
import pro.savel.kafka.common.exceptions.MethodNotAllowedException;
import pro.savel.kafka.producer.requests.*;

import java.nio.charset.StandardCharsets;
import java.util.Map;

@ChannelHandler.Sharable
public class ProducerRequestDecoder extends ChannelInboundHandlerAdapter {

    private static final Logger logger = LoggerFactory.getLogger(ProducerRequestDecoder.class);

    public static final String URI_PREFIX = "/producer";

    private static final Map<String, Class<? extends ProducerRequest>> REQUEST_TYPES = Map.of(
            "/create", ProducerCreateRequest.class,
            "/release", ProducerRemoveRequest.class,
            "/touch", ProducerTouchRequest.class,
            "/get-partitions", ProducerGetPartitionsRequest.class,
            "/begin-transaction", ProducerBeginTransactionRequest.class,
            "/commit-transaction", ProducerCommitTransactionRequest.class,
            "/abort-transaction", ProducerAbortTransactionRequest.class
    );

    private final ObjectMapper objectMapper;
    private final Validator validator;

    public ProducerRequestDecoder(ObjectMapper objectMapper, ValidatorFactory validatorFactory) {
        this.objectMapper = objectMapper;
        this.validator = validatorFactory == null ? null : validatorFactory.getValidator();
    }

    @Override
    public void channelRead(ChannelHandlerContext ctx, Object msg) {
        if (msg instanceof FullHttpRequest httpRequest && httpRequest.uri().startsWith(URI_PREFIX)) {
            try {
                decode(ctx, httpRequest);
            } catch (BadRequestException e) {
                HttpUtils.writeBadRequestAndClose(ctx, Utils.combineErrorMessage(e));
            } catch (MethodNotAllowedException e) {
                HttpUtils.writeMethodNotAllowedAndClose(ctx, Utils.combineErrorMessage(e));
            } catch (Exception e) {
                logger.error("An unexpected error occurred while decoding producer request.", e);
                HttpUtils.writeInternalServerErrorAndClose(ctx, Utils.combineErrorMessage(e));
            } finally {
                ReferenceCountUtil.release(msg);
            }
        } else {
            ctx.fireChannelRead(msg);
        }
    }

    private static void passBearer(ChannelHandlerContext ctx, FullHttpRequest httpRequest, ProducerRequest request) {
        var bearer = new RequestBearer(httpRequest, request);
        ctx.fireChannelRead(bearer);
    }

    private void decode(ChannelHandlerContext ctx, FullHttpRequest httpRequest) throws BadRequestException, MethodNotAllowedException {
        var decoder = new QueryStringDecoder(httpRequest.uri(), StandardCharsets.UTF_8, true);
        var pathMethod = decoder.path().substring(URI_PREFIX.length());
        if (pathMethod.isEmpty()) {
            requireMethod(httpRequest, HttpMethod.GET);
            decodeListRequest(ctx, httpRequest);
        } else if ("/send".equals(pathMethod)) {
            requireMethod(httpRequest, HttpMethod.POST);
            decodeSendRequest(ctx, httpRequest);
        } else {
            var requestType = REQUEST_TYPES.get(pathMethod);
            if (requestType == null) {
                HttpUtils.writeNotFoundAndClose(ctx);
                return;
            }
            requireMethod(httpRequest, HttpMethod.POST);
            decodeRequest(ctx, httpRequest, requestType);
        }
    }

    private static void requireMethod(FullHttpRequest httpRequest, HttpMethod method) throws MethodNotAllowedException {
        if (httpRequest.method() != method)
            throw new MethodNotAllowedException("Unsupported HTTP method.");
    }

    private void decodeListRequest(ChannelHandlerContext ctx, FullHttpRequest httpRequest) {
        var request = new ProducerListRequest();
        var bearer = new RequestBearer(httpRequest, request);
        ctx.fireChannelRead(bearer);
    }

    private void decodeSendRequest(ChannelHandlerContext ctx, FullHttpRequest httpRequest) throws BadRequestException {
        var contentType = HttpUtils.getContentType(httpRequest);
        ProducerSendRequest request;
        if (HttpUtils.isJson(contentType)) {
            var stringRequest = JsonUtils.parseJson(objectMapper, httpRequest.content(), ProducerSendStringRequest.class);
            request = ProducerRequestMapper.mapProduceRequest(stringRequest);
        } else if (HttpUtils.isOctetStream(contentType)) {
            request = ProducerRequestDeserializer.deserializeBinarySend(httpRequest.content());
        } else
            throw new BadRequestException("Invalid Content-Type header in request.");
        if (validator != null) {
            var violations = validator.validate(request);
            if (!violations.isEmpty()) {
                HttpUtils.writeBadRequestAndClose(ctx, Utils.combineConstraintViolationMessage(violations));
                return;
            }
        }
        passBearer(ctx, httpRequest, request);
    }

    private <T extends ProducerRequest> void decodeRequest(ChannelHandlerContext ctx, FullHttpRequest httpRequest, Class<T> clazz) throws BadRequestException {
        var contentType = HttpUtils.getContentType(httpRequest);
        T request;
        if (HttpUtils.isJson(contentType))
            request = JsonUtils.parseJson(objectMapper, httpRequest.content(), clazz);
        else
            throw new BadRequestException("Invalid Content-Type header in request.");
        if (validator != null) {
            var violations = validator.validate(request);
            if (!violations.isEmpty()) {
                HttpUtils.writeBadRequestAndClose(ctx, Utils.combineConstraintViolationMessage(violations));
                return;
            }
        }
        passBearer(ctx, httpRequest, request);
    }
}
