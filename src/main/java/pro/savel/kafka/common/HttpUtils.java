// Copyright 2025 Sergey Savelev
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
import io.netty.handler.codec.http.DefaultFullHttpResponse;
import io.netty.handler.codec.http.FullHttpRequest;
import io.netty.handler.codec.http.HttpResponseStatus;
import io.netty.handler.codec.http.HttpVersion;
import io.netty.util.AsciiString;
import pro.savel.kafka.common.exceptions.BadRequestException;

import java.nio.charset.StandardCharsets;

public abstract class HttpUtils {

    public static final String APPLICATION_JSON = "application/json";
    public static final String APPLICATION_JSON_CHARSET_UTF8 = "application/json; charset=utf-8";
    public static final String TEXT_PLAIN_CHARSET_UTF8 = "text/plain; charset=utf-8";
    public static final String APPLICATION_OCTET_STREAM = "application/octet-stream";

    public static final AsciiString ASCII_CONTENT_TYPE = AsciiString.cached("Content-Type");
    public static final AsciiString ASCII_CONTENT_LENGTH = AsciiString.cached("Content-Length");
    public static final AsciiString ASCII_CONNECTION = AsciiString.cached("Connection");
    public static final AsciiString ASCII_APPLICATION_JSON_CHARSET_UTF8 = AsciiString.cached(APPLICATION_JSON_CHARSET_UTF8);
    public static final AsciiString ASCII_TEXT_PLAIN_CHARSET_UTF8 = AsciiString.cached(TEXT_PLAIN_CHARSET_UTF8);

    public static String getContentType(FullHttpRequest httpRequest) throws BadRequestException {
        var contentType = httpRequest.headers().get(ASCII_CONTENT_TYPE);
        if (contentType == null)
            throw new BadRequestException("Missing Content-Type header in request.");
        return contentType;
    }

    public static boolean isJson(String contentType) {
        return APPLICATION_JSON.equals(contentType) || APPLICATION_JSON_CHARSET_UTF8.equals(contentType);
    }

    public static boolean isOctetStream(String contentType) {
        return APPLICATION_OCTET_STREAM.equals(contentType);
    }

    public static void writeBadRequestAndClose(ChannelHandlerContext ctx, HttpVersion version, String message) {
        writeHttpResponseAndClose(ctx, version, HttpResponseStatus.BAD_REQUEST, message);
    }

    public static void writeNotFoundAndClose(ChannelHandlerContext ctx, HttpVersion version) {
        writeNotFoundAndClose(ctx, version, null);
    }

    public static void writeNotFoundAndClose(ChannelHandlerContext ctx, HttpVersion version, String message) {
        writeHttpResponseAndClose(ctx, version, HttpResponseStatus.NOT_FOUND, message);
    }

    public static void writeMethodNotAllowedAndClose(ChannelHandlerContext ctx, HttpVersion version) {
        writeMethodNotAllowedAndClose(ctx, version, null);
    }

    public static void writeMethodNotAllowedAndClose(ChannelHandlerContext ctx, HttpVersion version, String message) {
        writeHttpResponseAndClose(ctx, version, HttpResponseStatus.METHOD_NOT_ALLOWED, message);
    }

    public static void writeUnauthorizedAndClose(ChannelHandlerContext ctx, HttpVersion version) {
        writeUnauthorizedAndClose(ctx, version, null);
    }

    public static void writeUnauthorizedAndClose(ChannelHandlerContext ctx, HttpVersion version, String message) {
        writeHttpResponseAndClose(ctx, version, HttpResponseStatus.UNAUTHORIZED, message);
    }

    public static void writeForbiddenAndClose(ChannelHandlerContext ctx, HttpVersion version) {
        writeForbiddenAndClose(ctx, version, null);
    }

    public static void writeForbiddenAndClose(ChannelHandlerContext ctx, HttpVersion version, String message) {
        writeHttpResponseAndClose(ctx, version, HttpResponseStatus.FORBIDDEN, message);
    }

    public static void writeInternalServerErrorAndClose(ChannelHandlerContext ctx, HttpVersion version) {
        writeInternalServerErrorAndClose(ctx, version, null);
    }

    public static void writeInternalServerErrorAndClose(ChannelHandlerContext ctx, HttpVersion version, String message) {
        writeHttpResponseAndClose(ctx, version, HttpResponseStatus.INTERNAL_SERVER_ERROR, message);
    }

    public static void writeHttpResponseAndClose(ChannelHandlerContext ctx, HttpVersion version, HttpResponseStatus status, String message) {
        var httpResponse = new DefaultFullHttpResponse(version, status);
        if (message != null) {
            httpResponse.headers().set(ASCII_CONTENT_TYPE, ASCII_TEXT_PLAIN_CHARSET_UTF8);
            httpResponse.content().writeCharSequence(message, StandardCharsets.UTF_8);
        }
        httpResponse.headers().setInt(ASCII_CONTENT_LENGTH, httpResponse.content().readableBytes());
        var future = ctx.writeAndFlush(httpResponse);
        future.addListener(ChannelFutureListener.CLOSE);
    }
}
