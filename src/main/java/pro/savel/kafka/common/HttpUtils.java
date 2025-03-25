package pro.savel.kafka.common;

import io.netty.channel.ChannelFutureListener;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.http.*;

import java.nio.charset.StandardCharsets;

public abstract class HttpUtils {

    public static final String APPLICATION_JSON = "application/json";
    public static final String APPLICATION_JSON_CHARSET_UTF8 = "application/json; charset=utf-8";
    public static final String APPLICATION_OCTET_STREAM = "application/octet-stream";

    public static boolean isJson(String contentType) {
        return APPLICATION_JSON.equals(contentType) || APPLICATION_JSON_CHARSET_UTF8.equals(contentType);
    }

    public static void writeBadRequest(ChannelHandlerContext ctx, HttpVersion version, String message) {
        writeHttpResponseAndClose(ctx, version, HttpResponseStatus.BAD_REQUEST, message);
    }

    public static void writeNotFound(ChannelHandlerContext ctx, HttpVersion version) {
        writeNotFound(ctx, version, null);
    }

    public static void writeNotFound(ChannelHandlerContext ctx, HttpVersion version, String message) {
        writeHttpResponseAndClose(ctx, version, HttpResponseStatus.NOT_FOUND, message);
    }

    public static void writeMethodNotAllowed(ChannelHandlerContext ctx, HttpVersion version) {
        writeMethodNotAllowed(ctx, version, null);
    }

    public static void writeMethodNotAllowed(ChannelHandlerContext ctx, HttpVersion version, String message) {
        writeHttpResponseAndClose(ctx, version, HttpResponseStatus.METHOD_NOT_ALLOWED, message);
    }

    public static void writeInternalServerError(ChannelHandlerContext ctx, HttpVersion version) {
        writeInternalServerError(ctx, version, null);
    }

    public static void writeInternalServerError(ChannelHandlerContext ctx, HttpVersion version, String message) {
        writeHttpResponseAndClose(ctx, version, HttpResponseStatus.INTERNAL_SERVER_ERROR, message);
    }

    public static void writeHttpResponseAndClose(ChannelHandlerContext ctx, HttpVersion version, HttpResponseStatus status, String message) {
        var httpResponse = new DefaultFullHttpResponse(version, status);
        if (message != null) {
            httpResponse.headers().set(HttpHeaderNames.CONTENT_TYPE, HttpHeaderValues.TEXT_PLAIN + "; charset=utf-8");
            httpResponse.content().writeCharSequence(message, StandardCharsets.UTF_8);
        }
        var future = ctx.writeAndFlush(httpResponse);
        future.addListener(ChannelFutureListener.CLOSE);
    }
}
