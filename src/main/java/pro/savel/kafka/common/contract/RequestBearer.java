package pro.savel.kafka.common.contract;

import io.netty.handler.codec.http.HttpHeaderNames;
import io.netty.handler.codec.http.HttpRequest;
import io.netty.handler.codec.http.HttpUtil;

public record RequestBearer(Request request, RequestBearer.SerializationType serializeTo, boolean connectionKeepAlive) {

    public RequestBearer(HttpRequest httpRequest, Request request) {
        this(request, getSerializationType(httpRequest), HttpUtil.isKeepAlive(httpRequest));
    }

    private static SerializationType getSerializationType(HttpRequest httpRequest) {
        SerializationType serializeTo;
        var headers = httpRequest.headers();
        var accept = headers.get(HttpHeaderNames.ACCEPT);
        var contentType = headers.get(HttpHeaderNames.CONTENT_TYPE);
        if (accept == null || "*/*".equals(accept)) {
            accept = contentType;
        }
        if ("application/json".equals(accept) || "application/json; charset=utf-8".equals(accept)) {
            serializeTo = SerializationType.JSON;
        } else if ("application/octet-stream".equals(accept)) {
            serializeTo = SerializationType.BINARY;
        } else {
            serializeTo = SerializationType.JSON;
        }
        return serializeTo;
    }

    public enum SerializationType {
        JSON,
        BINARY
    }
}
