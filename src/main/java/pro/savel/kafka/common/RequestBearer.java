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

import io.netty.handler.codec.http.HttpHeaderNames;
import io.netty.handler.codec.http.HttpRequest;
import io.netty.handler.codec.http.HttpUtil;
import io.netty.handler.codec.http.HttpVersion;
import pro.savel.kafka.common.contract.Request;
import pro.savel.kafka.common.contract.Serde;

public record RequestBearer(Request request, Serde serializeTo, HttpVersion protocolVersion,
                            boolean connectionKeepAlive) {

    public RequestBearer(HttpRequest httpRequest, Request request) {
        this(request, getSerde(httpRequest), httpRequest.protocolVersion(), HttpUtil.isKeepAlive(httpRequest));
    }

    private static Serde getSerde(HttpRequest httpRequest) {
        Serde serializeTo;
        var headers = httpRequest.headers();
        var accept = headers.get(HttpHeaderNames.ACCEPT);
        var contentType = headers.get(HttpHeaderNames.CONTENT_TYPE);
        if (accept == null || "*/*".equals(accept)) {
            accept = contentType;
        }
        if ("application/json".equals(accept) || "application/json; charset=utf-8".equals(accept)) {
            serializeTo = Serde.JSON;
        } else if ("application/octet-stream".equals(accept)) {
            serializeTo = Serde.BINARY;
        } else {
            // default
            serializeTo = Serde.JSON;
        }
        return serializeTo;
    }

}
