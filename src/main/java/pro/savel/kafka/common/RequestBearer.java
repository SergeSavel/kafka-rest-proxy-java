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

package pro.savel.kafka.common;

import io.netty.handler.codec.http.HttpHeaderNames;
import io.netty.handler.codec.http.HttpRequest;
import io.netty.handler.codec.http.HttpUtil;
import pro.savel.kafka.common.contract.Request;
import pro.savel.kafka.common.contract.Serde;

public record RequestBearer(Request request, Serde serializeTo, boolean connectionKeepAlive) {

    public RequestBearer(HttpRequest httpRequest, Request request) {
        this(request, getSerde(httpRequest), HttpUtil.isKeepAlive(httpRequest));
    }

    private static Serde getSerde(HttpRequest httpRequest) {
        var headers = httpRequest.headers();
        var accept = HttpUtils.mediaType(headers.get(HttpHeaderNames.ACCEPT));
        if (accept == null || "*/*".equals(accept)) {
            accept = HttpUtils.mediaType(headers.get(HttpHeaderNames.CONTENT_TYPE));
        }
        if (HttpUtils.APPLICATION_OCTET_STREAM.equals(accept)) {
            return Serde.BINARY;
        }
        // default
        return Serde.JSON;
    }

}
