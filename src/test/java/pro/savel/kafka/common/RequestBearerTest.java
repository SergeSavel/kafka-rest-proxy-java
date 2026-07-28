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

package pro.savel.kafka.common;

import io.netty.handler.codec.http.DefaultHttpRequest;
import io.netty.handler.codec.http.HttpHeaderNames;
import io.netty.handler.codec.http.HttpMethod;
import io.netty.handler.codec.http.HttpVersion;
import org.junit.jupiter.api.Test;
import pro.savel.kafka.common.contract.Serde;

import static org.junit.jupiter.api.Assertions.*;

class RequestBearerTest {

    private static DefaultHttpRequest request() {
        return new DefaultHttpRequest(HttpVersion.HTTP_1_1, HttpMethod.GET, "/test");
    }

    @Test
    void serde_noHeaders_defaultsToJson() {
        var httpRequest = request();
        var bearer = new RequestBearer(httpRequest, null);
        assertEquals(Serde.JSON, bearer.serializeTo());
    }

    @Test
    void serde_acceptJson_returnsJson() {
        var httpRequest = request();
        httpRequest.headers().set(HttpHeaderNames.ACCEPT, "application/json");
        var bearer = new RequestBearer(httpRequest, null);
        assertEquals(Serde.JSON, bearer.serializeTo());
    }

    @Test
    void serde_acceptBinary_returnsBinary() {
        var httpRequest = request();
        httpRequest.headers().set(HttpHeaderNames.ACCEPT, "application/octet-stream");
        var bearer = new RequestBearer(httpRequest, null);
        assertEquals(Serde.BINARY, bearer.serializeTo());
    }

    @Test
    void serde_acceptWildcard_fallsBackToContentType() {
        var httpRequest = request();
        httpRequest.headers().set(HttpHeaderNames.ACCEPT, "*/*");
        httpRequest.headers().set(HttpHeaderNames.CONTENT_TYPE, "application/octet-stream");
        var bearer = new RequestBearer(httpRequest, null);
        assertEquals(Serde.BINARY, bearer.serializeTo());
    }

    @Test
    void serde_acceptNull_fallsBackToContentType() {
        var httpRequest = request();
        httpRequest.headers().set(HttpHeaderNames.CONTENT_TYPE, "application/json");
        var bearer = new RequestBearer(httpRequest, null);
        assertEquals(Serde.JSON, bearer.serializeTo());
    }

    @Test
    void serde_unknownAccept_defaultsToJson() {
        var httpRequest = request();
        httpRequest.headers().set(HttpHeaderNames.ACCEPT, "text/html");
        var bearer = new RequestBearer(httpRequest, null);
        assertEquals(Serde.JSON, bearer.serializeTo());
    }

    @Test
    void serde_acceptJsonWithCharset_returnsJson() {
        var httpRequest = request();
        httpRequest.headers().set(HttpHeaderNames.ACCEPT, "application/json; charset=utf-8");
        var bearer = new RequestBearer(httpRequest, null);
        assertEquals(Serde.JSON, bearer.serializeTo());
    }

    @Test
    void keepAlive_http11_defaultsToTrue() {
        var httpRequest = request();
        var bearer = new RequestBearer(httpRequest, null);
        assertTrue(bearer.connectionKeepAlive());
    }
}
