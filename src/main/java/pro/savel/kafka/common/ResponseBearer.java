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

import io.netty.handler.codec.http.HttpResponseStatus;
import io.netty.handler.codec.http.HttpVersion;
import lombok.Data;
import pro.savel.kafka.common.contract.Response;
import pro.savel.kafka.common.contract.Serde;

@Data
public abstract class ResponseBearer<TResponse extends Response> {

    private final TResponse response;
    private final HttpResponseStatus status;
    private final Serde serializeTo;
    private final HttpVersion protocolVersion;
    private final boolean connectionKeepAlive;

    public ResponseBearer(RequestBearer requestBearer, HttpResponseStatus status, TResponse response) {
        this.response = response;
        this.status = status;
        this.serializeTo = requestBearer.serializeTo();
        this.protocolVersion = requestBearer.protocolVersion();
        this.connectionKeepAlive = requestBearer.connectionKeepAlive();
    }
}
