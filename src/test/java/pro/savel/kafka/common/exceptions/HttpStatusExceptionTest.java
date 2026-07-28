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

package pro.savel.kafka.common.exceptions;

import io.netty.handler.codec.http.HttpResponseStatus;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

class HttpStatusExceptionTest {

    @Test
    void badRequest_returns400() {
        var ex = new BadRequestException("bad");
        assertEquals(HttpResponseStatus.BAD_REQUEST, ex.status());
        assertEquals("bad", ex.getMessage());
    }

    @Test
    void notFound_returns404() {
        var ex = new NotFoundException("not found");
        assertEquals(HttpResponseStatus.NOT_FOUND, ex.status());
    }

    @Test
    void methodNotAllowed_returns405() {
        var ex = new MethodNotAllowedException("method");
        assertEquals(HttpResponseStatus.METHOD_NOT_ALLOWED, ex.status());
    }

    @Test
    void unauthenticated_returns401() {
        var ex = new UnauthenticatedException("unauth");
        assertEquals(HttpResponseStatus.UNAUTHORIZED, ex.status());
    }

    @Test
    void unauthorized_returns403() {
        var ex = new UnauthorizedException("forbidden");
        assertEquals(HttpResponseStatus.FORBIDDEN, ex.status());
    }

    @Test
    void conflict_returns409() {
        var ex = new ConflictException("conflict");
        assertEquals(HttpResponseStatus.CONFLICT, ex.status());
    }

    @Test
    void exceptionWithCause_preservesCause() {
        var cause = new RuntimeException("root");
        var ex = new BadRequestException("bad", cause);
        assertEquals(cause, ex.getCause());
        assertEquals(HttpResponseStatus.BAD_REQUEST, ex.status());
    }
}
