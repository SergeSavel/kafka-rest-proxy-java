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

import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandlerAdapter;
import io.netty.channel.embedded.EmbeddedChannel;
import org.apache.kafka.common.errors.*;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import pro.savel.kafka.common.exceptions.*;

import static org.junit.jupiter.api.Assertions.*;

class CommonErrorsTest {

    private EmbeddedChannel channel;
    private ChannelHandlerContext ctx;

    @BeforeEach
    void setUp() {
        channel = new EmbeddedChannel(new ChannelInboundHandlerAdapter());
        ctx = channel.pipeline().firstContext();
    }

    @AfterEach
    void tearDown() {
        channel.close();
    }

//region HttpStatusException subclasses

    @Test
    void handle_badRequest_returns400() {
        assertTrue(CommonErrors.handle(ctx, new BadRequestException("bad")));
    }

    @Test
    void handle_notFound_returns404() {
        assertTrue(CommonErrors.handle(ctx, new NotFoundException("nf")));
    }

    @Test
    void handle_methodNotAllowed_returns405() {
        assertTrue(CommonErrors.handle(ctx, new MethodNotAllowedException("mna")));
    }

    @Test
    void handle_unauthenticated_returns401() {
        assertTrue(CommonErrors.handle(ctx, new UnauthenticatedException("ua")));
    }

    @Test
    void handle_unauthorized_returns403() {
        assertTrue(CommonErrors.handle(ctx, new UnauthorizedException("unauth")));
    }

    @Test
    void handle_conflict_returns409() {
        assertTrue(CommonErrors.handle(ctx, new ConflictException("c")));
    }

//endregion

//region Kafka exceptions

    @Test
    void handle_illegalArgumentException_returnsTrue() {
        assertTrue(CommonErrors.handle(ctx, new IllegalArgumentException("arg")));
    }

    @Test
    void handle_illegalStateException_returnsTrue() {
        assertTrue(CommonErrors.handle(ctx, new IllegalStateException("state")));
    }

    @Test
    void handle_authorizationException_returnsTrue() {
        assertTrue(CommonErrors.handle(ctx, new AuthorizationException("auth") {}));
    }

    @Test
    void handle_authenticationException_returnsTrue() {
        assertTrue(CommonErrors.handle(ctx, new AuthenticationException("auth") {}));
    }

    @Test
    void handle_invalidRequestException_returnsTrue() {
        assertTrue(CommonErrors.handle(ctx, new InvalidRequestException("inv") {}));
    }

    @Test
    void handle_timeoutException_returnsTrue() {
        assertTrue(CommonErrors.handle(ctx, new TimeoutException("to") {}));
    }

    @Test
    void handle_topicExistsException_returnsTrue() {
        assertTrue(CommonErrors.handle(ctx, new TopicExistsException("te") {}));
    }

//endregion

//region CompletionException / TimeoutException unwrapping

    @Test
    void handle_completionException_notHandledDirectly() {
        // CompletionException is unwrapped by processor handleError(), not by CommonErrors.handle()
        var wrapped = new java.util.concurrent.CompletionException(new IllegalArgumentException("inner"));
        assertFalse(CommonErrors.handle(ctx, wrapped));
    }

    @Test
    void handle_kafkaTimeoutException_unwrapsCause() {
        var cause = new AuthorizationException("auth") {};
        var wrapped = new org.apache.kafka.common.errors.TimeoutException("to", cause);
        assertTrue(CommonErrors.handle(ctx, wrapped));
    }

//endregion

//region InterruptedException

    @Test
    void handle_interruptedException_restoresInterruptFlag() {
        Thread.interrupted(); // clear

        CommonErrors.handle(ctx, new InterruptedException("interrupted"));

        assertTrue(Thread.currentThread().isInterrupted(), "Interrupt flag should be restored");
        Thread.interrupted(); // clear for test framework
    }

//endregion

//region Unhandled

    @Test
    void handle_null_returnsFalse() {
        assertFalse(CommonErrors.handle(ctx, null));
    }

    @Test
    void handle_unknownException_returnsFalse() {
        assertFalse(CommonErrors.handle(ctx, new RuntimeException("unknown")));
    }

//endregion
}
