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

package pro.savel.kafka;

import com.fasterxml.jackson.databind.ObjectMapper;
import io.netty.channel.ChannelInboundHandlerAdapter;
import io.netty.channel.embedded.EmbeddedChannel;
import io.netty.handler.codec.http.*;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Base64;
import java.util.Locale;

import static org.junit.jupiter.api.Assertions.*;

class BasicAuthenticationHandlerTest {

    private final ObjectMapper objectMapper = new ObjectMapper();

    private static String basicAuth(String user, String password) {
        return "Basic " + Base64.getEncoder().encodeToString((user + ":" + password).getBytes(StandardCharsets.UTF_8));
    }

    private static FullHttpRequest getRequest() {
        return new DefaultFullHttpRequest(HttpVersion.HTTP_1_1, HttpMethod.GET, "/test");
    }

//region initialize

    @Test
    void initialize_missingFile_disablesAuth() {
        var handler = new BasicAuthenticationHandler(objectMapper);
        handler.initialize("/nonexistent/users.json");

        // Auth disabled — request should pass through
        var channel = new EmbeddedChannel(handler, new ChannelInboundHandlerAdapter());
        var request = getRequest();
        assertTrue(channel.writeInbound(request));
        channel.close();
    }

    @Test
    void initialize_validFile_loadsUsers(@TempDir Path tempDir) throws IOException {
        var usersFile = tempDir.resolve("users.json");
        Files.writeString(usersFile, """
                [{"username":"admin","password":"secret"}]
                """);

        var handler = new BasicAuthenticationHandler(objectMapper);
        handler.initialize(usersFile.toString());

        // Auth enabled — request without credentials should fail
        var channel = new EmbeddedChannel(handler, new ChannelInboundHandlerAdapter());
        var request = getRequest();
        assertFalse(channel.writeInbound(request));
        var response = channel.readOutbound();
        assertNotNull(response); // 401 response
        channel.close();
    }

    @Test
    void initialize_malformedFile_throwsException(@TempDir Path tempDir) throws IOException {
        var usersFile = tempDir.resolve("users.json");
        Files.writeString(usersFile, "not json");

        var handler = new BasicAuthenticationHandler(objectMapper);
        assertThrows(RuntimeException.class, () -> handler.initialize(usersFile.toString()));
    }

//endregion

//region authenticate

    @Test
    void authenticate_missingAuthHeader_returns401(@TempDir Path tempDir) throws IOException {
        var usersFile = tempDir.resolve("users.json");
        Files.writeString(usersFile, """
                [{"username":"admin","password":"secret"}]
                """);

        var handler = new BasicAuthenticationHandler(objectMapper);
        handler.initialize(usersFile.toString());

        var channel = new EmbeddedChannel(handler, new ChannelInboundHandlerAdapter());
        var request = getRequest();
        assertFalse(channel.writeInbound(request));
        channel.close();
    }

    @Test
    void authenticate_invalidPrefix_returns401(@TempDir Path tempDir) throws IOException {
        var usersFile = tempDir.resolve("users.json");
        Files.writeString(usersFile, """
                [{"username":"admin","password":"secret"}]
                """);

        var handler = new BasicAuthenticationHandler(objectMapper);
        handler.initialize(usersFile.toString());

        var channel = new EmbeddedChannel(handler, new ChannelInboundHandlerAdapter());
        var request = getRequest();
        request.headers().set(HttpHeaderNames.AUTHORIZATION, "Bearer token123");
        assertFalse(channel.writeInbound(request));
        channel.close();
    }

    @Test
    void authenticate_malformedBase64_returns401(@TempDir Path tempDir) throws IOException {
        var usersFile = tempDir.resolve("users.json");
        Files.writeString(usersFile, """
                [{"username":"admin","password":"secret"}]
                """);

        var handler = new BasicAuthenticationHandler(objectMapper);
        handler.initialize(usersFile.toString());

        var channel = new EmbeddedChannel(handler, new ChannelInboundHandlerAdapter());
        var request = getRequest();
        request.headers().set(HttpHeaderNames.AUTHORIZATION, "Basic !!!not-base64!!!");
        assertFalse(channel.writeInbound(request));
        channel.close();
    }

    @Test
    void authenticate_wrongPassword_returns401(@TempDir Path tempDir) throws IOException {
        var usersFile = tempDir.resolve("users.json");
        Files.writeString(usersFile, """
                [{"username":"admin","password":"secret"}]
                """);

        var handler = new BasicAuthenticationHandler(objectMapper);
        handler.initialize(usersFile.toString());

        var channel = new EmbeddedChannel(handler, new ChannelInboundHandlerAdapter());
        var request = getRequest();
        request.headers().set(HttpHeaderNames.AUTHORIZATION, basicAuth("admin", "wrong"));
        assertFalse(channel.writeInbound(request));
        channel.close();
    }

    @Test
    void authenticate_wrongUsername_returns401(@TempDir Path tempDir) throws IOException {
        var usersFile = tempDir.resolve("users.json");
        Files.writeString(usersFile, """
                [{"username":"admin","password":"secret"}]
                """);

        var handler = new BasicAuthenticationHandler(objectMapper);
        handler.initialize(usersFile.toString());

        var channel = new EmbeddedChannel(handler, new ChannelInboundHandlerAdapter());
        var request = getRequest();
        request.headers().set(HttpHeaderNames.AUTHORIZATION, basicAuth("unknown", "secret"));
        assertFalse(channel.writeInbound(request));
        channel.close();
    }

    @Test
    void authenticate_validCredentials_passesThrough(@TempDir Path tempDir) throws IOException {
        var usersFile = tempDir.resolve("users.json");
        Files.writeString(usersFile, """
                [{"username":"admin","password":"secret"}]
                """);

        var handler = new BasicAuthenticationHandler(objectMapper);
        handler.initialize(usersFile.toString());

        var channel = new EmbeddedChannel(handler, new ChannelInboundHandlerAdapter());
        var request = getRequest();
        request.headers().set(HttpHeaderNames.AUTHORIZATION, basicAuth("admin", "secret"));
        assertTrue(channel.writeInbound(request));
        channel.close();
    }

    @Test
    void authenticate_caseInsensitiveUsername_passesThrough(@TempDir Path tempDir) throws IOException {
        var usersFile = tempDir.resolve("users.json");
        Files.writeString(usersFile, """
                [{"username":"Admin","password":"secret"}]
                """);

        var handler = new BasicAuthenticationHandler(objectMapper);
        handler.initialize(usersFile.toString());

        var channel = new EmbeddedChannel(handler, new ChannelInboundHandlerAdapter());
        var request = getRequest();
        request.headers().set(HttpHeaderNames.AUTHORIZATION, basicAuth("ADMIN", "secret"));
        assertTrue(channel.writeInbound(request));
        channel.close();
    }

    @Test
    void authenticate_caseInsensitiveUsername_isLocaleIndependent(@TempDir Path tempDir) throws IOException {
        // Turkish uppercases 'i' to dotted 'İ', so "Admin" and "ADMIN" would fold to different keys
        // and a valid credential would be rejected on a host with that default locale.
        var previousDefault = Locale.getDefault();
        Locale.setDefault(Locale.forLanguageTag("tr-TR"));
        try {
            var usersFile = tempDir.resolve("users.json");
            Files.writeString(usersFile, """
                    [{"username":"Admin","password":"secret"}]
                    """);

            var handler = new BasicAuthenticationHandler(objectMapper);
            handler.initialize(usersFile.toString());

            var channel = new EmbeddedChannel(handler, new ChannelInboundHandlerAdapter());
            var request = getRequest();
            request.headers().set(HttpHeaderNames.AUTHORIZATION, basicAuth("ADMIN", "secret"));
            assertTrue(channel.writeInbound(request));
            channel.close();
        } finally {
            Locale.setDefault(previousDefault);
        }
    }

    @Test
    void authenticate_lowercaseScheme_passesThrough(@TempDir Path tempDir) throws IOException {
        var usersFile = tempDir.resolve("users.json");
        Files.writeString(usersFile, """
                [{"username":"admin","password":"secret"}]
                """);

        var handler = new BasicAuthenticationHandler(objectMapper);
        handler.initialize(usersFile.toString());

        var channel = new EmbeddedChannel(handler, new ChannelInboundHandlerAdapter());
        var request = getRequest();
        var encoded = Base64.getEncoder().encodeToString("admin:secret".getBytes(StandardCharsets.UTF_8));
        request.headers().set(HttpHeaderNames.AUTHORIZATION, "basic " + encoded);
        assertTrue(channel.writeInbound(request));
        channel.close();
    }

    @Test
    void authenticate_noColonInCredentials_returns401(@TempDir Path tempDir) throws IOException {
        var usersFile = tempDir.resolve("users.json");
        Files.writeString(usersFile, """
                [{"username":"admin","password":"secret"}]
                """);

        var handler = new BasicAuthenticationHandler(objectMapper);
        handler.initialize(usersFile.toString());

        var channel = new EmbeddedChannel(handler, new ChannelInboundHandlerAdapter());
        var request = getRequest();
        var encoded = Base64.getEncoder().encodeToString("noseparator".getBytes(StandardCharsets.UTF_8));
        request.headers().set(HttpHeaderNames.AUTHORIZATION, "Basic " + encoded);
        assertFalse(channel.writeInbound(request));
        channel.close();
    }

//endregion
}
