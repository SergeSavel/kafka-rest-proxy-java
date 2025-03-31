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

import java.util.Map;
import java.util.UUID;

public abstract class ClientWrapper implements AutoCloseable {

    private final UUID id = UUID.randomUUID();
    private final String name;
    private final String username;
    private final int expirationTimeout;
    private final String token = UUID.randomUUID().toString();

    private long expiresAt;

    protected ClientWrapper(String name, Map<String, String> config, int expirationTimeout) {
        this.name = name;
        this.username = getUsernameFromConfig(config);
        this.expirationTimeout = expirationTimeout;
        touch();
    }

    private static String getUsernameFromConfig(Map<String, String> config) {
        return config.get("sasl.username");
    }

    public void touch() {
        expiresAt = System.currentTimeMillis() + expirationTimeout;
    }

    @Override
    public void close() {
    }

    public UUID id() {
        return id;
    }

    public String name() {
        return name;
    }

    public String username() {
        return username;
    }

    public int expirationTimeout() {
        return expirationTimeout;
    }

    public String token() {
        return token;
    }

    public long expiresAt() {
        return expiresAt;
    }
}
