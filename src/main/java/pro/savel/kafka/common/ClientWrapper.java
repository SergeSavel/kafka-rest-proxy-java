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

import lombok.Getter;

import java.util.Properties;
import java.util.UUID;

@Getter
public abstract class ClientWrapper implements AutoCloseable {

    private final String id = UUID.randomUUID().toString();
    private final String name;
    private final String username;
    private final int expirationTimeout;
    private final String token = UUID.randomUUID().toString();

    private long expiresAt;

    protected ClientWrapper(String name, Properties config, int expirationTimeout) {
        this.name = name;
        this.username = getUsernameFromConfig(config);
        this.expirationTimeout = expirationTimeout;
        touch();
    }

    private static String getUsernameFromConfig(Properties config) {
        return config.getProperty("sasl.username");
    }

    public void touch() {
        expiresAt = System.currentTimeMillis() + expirationTimeout;
    }

    @Override
    public abstract void close();
}
