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

import lombok.Getter;

import java.time.Duration;
import java.util.Properties;
import java.util.concurrent.TimeUnit;

@Getter
public abstract class ClientWrapper implements AutoCloseable {

    protected static final Duration CLOSE_TIMEOUT = Duration.ofSeconds(30);

    private final String id;
    private final String name;
    private final String username;
    private final int expirationTimeout;
    private final String owner;

    // expiresAt is wall clock for the list endpoints; the retirer decides by the monotonic
    // deadline, so clock adjustments can neither expire instances early nor keep them alive.
    private volatile long expiresAt;
    private volatile long deadlineNanos;

    protected ClientWrapper(String id, String name, Properties config, int expirationTimeout, String owner) {
        this.id = id;
        this.name = name;
        this.username = SaslConfigValidator.usernameFromJaasConfig(config);
        this.expirationTimeout = expirationTimeout;
        this.owner = owner;
        touch();
    }

    public void touch() {
        expiresAt = System.currentTimeMillis() + expirationTimeout;
        deadlineNanos = System.nanoTime() + TimeUnit.MILLISECONDS.toNanos(expirationTimeout);
    }

    @Override
    public abstract void close();
}
