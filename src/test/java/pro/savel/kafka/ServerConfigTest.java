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

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertThrows;

class ServerConfigTest {

    @Test
    void validConfig_isAccepted() {
        assertDoesNotThrow(() -> config("0.0.0.0", 8086, 0, 1024, 300, 300, 32, 4));
    }

    @Test
    void invalidNetworkSettings_areRejected() {
        assertThrows(IllegalArgumentException.class, () -> config(" ", 8086, 0, 1024, 300, 300, 32, 4));
        assertThrows(IllegalArgumentException.class, () -> config("0.0.0.0", 0, 0, 1024, 300, 300, 32, 4));
        assertThrows(IllegalArgumentException.class, () -> config("0.0.0.0", 65_536, 0, 1024, 300, 300, 32, 4));
        assertThrows(IllegalArgumentException.class, () -> config("0.0.0.0", 8086, -1, 1024, 300, 300, 32, 4));
        assertThrows(IllegalArgumentException.class, () -> config("0.0.0.0", 8086, 0, 0, 300, 300, 32, 4));
    }

    @Test
    void invalidTimeoutsAndRequestLimits_areRejected() {
        assertThrows(IllegalArgumentException.class, () -> config("0.0.0.0", 8086, 0, 1024, 0, 300, 32, 4));
        assertThrows(IllegalArgumentException.class, () -> config("0.0.0.0", 8086, 0, 1024, 300, 0, 32, 4));
        assertThrows(IllegalArgumentException.class, () -> config("0.0.0.0", 8086, 0, 1024, 300, 300, 0, 4));
        assertThrows(IllegalArgumentException.class, () -> config("0.0.0.0", 8086, 0, 1024, 300, 300, 32, 0));
        assertThrows(IllegalArgumentException.class, () -> config("0.0.0.0", 8086, 0, 1024, 300, 300, 4, 5));
    }

    private static ServerConfig config(
            String host,
            int port,
            int workerThreads,
            int backlog,
            int readTimeoutSeconds,
            int writeTimeoutSeconds,
            int maxRequestBytes,
            int maxJsonRequestBytes) {
        return new ServerConfig(
                host,
                port,
                workerThreads,
                backlog,
                readTimeoutSeconds,
                writeTimeoutSeconds,
                maxRequestBytes,
                maxJsonRequestBytes,
                true);
    }
}
