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

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

public class ClientKiller<Wrapper extends ClientWrapper> implements AutoCloseable {

    private static final Logger logger = LoggerFactory.getLogger(ClientKiller.class);

    private final ClientProvider<Wrapper> provider;
    private final ScheduledExecutorService executor = Executors.newSingleThreadScheduledExecutor();

    public ClientKiller(ClientProvider<Wrapper> provider) {
        this.provider = provider;
        final var task = new Runnable() {
            @Override
            public void run() {
                retireClients();
            }
        };
        executor.scheduleAtFixedRate(task, 0, 5, TimeUnit.SECONDS);
    }

    @Override
    public void close() {
        executor.shutdownNow();
        try {
            var terminated = executor.awaitTermination(5, TimeUnit.SECONDS);
            if (!terminated) {
                logger.error("Failed to terminate executor.");
            }
        } catch (InterruptedException e) {
            logger.error(e.getMessage(), e);
        }
    }

    public void retireClients() {
        var currentTimestamp = System.currentTimeMillis();
        var clients = provider.getItems();
        for (var client : clients) {
            if (client.expiresAt() <= currentTimestamp) {
                provider.removeItem(client.id());
                logger.info("Removed expired {} with name '{}' and id '{}'.", client.getClass().getSimpleName(), client.name(), client.id());
            }
        }
    }
}
