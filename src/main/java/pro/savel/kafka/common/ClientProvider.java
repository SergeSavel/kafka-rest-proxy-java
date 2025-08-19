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

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import pro.savel.kafka.common.exceptions.NotFoundException;

import java.util.Collection;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

public abstract class ClientProvider<Wrapper extends ClientWrapper> implements AutoCloseable {

    private static final Logger logger = LoggerFactory.getLogger(ClientProvider.class);

    protected final ConcurrentHashMap<String, Wrapper> wrappers = new ConcurrentHashMap<>();
    private final ScheduledExecutorService retirer = Executors.newSingleThreadScheduledExecutor();

    public ClientProvider() {
        final var task = new Runnable() {
            @Override
            public void run() {
                retireClients();
            }
        };
        retirer.scheduleAtFixedRate(task, 0, 1, TimeUnit.SECONDS);
    }

    @Override
    public void close() {
        wrappers.forEach((uuid, wrapper) -> wrapper.close());
        wrappers.clear();
        retirer.shutdownNow();
        try {
            var terminated = retirer.awaitTermination(5, TimeUnit.SECONDS);
            if (!terminated) {
                logger.error("Failed to terminate executor.");
            }
        } catch (InterruptedException e) {
            logger.error(e.getMessage(), e);
        }
    }

    private void retireClients() {
        var currentTimestamp = System.currentTimeMillis();
        var clients = getItems();
        for (var client : clients) {
            if (client.getExpiresAt() <= currentTimestamp) {
                removeItem(client.getId());
                logger.info("Removed expired {} with name '{}' and id '{}'.", client.getClass().getSimpleName(), client.getName(), client.getId());
            }
        }
    }

    public Collection<Wrapper> getItems() {
        return wrappers.values();
    }

    protected void addItem(Wrapper wrapper) {
        wrappers.put(wrapper.getId(), wrapper);
    }

    protected Wrapper getItem(String id) throws NotFoundException {
        var wrapper = wrappers.get(id);
        if (wrapper == null)
            throw new NotFoundException("Client not found.", null);
        return wrapper;
    }

//    protected Wrapper getItem(String id, String token) throws NotFoundException, BadRequestException {
//        var wrapper = getItem(id);
//        if (!wrapper.getToken().equals(token))
//            throw new BadRequestException("Invalid token.", null);
//        return wrapper;
//    }

    protected void removeItem(String id) {
        var wrapper = wrappers.remove(id);
        if (wrapper != null)
            wrapper.close();
    }

//    protected void removeItem(String id, String token) throws BadRequestException {
//        var wrapper = wrappers.get(id);
//        if (wrapper == null)
//            return;
//        if (!token.equals(wrapper.getToken()))
//            throw new BadRequestException("Invalid token.", null);
//        wrapper = wrappers.remove(id);
//        if (wrapper != null)
//            wrapper.close();
//    }
}
