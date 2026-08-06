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

import java.time.Duration;
import java.util.ArrayList;
import java.util.Collection;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;

public abstract class ClientProvider<Wrapper extends ClientWrapper> implements AutoCloseable {

    private static final Logger logger = LoggerFactory.getLogger(ClientProvider.class);
    private static final int DEFAULT_CLOSE_PARALLELISM = 32;
    private static final int CLOSE_PARALLELISM = Math.max(1,
            Integer.getInteger("client.close.parallelism", DEFAULT_CLOSE_PARALLELISM));
    private static final Duration DEFAULT_SHUTDOWN_TIMEOUT = Duration.ofSeconds(35);

    protected final ConcurrentHashMap<String, Wrapper> wrappers = new ConcurrentHashMap<>();
    private final ExecutorService closeExecutor = Executors.newVirtualThreadPerTaskExecutor();
    private final Semaphore closePermits = new Semaphore(CLOSE_PARALLELISM);
    private final ScheduledExecutorService retirer = Executors.newSingleThreadScheduledExecutor(r -> {
        var t = new Thread(r, "client-retirer");
        t.setDaemon(true);
        return t;
    });

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
        close(ShutdownDeadline.after(DEFAULT_SHUTDOWN_TIMEOUT));
    }

    public void close(ShutdownDeadline deadline) {
        retirer.shutdownNow();
        awaitRetirerTermination(deadline);

        var detachedWrappers = new ArrayList<Wrapper>(wrappers.size());
        wrappers.forEach((id, wrapper) -> {
            if (wrappers.remove(id, wrapper))
                detachedWrappers.add(wrapper);
        });
        detachedWrappers.forEach(this::closeInBackground);

        closeExecutor.shutdown();
        try {
            if (!closeExecutor.awaitTermination(deadline.remainingNanos(), TimeUnit.NANOSECONDS)) {
                logger.warn("Timed out waiting for Kafka clients to close.");
                closeExecutor.shutdownNow();
            }
        } catch (InterruptedException e) {
            closeExecutor.shutdownNow();
            Thread.currentThread().interrupt();
        }
    }

    private void awaitRetirerTermination(ShutdownDeadline deadline) {
        try {
            if (!retirer.awaitTermination(deadline.remainingNanos(), TimeUnit.NANOSECONDS))
                logger.warn("Failed to terminate client retirer.");
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }

    void retireClients() {
        var currentTimestamp = System.currentTimeMillis();
        var clients = getItems();
        for (var client : clients) {
            if (client.getExpiresAt() <= currentTimestamp && wrappers.remove(client.getId(), client)) {
                closeInBackground(client);
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

    protected void removeItem(String id) {
        var wrapper = wrappers.remove(id);
        if (wrapper != null)
            closeAsync(wrapper).join();
    }

    private void closeInBackground(Wrapper wrapper) {
        closeAsync(wrapper).whenComplete((ignored, error) -> {
            if (error != null)
                logger.warn("Failed to close {} with id '{}'.", wrapper.getClass().getSimpleName(), wrapper.getId(),
                        unwrap(error));
        });
    }

    private CompletableFuture<Void> closeAsync(Wrapper wrapper) {
        try {
            return CompletableFuture.runAsync(() -> closeWrapper(wrapper), closeExecutor);
        } catch (RejectedExecutionException e) {
            try {
                wrapper.close();
                return CompletableFuture.completedFuture(null);
            } catch (Throwable closeError) {
                return CompletableFuture.failedFuture(closeError);
            }
        }
    }

    private void closeWrapper(Wrapper wrapper) {
        var acquired = false;
        try {
            closePermits.acquire();
            acquired = true;
            wrapper.close();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new CompletionException(e);
        } finally {
            if (acquired)
                closePermits.release();
        }
    }

    private static Throwable unwrap(Throwable error) {
        var result = error;
        while (result instanceof CompletionException && result.getCause() != null)
            result = result.getCause();
        return result;
    }
}
