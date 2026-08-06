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

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import pro.savel.kafka.common.exceptions.NotFoundException;

import java.time.Duration;
import java.util.Properties;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.*;

class ClientProviderTest {

    // Test stub — concrete ClientWrapper
    static class TestWrapper extends ClientWrapper {
        volatile boolean closed = false;
        volatile boolean closedOnVirtualThread = false;
        final boolean shouldThrow;
        final CountDownLatch closedLatch = new CountDownLatch(1);

        TestWrapper(String id, String name, int expirationTimeout) {
            this(id, name, expirationTimeout, false);
        }

        TestWrapper(String id, String name, int expirationTimeout, boolean shouldThrow) {
            super(id, name, new Properties(), expirationTimeout, "test-owner");
            this.shouldThrow = shouldThrow;
        }

        @Override
        public void close() {
            if (shouldThrow)
                throw new RuntimeException("close failed");
            closed = true;
            closedOnVirtualThread = Thread.currentThread().isVirtual();
            closedLatch.countDown();
        }

        boolean awaitClosed() throws InterruptedException {
            return closedLatch.await(1, TimeUnit.SECONDS);
        }
    }

    // Test stub — concrete ClientProvider
    static class TestProvider extends ClientProvider<TestWrapper> {
    }

    static class BlockingTestWrapper extends TestWrapper {
        private final CountDownLatch closeStarted;
        private final CountDownLatch allowClose;

        BlockingTestWrapper(String id, CountDownLatch closeStarted, CountDownLatch allowClose) {
            super(id, id, 60_000);
            this.closeStarted = closeStarted;
            this.allowClose = allowClose;
        }

        @Override
        public void close() {
            closeStarted.countDown();
            try {
                allowClose.await();
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
            super.close();
        }
    }

    static class UninterruptibleBlockingTestWrapper extends TestWrapper {
        private final CountDownLatch closeStarted;
        private final CountDownLatch allowClose;

        UninterruptibleBlockingTestWrapper(String id, CountDownLatch closeStarted, CountDownLatch allowClose) {
            super(id, id, 60_000);
            this.closeStarted = closeStarted;
            this.allowClose = allowClose;
        }

        @Override
        public void close() {
            closeStarted.countDown();
            var interrupted = false;
            while (true) {
                try {
                    allowClose.await();
                    break;
                } catch (InterruptedException e) {
                    interrupted = true;
                }
            }
            if (interrupted)
                Thread.currentThread().interrupt();
            super.close();
        }
    }

    private TestProvider provider = new TestProvider();

    @AfterEach
    void tearDown() {
        provider.close();
    }

//region addItem / getItem / getItems

    @Test
    void addItem_and_getItem_returnsWrapper() throws NotFoundException {
        var wrapper = new TestWrapper("id-1", "test", 60_000);
        provider.addItem(wrapper);
        assertSame(wrapper, provider.getItem("id-1"));
    }

    @Test
    void getItem_notFound_throwsNotFoundException() {
        assertThrows(NotFoundException.class, () -> provider.getItem("nonexistent"));
    }

    @Test
    void getItems_empty_returnsEmpty() {
        assertTrue(provider.getItems().isEmpty());
    }

    @Test
    void getItems_afterAdd_returnsAll() {
        provider.addItem(new TestWrapper("a", "n1", 60_000));
        provider.addItem(new TestWrapper("b", "n2", 60_000));
        assertEquals(2, provider.getItems().size());
    }

//endregion

//region removeItem

    @Test
    void removeItem_existing_closesAndRemoves() {
        var wrapper = new TestWrapper("id-1", "test", 60_000);
        provider.addItem(wrapper);
        provider.removeItem("id-1");
        assertTrue(wrapper.closed);
        assertTrue(wrapper.closedOnVirtualThread);
        assertTrue(provider.getItems().isEmpty());
    }

    @Test
    void removeItem_nonexistent_doesNothing() {
        assertDoesNotThrow(() -> provider.removeItem("nonexistent"));
    }

//endregion

//region close

    @Test
    void close_closesAllWrappers() {
        var w1 = new TestWrapper("a", "n1", 60_000);
        var w2 = new TestWrapper("b", "n2", 60_000);
        provider.addItem(w1);
        provider.addItem(w2);
        provider.close();
        assertTrue(w1.closed);
        assertTrue(w2.closed);
    }

    @Test
    void close_exceptionInOneWrapper_closesRemaining() {
        var good = new TestWrapper("good", "g", 60_000);
        var bad = new TestWrapper("bad", "b", 60_000, true);

        provider.addItem(good);
        provider.addItem(bad);

        assertDoesNotThrow(() -> provider.close());
        assertTrue(good.closed);
    }

    @Test
    void close_startsWrapperCloseOperationsConcurrently() throws InterruptedException {
        var closeStarted = new CountDownLatch(2);
        var allowClose = new CountDownLatch(1);
        provider.addItem(new BlockingTestWrapper("a", closeStarted, allowClose));
        provider.addItem(new BlockingTestWrapper("b", closeStarted, allowClose));

        var closeThread = Thread.startVirtualThread(provider::close);
        try {
            assertTrue(closeStarted.await(1, TimeUnit.SECONDS));
        } finally {
            allowClose.countDown();
        }
        closeThread.join(1_000);
        assertFalse(closeThread.isAlive());
    }

    @Test
    void close_returnsAtDeadlineWhenWrapperIgnoresInterruption() throws InterruptedException {
        var closeStarted = new CountDownLatch(1);
        var allowClose = new CountDownLatch(1);
        var wrapper = new UninterruptibleBlockingTestWrapper("a", closeStarted, allowClose);
        provider.addItem(wrapper);

        var closeThread = Thread.startVirtualThread(
                () -> provider.close(ShutdownDeadline.after(Duration.ofMillis(100))));
        assertTrue(closeStarted.await(1, TimeUnit.SECONDS));
        closeThread.join(1_000);

        assertFalse(closeThread.isAlive());
        allowClose.countDown();
        assertTrue(wrapper.awaitClosed());
    }

//endregion

//region retireClients (expiration)

    @Test
    void retireClients_expired_removesWrapper() throws InterruptedException {
        var wrapper = new TestWrapper("id-1", "test", 1); // 1ms expiration
        provider.addItem(wrapper);

        // Wait for expiration
        try { Thread.sleep(50); } catch (InterruptedException ignored) { Thread.currentThread().interrupt(); }

        provider.retireClients();
        assertTrue(provider.getItems().isEmpty());
        assertTrue(wrapper.awaitClosed());
        assertTrue(wrapper.closedOnVirtualThread);
    }

    @Test
    void retireClients_notExpired_keepsWrapper() {
        var wrapper = new TestWrapper("id-1", "test", 60_000); // 60s expiration
        provider.addItem(wrapper);

        provider.retireClients();
        assertFalse(wrapper.closed);
        assertEquals(1, provider.getItems().size());
    }

    @Test
    void touch_extendsExpiration() {
        var wrapper = new TestWrapper("id-1", "test", 50); // 50ms
        provider.addItem(wrapper);

        // Touch before expiration
        try { Thread.sleep(30); } catch (InterruptedException ignored) { Thread.currentThread().interrupt(); }
        wrapper.touch();

        // Should not be expired yet
        try { Thread.sleep(30); } catch (InterruptedException ignored) { Thread.currentThread().interrupt(); }
        provider.retireClients();
        assertFalse(wrapper.closed);
    }

//endregion
}
