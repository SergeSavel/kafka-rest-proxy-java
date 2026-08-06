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
import pro.savel.kafka.common.ShutdownDeadline;

import java.time.Duration;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertTrue;

class ServerLifecycleTest {

    @Test
    void close_executesStepsOnceInOrderWithSharedDeadline() {
        var order = new CopyOnWriteArrayList<Integer>();
        var deadlines = new CopyOnWriteArrayList<ShutdownDeadline>();
        var lifecycle = new ServerLifecycle(Duration.ofSeconds(1),
                deadline -> {
                    deadlines.add(deadline);
                    order.add(1);
                },
                deadline -> {
                    deadlines.add(deadline);
                    order.add(2);
                },
                deadline -> {
                    deadlines.add(deadline);
                    order.add(3);
                });

        lifecycle.close();
        lifecycle.close();

        assertEquals(List.of(1, 2, 3), order);
        assertEquals(3, deadlines.size());
        assertSame(deadlines.get(0), deadlines.get(1));
        assertSame(deadlines.get(0), deadlines.get(2));
    }

    @Test
    void concurrentClose_waitsForRunningShutdownAndDoesNotRepeatSteps() throws InterruptedException {
        var stepCalls = new AtomicInteger();
        var stepStarted = new CountDownLatch(1);
        var allowStepToFinish = new CountDownLatch(1);
        var secondCloseReturned = new CountDownLatch(1);
        var lifecycle = new ServerLifecycle(Duration.ofSeconds(1), deadline -> {
            stepCalls.incrementAndGet();
            stepStarted.countDown();
            awaitUninterruptibly(allowStepToFinish);
        });

        var firstClose = Thread.startVirtualThread(lifecycle::close);
        assertTrue(stepStarted.await(1, TimeUnit.SECONDS));
        var secondClose = Thread.startVirtualThread(() -> {
            lifecycle.close();
            secondCloseReturned.countDown();
        });

        assertFalse(secondCloseReturned.await(50, TimeUnit.MILLISECONDS));
        allowStepToFinish.countDown();
        firstClose.join(1_000);
        secondClose.join(1_000);

        assertFalse(firstClose.isAlive());
        assertFalse(secondClose.isAlive());
        assertEquals(1, stepCalls.get());
    }

    private static void awaitUninterruptibly(CountDownLatch latch) {
        var interrupted = false;
        while (true) {
            try {
                latch.await();
                break;
            } catch (InterruptedException e) {
                interrupted = true;
            }
        }
        if (interrupted)
            Thread.currentThread().interrupt();
    }
}
