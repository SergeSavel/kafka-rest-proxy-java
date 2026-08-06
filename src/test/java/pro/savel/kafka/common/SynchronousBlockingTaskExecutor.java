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

import io.netty.channel.ChannelHandlerContext;

import java.util.concurrent.Callable;
import java.util.function.BiConsumer;

/**
 * Test double for {@link BlockingTaskExecutor} that runs the operation synchronously on the calling
 * thread instead of dispatching to a virtual thread and the channel's event loop. This keeps processor
 * tests deterministic and avoids the need to pump an {@link io.netty.channel.embedded.EmbeddedChannel}
 * while waiting for a background thread.
 */
public class SynchronousBlockingTaskExecutor extends BlockingTaskExecutor {

    @Override
    public <T> void execute(ChannelHandlerContext ctx, Callable<T> operation, BiConsumer<T, Throwable> completion) {
        T result = null;
        Throwable error = null;
        try {
            result = operation.call();
        } catch (Throwable e) {
            error = e;
        }
        completion.accept(result, error);
    }
}
