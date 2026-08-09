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

import io.netty.channel.ChannelHandler;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandlerAdapter;
import io.netty.util.ReferenceCountUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import pro.savel.kafka.common.contract.Request;

import java.util.concurrent.Callable;
import java.util.concurrent.CompletionException;
import java.util.concurrent.ExecutionException;
import java.util.function.Consumer;

/**
 * Shared scaffolding for the producer/consumer/admin request processors: request-type filtering,
 * error handling and unwrapping, and blocking-task dispatch. Subclasses implement
 * {@link #processRequest} and, if they map an exception type CommonErrors doesn't know about
 * (e.g. consumer's {@code InvalidOffsetException}), override {@link #handleSpecificError}.
 * <p>
 * Deliberately doesn't override {@code exceptionCaught}: the inherited default
 * ({@code ChannelHandlerAdapter}) already just calls {@code ctx.fireExceptionCaught(cause)}, which is
 * what lets {@code DefaultInboundHandler} further down the pipeline turn read/write timeouts into
 * proper 408/504 responses. An override here would intercept and stop that propagation.
 * <p>
 * Lifecycle is out of scope here too: {@code ServerInitializer} owns and closes each {@code *Provider}
 * directly, so processors don't implement {@code AutoCloseable}.
 */
@ChannelHandler.Sharable
public abstract class AbstractRequestProcessor extends ChannelInboundHandlerAdapter {

    protected final Logger logger = LoggerFactory.getLogger(getClass());

    private final String kind;
    private final Class<? extends Request> requestType;
    private final BlockingTaskExecutor blockingTaskExecutor;

    protected AbstractRequestProcessor(String kind, Class<? extends Request> requestType,
                                        BlockingTaskExecutor blockingTaskExecutor) {
        this.kind = kind;
        this.requestType = requestType;
        this.blockingTaskExecutor = blockingTaskExecutor;
    }

    @Override
    public void channelRead(ChannelHandlerContext ctx, Object msg) {
        if (msg instanceof RequestBearer bearer && requestType.isInstance(bearer.request())) {
            try {
                processRequest(ctx, bearer);
            } catch (Exception e) {
                if (!handleError(ctx, e)) {
                    logger.error("An unexpected error occurred while processing {} request.", kind, e);
                    HttpUtils.writeInternalServerErrorAndClose(ctx, Utils.combineErrorMessage(e));
                }
            } finally {
                ReferenceCountUtil.release(msg);
            }
        } else {
            ctx.fireChannelRead(msg);
        }
    }

    protected abstract void processRequest(ChannelHandlerContext ctx, RequestBearer requestBearer);

    protected <T> void execute(ChannelHandlerContext ctx, Callable<T> operation, Consumer<T> completion) {
        blockingTaskExecutor.execute(ctx, operation, (result, error) -> {
            if (error == null) {
                completion.accept(result);
            } else if (!handleError(ctx, error)) {
                logger.error("An unexpected error occurred while processing {} request.", kind, error);
                HttpUtils.writeInternalServerErrorAndClose(ctx, Utils.combineErrorMessage(error));
            }
        });
    }

    protected boolean handleError(ChannelHandlerContext ctx, Throwable error) {
        if ((error instanceof CompletionException || error instanceof ExecutionException) && error.getCause() != null)
            return handleError(ctx, error.getCause());
        if (error instanceof org.apache.kafka.common.errors.TimeoutException && error.getCause() != null)
            return handleError(ctx, error.getCause());
        if (handleSpecificError(ctx, error))
            return true;
        return CommonErrors.handle(ctx, error);
    }

    /**
     * Hook for a processor-specific exception type CommonErrors doesn't cover (e.g. consumer's
     * InvalidOffsetException). Return true if this method wrote a response for the error.
     */
    protected boolean handleSpecificError(ChannelHandlerContext ctx, Throwable error) {
        return false;
    }
}
