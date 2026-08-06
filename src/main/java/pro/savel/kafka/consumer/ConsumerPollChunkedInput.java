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

package pro.savel.kafka.consumer;

import com.fasterxml.jackson.databind.ObjectMapper;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufAllocator;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.stream.ChunkedInput;
import pro.savel.kafka.common.contract.Serde;
import pro.savel.kafka.consumer.responses.ConsumerPollResponse;

final class ConsumerPollChunkedInput implements ChunkedInput<ByteBuf> {

    private final ObjectMapper objectMapper;
    private final Serde serde;
    private final int chunkSize;

    private ConsumerPollResponse response;
    private ByteBuf currentBuffer;
    private int messageIndex;
    private boolean started;
    private boolean complete;
    private long progress;

    ConsumerPollChunkedInput(ObjectMapper objectMapper, ConsumerPollResponse response, Serde serde, int chunkSize) {
        if (objectMapper == null)
            throw new IllegalArgumentException("objectMapper must not be null");
        if (response == null)
            throw new IllegalArgumentException("response must not be null");
        if (serde != Serde.JSON && serde != Serde.BINARY)
            throw new IllegalArgumentException("Unsupported poll response serde: " + serde);
        if (chunkSize <= 0)
            throw new IllegalArgumentException("chunkSize must be greater than 0");
        this.objectMapper = objectMapper;
        this.response = response;
        this.serde = serde;
        this.chunkSize = chunkSize;
    }

    @Override
    public boolean isEndOfInput() {
        return complete && currentBuffer == null;
    }

    @Override
    public void close() {
        if (currentBuffer != null) {
            currentBuffer.release();
            currentBuffer = null;
        }
        response = null;
        complete = true;
    }

    @Deprecated
    @Override
    public ByteBuf readChunk(ChannelHandlerContext ctx) throws Exception {
        return readChunk(ctx.alloc());
    }

    @Override
    public ByteBuf readChunk(ByteBufAllocator allocator) throws Exception {
        if (isEndOfInput())
            return null;
        if (currentBuffer == null)
            currentBuffer = serializeNext(allocator);

        ByteBuf chunk;
        if (currentBuffer.readableBytes() <= chunkSize) {
            chunk = currentBuffer;
            currentBuffer = null;
        } else {
            chunk = currentBuffer.readRetainedSlice(chunkSize);
        }
        progress += chunk.readableBytes();
        return chunk;
    }

    @Override
    public long length() {
        return -1;
    }

    @Override
    public long progress() {
        return progress;
    }

    private ByteBuf serializeNext(ByteBufAllocator allocator) throws Exception {
        return switch (serde) {
            case JSON -> serializeNextJson(allocator);
            case BINARY -> serializeNextBinary(allocator);
        };
    }

    private ByteBuf serializeNextJson(ByteBufAllocator allocator) throws Exception {
        if (messageIndex < response.size()) {
            var message = response.get(messageIndex);
            var result = ConsumerResponseSerializer.serializePollJsonMessage(
                    objectMapper, allocator, message, messageIndex == 0);
            messageIndex++;
            started = true;
            return result;
        }

        complete = true;
        var suffix = allocator.buffer(started ? 1 : 2);
        if (!started)
            suffix.writeByte('[');
        suffix.writeByte(']');
        return suffix;
    }

    private ByteBuf serializeNextBinary(ByteBufAllocator allocator) {
        if (!started) {
            started = true;
            if (response.isEmpty())
                complete = true;
            return ConsumerResponseSerializer.serializePollBinaryHeader(allocator, response.size());
        }

        var result = ConsumerResponseSerializer.serializePollBinaryMessage(allocator, response.get(messageIndex));
        messageIndex++;
        if (messageIndex == response.size())
            complete = true;
        return result;
    }
}
