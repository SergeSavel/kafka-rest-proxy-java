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

package pro.savel.kafka;

import com.fasterxml.jackson.databind.ObjectMapper;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.ChannelPipeline;
import io.netty.channel.socket.SocketChannel;
import io.netty.handler.codec.http.HttpObjectAggregator;
import io.netty.handler.codec.http.HttpServerCodec;

class ServerInitializer extends ChannelInitializer<SocketChannel> implements AutoCloseable {

    private static final ObjectMapper objectMapper = new ObjectMapper();
    private final RequestDecoder requestdecoder = new RequestDecoder(objectMapper);
    private final RequestProcessor requestProcessor = new RequestProcessor();
    private final ResponseEncoder responseEncoder = new ResponseEncoder(objectMapper);

    @Override
    protected void initChannel(SocketChannel channel) {

        ChannelPipeline pipeline = channel.pipeline();
        pipeline.addLast(new HttpServerCodec());
        pipeline.addLast(new HttpObjectAggregator(32 * 1024 * 1024));
        pipeline.addLast(requestdecoder);
        pipeline.addLast(requestProcessor);
        pipeline.addLast(responseEncoder);
    }

    @Override
    public void close() {
        requestProcessor.close();
    }
}
