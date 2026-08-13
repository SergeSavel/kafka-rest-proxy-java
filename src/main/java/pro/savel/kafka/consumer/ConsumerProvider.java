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

package pro.savel.kafka.consumer;

import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;
import pro.savel.kafka.common.ClientProvider;
import pro.savel.kafka.common.SaslConfigValidator;
import pro.savel.kafka.common.exceptions.BadRequestException;
import pro.savel.kafka.common.exceptions.NotFoundException;

import java.util.Properties;
import java.util.function.Function;

public class ConsumerProvider extends ClientProvider<ConsumerWrapper> {

    private final Function<Properties, Consumer<byte[], byte[]>> clientFactory;

    public ConsumerProvider() {
        this(config -> {
            var deserializer = new ByteArrayDeserializer();
            return new KafkaConsumer<>(config, deserializer, deserializer);
        });
    }

    public ConsumerProvider(Function<Properties, Consumer<byte[], byte[]>> clientFactory) {
        this.clientFactory = clientFactory;
    }

    public ConsumerWrapper createConsumer(String name, Properties config, int expirationTimeout, String owner) {
        SaslConfigValidator.rejectEmptyScramPassword(config);
        var consumer = clientFactory.apply(config);
        var wrapper = new ConsumerWrapper(name, config, consumer, expirationTimeout, owner);
        addItem(wrapper);
        return wrapper;
    }

    protected ConsumerWrapper getConsumer(String id, String token) throws NotFoundException, BadRequestException {
        var wrapper = getItem(id);
        if (!wrapper.getToken().equals(token))
            throw new BadRequestException("Invalid token.", null);
        return wrapper;
    }

    public void removeConsumer(String id, String token) throws BadRequestException {
        var wrapper = wrappers.get(id);
        if (wrapper == null)
            return;
        if (!token.equals(wrapper.getToken()))
            throw new BadRequestException("Invalid token.", null);
        removeItem(id);
    }
}
