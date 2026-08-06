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

package pro.savel.kafka.producer;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import pro.savel.kafka.common.ClientProvider;
import pro.savel.kafka.common.exceptions.BadRequestException;
import pro.savel.kafka.common.exceptions.NotFoundException;

import java.util.Properties;
import java.util.function.Function;

public class ProducerProvider extends ClientProvider<ProducerWrapper> {

    private final Function<Properties, Producer<byte[], byte[]>> clientFactory;

    public ProducerProvider() {
        this(config -> {
            var serializer = new ByteArraySerializer();
            return new KafkaProducer<>(config, serializer, serializer);
        });
    }

    public ProducerProvider(Function<Properties, Producer<byte[], byte[]>> clientFactory) {
        this.clientFactory = clientFactory;
    }

    public ProducerWrapper createProducer(String name, Properties config, int expirationTimeout, String owner) {
        var producer = clientFactory.apply(config);
        var wrapper = new ProducerWrapper(name, config, producer, expirationTimeout, owner);
        addItem(wrapper);
        return wrapper;
    }

    protected ProducerWrapper getProducer(String id, String token) throws NotFoundException, BadRequestException {
        var wrapper = getItem(id);
        if (!wrapper.getToken().equals(token))
            throw new BadRequestException("Invalid token.", null);
        return wrapper;
    }

    public void removeProducer(String id, String token) throws BadRequestException {
        var wrapper = wrappers.get(id);
        if (wrapper == null)
            return;
        if (!token.equals(wrapper.getToken()))
            throw new BadRequestException("Invalid token.", null);
        removeItem(id);
    }
}
