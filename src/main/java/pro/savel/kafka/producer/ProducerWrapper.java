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

package pro.savel.kafka.producer;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.ByteBufferSerializer;
import pro.savel.kafka.common.ClientWrapper;

import java.util.Map;
import java.util.Properties;

public class ProducerWrapper extends ClientWrapper {

    KafkaProducer<byte[], byte[]> producer;

    protected ProducerWrapper(String name, Map<String, String> config, int expirationTimeout) {
        super(name, config, expirationTimeout);

        var properties = new Properties(config.size());
        for (Map.Entry<String, String> entry : config.entrySet()) {
            properties.setProperty(entry.getKey(), entry.getValue());
        }
        properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, ByteBufferSerializer.class.getName());
        properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, ByteBufferSerializer.class.getName());

        producer = new KafkaProducer<>(properties);
    }

    @Override
    public void close() {
        producer.close();
    }
}
