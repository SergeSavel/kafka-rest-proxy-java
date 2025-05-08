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

import lombok.EqualsAndHashCode;
import lombok.Getter;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import pro.savel.kafka.common.ClientWrapper;

import java.util.Properties;

@Getter
@EqualsAndHashCode(callSuper = false)
public class ProducerWrapper extends ClientWrapper {

    private final KafkaProducer<byte[], byte[]> producer;

    protected ProducerWrapper(String name, Properties config, int expirationTimeout) {
        super(name, config, expirationTimeout);
        var serializer = new ByteArraySerializer();
        producer = new KafkaProducer<>(config, serializer, serializer);
    }

    @Override
    public void close() {
        producer.flush();
        producer.close();
    }
}
