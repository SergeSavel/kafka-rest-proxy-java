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

package pro.savel.kafka.producer.requests;

import jakarta.validation.constraints.NotEmpty;
import jakarta.validation.constraints.PositiveOrZero;
import lombok.Data;
import lombok.Getter;

import java.util.List;

@Data
public class ProducerSendRequest implements ProducerRequest {
    @NotEmpty
    private String producerId;
    @NotEmpty
    private String token;
    @NotEmpty
    private String topic;
    @PositiveOrZero
    private Integer partition;
    private List<Header> headers;
    private byte[] key;
    private byte[] value;

    // A list, not a map: Kafka headers allow duplicate keys.
    @Getter
    public static class Header {

        private final String key;
        private final byte[] value;

        public Header(String key, byte[] value) {
            this.key = key;
            this.value = value;
        }
    }
}
