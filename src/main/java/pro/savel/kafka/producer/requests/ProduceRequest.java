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

package pro.savel.kafka.producer.requests;

import java.util.Arrays;
import java.util.Map;
import java.util.Objects;
import java.util.UUID;

public final class ProduceRequest implements ProducerRequest {

    private UUID id;
    private String token;
    private String topic;
    private Integer partition;
    private Map<String, byte[]> headers;
    private byte[] key;
    private byte[] value;

    public UUID getId() {
        return id;
    }

    public void setId(UUID id) {
        this.id = id;
    }

    public String getToken() {
        return token;
    }

    public void setToken(String token) {
        this.token = token;
    }

    public String getTopic() {
        return topic;
    }

    public void setTopic(String topic) {
        this.topic = topic;
    }

    public Integer getPartition() {
        return partition;
    }

    public void setPartition(Integer partition) {
        this.partition = partition;
    }

    public Map<String, byte[]> getHeaders() {
        return headers;
    }

    public void setHeaders(Map<String, byte[]> headers) {
        this.headers = headers;
    }

    public byte[] getKey() {
        return key;
    }

    public void setKey(byte[] key) {
        this.key = key;
    }

    public byte[] getValue() {
        return value;
    }

    public void setValue(byte[] value) {
        this.value = value;
    }

    @Override
    public boolean equals(Object obj) {
        if (obj == this) return true;
        if (obj == null || obj.getClass() != this.getClass()) return false;
        var that = (ProduceRequest) obj;
        return Objects.equals(this.headers, that.headers) &&
                Arrays.equals(this.key, that.key) &&
                Arrays.equals(this.value, that.value);
    }

    @Override
    public int hashCode() {
        return Objects.hash(headers, Arrays.hashCode(key), Arrays.hashCode(value));
    }
}
