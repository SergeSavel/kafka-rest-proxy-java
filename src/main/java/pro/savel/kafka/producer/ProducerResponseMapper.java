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

import pro.savel.kafka.producer.responses.Producer;
import pro.savel.kafka.producer.responses.ProducerWithTokenResponse;

public abstract class ProducerResponseMapper {

    public static Producer mapProducer(ProducerWrapper source) {
        if (source == null)
            return null;
        var result = new Producer();
        result.setId(source.getId());
        result.setName(source.getName());
        result.setUsername(source.getUsername());
        result.setExpiresAt(source.getExpiresAt());
        return result;
    }

    public static ProducerWithTokenResponse mapProducerWithToken(ProducerWrapper source) {
        if (source == null)
            return null;
        var result = new ProducerWithTokenResponse();
        result.setId(source.getId());
        result.setName(source.getName());
        result.setUsername(source.getUsername());
        result.setExpiresAt(source.getExpiresAt());
        result.setToken(source.getToken());
        return result;
    }
}
