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

package pro.savel.kafka.producer.responses;

import lombok.Getter;
import pro.savel.kafka.producer.ProducerWrapper;

@Getter
public class ProducerCreateResponse implements ProducerResponse {

    private String id;
    private String token;

    private ProducerCreateResponse() {
    }

    public static ProducerCreateResponse of(ProducerWrapper source) {
        if (source == null)
            return null;
        var result = new ProducerCreateResponse();
        result.id = source.getId();
        result.token = source.getToken();
        return result;
    }
}
