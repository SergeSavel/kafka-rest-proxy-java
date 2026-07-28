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

package pro.savel.kafka.producer.responses;

import lombok.Getter;
import pro.savel.kafka.producer.ProducerWrapper;

@Getter
public class ProducerListing {

    private String id;
    private String name;
    private String owner;
    private String username;
    private long expiresAt;

    private ProducerListing() {
    }

    public static ProducerListing of(ProducerWrapper source) {
        if (source == null)
            return null;
        var result = new ProducerListing();
        result.id = source.getId();
        result.name = source.getName();
        result.owner = source.getOwner();
        result.username = source.getUsername();
        result.expiresAt = source.getExpiresAt();
        return result;
    }
}
