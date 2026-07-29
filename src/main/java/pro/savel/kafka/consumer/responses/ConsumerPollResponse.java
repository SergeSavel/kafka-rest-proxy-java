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

package pro.savel.kafka.consumer.responses;

import org.apache.kafka.clients.consumer.ConsumerRecords;

import java.util.ArrayList;

public class ConsumerPollResponse extends ArrayList<ConsumerMessage> implements ConsumerResponse {

    private ConsumerPollResponse(int initialCapacity) {
        super(initialCapacity);
    }

    public static ConsumerPollResponse of(ConsumerRecords<byte[], byte[]> source) {
        if (source == null)
            return null;
        var result = new ConsumerPollResponse(source.count());
        source.forEach(record -> result.add(ConsumerMessage.of(record)));
        return result;
    }
}
