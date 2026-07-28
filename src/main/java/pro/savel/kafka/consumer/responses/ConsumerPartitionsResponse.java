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

package pro.savel.kafka.consumer.responses;

import lombok.Getter;

import java.util.ArrayList;
import java.util.Collection;

public class ConsumerPartitionsResponse extends ArrayList<ConsumerPartitionsResponse.PartitionInfo> implements ConsumerResponse {

    private ConsumerPartitionsResponse(int initialCapacity) {
        super(initialCapacity);
    }

    public static ConsumerPartitionsResponse of(Collection<org.apache.kafka.common.PartitionInfo> source) {
        if (source == null)
            return null;
        var result =  new ConsumerPartitionsResponse(source.size());
        source.forEach(partitionInfoSource -> result.add(PartitionInfo.of(partitionInfoSource)));
        return result;
    }

    @Getter
    public static class PartitionInfo {

        private int partition;

        private PartitionInfo() {
        }

        private static ConsumerPartitionsResponse.PartitionInfo of(org.apache.kafka.common.PartitionInfo source) {
            if (source == null)
                return null;
            var result = new PartitionInfo();
            result.partition = source.partition();
            return result;
        }
    }
}
