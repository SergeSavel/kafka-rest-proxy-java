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

package pro.savel.kafka.consumer;

import pro.savel.kafka.consumer.responses.*;

import java.util.*;

@Deprecated
public class ConsumerResponseMapper {

    @Deprecated
    public static ConsumerListPartitionsResponse mapPartitionsResponse(Collection<org.apache.kafka.common.PartitionInfo> source) {
        if (source == null)
            return null;
        var result = new ConsumerListPartitionsResponse();
        result.setPartitions(new ArrayList<>(source.size()));
        source.forEach(partitionInfoSource -> {
            result.setTopic(partitionInfoSource.topic());
            var partitionInfo = new ConsumerListPartitionsResponse.PartitionInfo();
            partitionInfo.setPartition(partitionInfoSource.partition());
            result.getPartitions().add(partitionInfo);
        });
        return result;
    }

}
