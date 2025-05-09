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

package pro.savel.kafka.admin.responses;

import lombok.Data;
import pro.savel.kafka.common.contract.Node;

import java.util.Collection;

@Data
public class AdminDescribeTopicResponse implements AdminResponse {
    private String id;
    private String name;
    private boolean isInternal;
    private Collection<String> authorizedOperations;
    private Collection<PartitionInfo> partitions;

    @Data
    public static class PartitionInfo {
        private int id;
        private Collection<Node> replicas;
        private Collection<Integer> isr;
    }
}
