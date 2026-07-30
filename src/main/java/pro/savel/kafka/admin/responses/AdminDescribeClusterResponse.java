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

import java.util.ArrayList;
import java.util.Collection;
import lombok.Getter;
import org.apache.kafka.common.acl.AclOperation;
import pro.savel.kafka.common.contract.Node;

@Getter
public class AdminDescribeClusterResponse implements AdminResponse {

    private String clusterId;

    @Deprecated
    private Node controller;

    private Integer controllerId;
    private Collection<Node> nodes;
    private Collection<String> authorizedOperations;

    private AdminDescribeClusterResponse() {}

    public static AdminDescribeClusterResponse of(
        String clusterId,
        org.apache.kafka.common.Node controller,
        Collection<org.apache.kafka.common.Node> nodes,
        Collection<AclOperation> authorizedOperations
    ) {
        var result = new AdminDescribeClusterResponse();
        result.clusterId = clusterId;
        result.controller = Node.of(controller);
        result.controllerId = controller == null ? null : controller.id();
        result.nodes = Node.of(nodes);
        if (authorizedOperations != null) {
            result.authorizedOperations = new ArrayList<>(authorizedOperations.size());
            authorizedOperations.forEach(op -> result.authorizedOperations.add(op.name()));
        }
        return result;
    }
}
