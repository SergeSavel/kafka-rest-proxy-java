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

package pro.savel.kafka.admin;

import org.apache.kafka.clients.admin.*;
import org.apache.kafka.common.ClassicGroupState;
import org.apache.kafka.common.GroupState;
import org.apache.kafka.common.GroupType;
import org.apache.kafka.common.acl.AclOperation;

import pro.savel.kafka.common.contract.TopicPartition;

import java.util.*;

public class AdminResponseMapper {

    public static Set<String> mapAclOperations(Set<AclOperation> source) {
        if (source == null)
            return null;
        var result = new HashSet<String>(source.size());
        source.forEach(aclOperation -> result.add(mapAclOperation(aclOperation)));
        return result;
    }

    public static String mapAclOperation(AclOperation source) {
        if (source == null)
            return null;
        return source.name();
    }

    public static String mapGroupType(GroupType source) {
        if (source == null)
            return null;
        return source.name();
    }

    public static String mapGroupState(GroupState source) {
        if (source == null)
            return null;
        return source.name();
    }

    public static String mapGroupState(ClassicGroupState source) {
        if (source == null)
            return null;
        return source.name();
    }

    public static Collection<TopicPartition> mapMemberAssignment(ShareMemberAssignment source) {
        if (source == null)
            return null;
        return TopicPartition.of(source.topicPartitions());
    }
}
