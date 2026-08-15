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

package pro.savel.kafka.admin;

import org.apache.kafka.common.ClassicGroupState;
import org.apache.kafka.common.GroupState;
import org.apache.kafka.common.GroupType;
import org.apache.kafka.common.Uuid;
import org.apache.kafka.common.acl.AclOperation;
import org.junit.jupiter.api.Test;

import java.util.Set;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;

class AdminResponseMapperTest {

    @Test
    void mapUuid_null_returnsNull() {
        assertNull(AdminResponseMapper.mapUuid(null));
    }

    @Test
    void mapUuid_value_returnsString() {
        var uuid = Uuid.randomUuid();
        assertEquals(uuid.toString(), AdminResponseMapper.mapUuid(uuid));
    }

    @Test
    void mapAclOperations_null_returnsNull() {
        assertNull(AdminResponseMapper.mapAclOperations(null));
    }

    @Test
    void mapAclOperations_set_returnsNames() {
        var result = AdminResponseMapper.mapAclOperations(Set.of(AclOperation.READ, AclOperation.WRITE));
        assertEquals(Set.of("READ", "WRITE"), result);
    }

    @Test
    void mapAclOperation_null_returnsNull() {
        assertNull(AdminResponseMapper.mapAclOperation(null));
    }

    @Test
    void mapAclOperation_value_returnsName() {
        assertEquals("DESCRIBE", AdminResponseMapper.mapAclOperation(AclOperation.DESCRIBE));
    }

    @Test
    void mapAclOperation_unknown_returnsUnknownName() {
        assertEquals("UNKNOWN", AdminResponseMapper.mapAclOperation(AclOperation.UNKNOWN));
    }

    @Test
    void mapGroupType_null_returnsNull() {
        assertNull(AdminResponseMapper.mapGroupType(null));
    }

    @Test
    void mapGroupType_value_returnsName() {
        assertEquals("CONSUMER", AdminResponseMapper.mapGroupType(GroupType.CONSUMER));
    }

    @Test
    void mapGroupState_null_returnsNull() {
        assertNull(AdminResponseMapper.mapGroupState((GroupState) null));
        assertNull(AdminResponseMapper.mapGroupState((ClassicGroupState) null));
    }

    @Test
    void mapGroupState_value_returnsName() {
        assertEquals("STABLE", AdminResponseMapper.mapGroupState(GroupState.STABLE));
    }

    @Test
    void mapClassicGroupState_value_returnsName() {
        assertEquals("DEAD", AdminResponseMapper.mapGroupState(ClassicGroupState.DEAD));
    }
}
