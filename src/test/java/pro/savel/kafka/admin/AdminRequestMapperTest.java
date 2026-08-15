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

import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.acl.AccessControlEntry;
import org.apache.kafka.common.acl.AccessControlEntryFilter;
import org.apache.kafka.common.acl.AclBinding;
import org.apache.kafka.common.acl.AclBindingFilter;
import org.apache.kafka.common.acl.AclOperation;
import org.apache.kafka.common.acl.AclPermissionType;
import org.apache.kafka.common.resource.PatternType;
import org.apache.kafka.common.resource.ResourcePattern;
import org.apache.kafka.common.resource.ResourcePatternFilter;
import org.apache.kafka.common.resource.ResourceType;
import org.junit.jupiter.api.Test;
import pro.savel.kafka.admin.data.AdminAclBinding;
import pro.savel.kafka.admin.data.AdminAclBindingFilter;
import pro.savel.kafka.admin.requests.group.AdminAlterConsumerGroupOffsetsRequest;

import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;

class AdminRequestMapperTest {

    @Test
    void mapAclBindingFilter_null_returnsAny() {
        assertEquals(AclBindingFilter.ANY, AdminRequestMapper.mapAclBindingFilter(null));
    }

    @Test
    void mapAclBindingFilter_fullFilter_mapsAllFields() {
        var patternFilter = new AdminAclBindingFilter.ResourcePatternFilter();
        patternFilter.setResourceType("TOPIC");
        patternFilter.setName("test-topic");
        patternFilter.setPatternType("LITERAL");
        var entryFilter = new AdminAclBindingFilter.AccessControlEntryFilter();
        entryFilter.setPrincipal("User:alice");
        entryFilter.setHost("*");
        entryFilter.setOperation("READ");
        entryFilter.setPermissionType("ALLOW");
        var source = new AdminAclBindingFilter();
        source.setPatternFilter(patternFilter);
        source.setEntryFilter(entryFilter);

        var result = AdminRequestMapper.mapAclBindingFilter(source);

        var expectedPatternFilter = new ResourcePatternFilter(ResourceType.TOPIC, "test-topic", PatternType.LITERAL);
        var expectedEntryFilter = new AccessControlEntryFilter("User:alice", "*", AclOperation.READ, AclPermissionType.ALLOW);
        assertEquals(expectedPatternFilter, result.patternFilter());
        assertEquals(expectedEntryFilter, result.entryFilter());
    }

    @Test
    void mapAclBindingFilter_nullNestedFilters_mapToAny() {
        var source = new AdminAclBindingFilter();

        var result = AdminRequestMapper.mapAclBindingFilter(source);

        assertEquals(ResourcePatternFilter.ANY, result.patternFilter());
        assertEquals(AccessControlEntryFilter.ANY, result.entryFilter());
    }

    @Test
    void mapAclBindingFilter_invalidResourceType_throwsIllegalArgument() {
        var patternFilter = new AdminAclBindingFilter.ResourcePatternFilter();
        patternFilter.setResourceType("NOT_A_TYPE");
        patternFilter.setPatternType("LITERAL");
        var source = new AdminAclBindingFilter();
        source.setPatternFilter(patternFilter);

        assertThrows(IllegalArgumentException.class, () -> AdminRequestMapper.mapAclBindingFilter(source));
    }

    @Test
    void mapAclBindingFilter_invalidOperation_throwsIllegalArgument() {
        var entryFilter = new AdminAclBindingFilter.AccessControlEntryFilter();
        entryFilter.setOperation("NOT_AN_OPERATION");
        entryFilter.setPermissionType("ALLOW");
        var source = new AdminAclBindingFilter();
        source.setEntryFilter(entryFilter);

        assertThrows(IllegalArgumentException.class, () -> AdminRequestMapper.mapAclBindingFilter(source));
    }

    @Test
    void mapAclBindingFilters_null_returnsNull() {
        assertNull(AdminRequestMapper.mapAclBindingFilters(null));
    }

    @Test
    void mapAclBindingFilters_collection_mapsEachElement() {
        var source = List.of(new AdminAclBindingFilter(), new AdminAclBindingFilter());

        var result = AdminRequestMapper.mapAclBindingFilters(source);

        assertEquals(2, result.size());
        result.forEach(filter -> assertEquals(AclBindingFilter.ANY, filter));
    }

    @Test
    void mapAclBindings_null_returnsNull() {
        assertNull(AdminRequestMapper.mapAclBindings(null));
    }

    @Test
    void mapAclBindings_fullBinding_mapsAllFields() {
        var pattern = new AdminAclBinding.ResourcePattern();
        pattern.setResourceType("GROUP");
        pattern.setName("test-group");
        pattern.setPatternType("PREFIXED");
        var entry = new AdminAclBinding.AccessControlEntry();
        entry.setPrincipal("User:bob");
        entry.setHost("127.0.0.1");
        entry.setOperation("WRITE");
        entry.setPermissionType("DENY");
        var source = new AdminAclBinding();
        source.setPattern(pattern);
        source.setEntry(entry);

        var result = AdminRequestMapper.mapAclBindings(List.of(source)).iterator().next();

        var expectedPattern = new ResourcePattern(ResourceType.GROUP, "test-group", PatternType.PREFIXED);
        var expectedEntry = new AccessControlEntry("User:bob", "127.0.0.1", AclOperation.WRITE, AclPermissionType.DENY);
        assertEquals(new AclBinding(expectedPattern, expectedEntry), result);
    }

    @Test
    void mapTopicPartitionOffsetMetadata_null_returnsNull() {
        assertNull(AdminRequestMapper.mapTopicPartitionOffsetMetadata(null));
    }

    @Test
    void mapTopicPartitionOffsetMetadata_items_mappedToMap() {
        var first = new AdminAlterConsumerGroupOffsetsRequest.TopicPartitionOffsetMetadata();
        first.setTopic("topic-a");
        first.setPartition(0);
        first.setOffset(100L);
        first.setMetadata("meta-a");
        var second = new AdminAlterConsumerGroupOffsetsRequest.TopicPartitionOffsetMetadata();
        second.setTopic("topic-b");
        second.setPartition(1);
        second.setOffset(200L);
        second.setMetadata(null);

        var result = AdminRequestMapper.mapTopicPartitionOffsetMetadata(List.of(first, second));

        assertEquals(2, result.size());
        assertEquals(new OffsetAndMetadata(100L, "meta-a"), result.get(new TopicPartition("topic-a", 0)));
        assertEquals(new OffsetAndMetadata(200L, null), result.get(new TopicPartition("topic-b", 1)));
    }
}
