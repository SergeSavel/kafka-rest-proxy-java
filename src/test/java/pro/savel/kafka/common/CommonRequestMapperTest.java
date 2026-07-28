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

package pro.savel.kafka.common;

import org.apache.kafka.common.TopicPartition;
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

class CommonRequestMapperTest {

    @Test
    void mapPartitions_null_returnsNull() {
        assertNull(CommonRequestMapper.mapPartitions(null));
    }

    @Test
    void mapPartitions_emptyCollection_returnsEmptySet() {
        var result = CommonRequestMapper.mapPartitions(List.of());
        assertNotNull(result);
        assertTrue(result.isEmpty());
    }

    @Test
    void mapPartitions_validCollection_returnsMappedSet() {
        var dto = pro.savel.kafka.common.contract.TopicPartition.of(new TopicPartition("test-topic", 3));
        var result = CommonRequestMapper.mapPartitions(List.of(dto));
        assertEquals(1, result.size());
        var tp = result.iterator().next();
        assertEquals("test-topic", tp.topic());
        assertEquals(3, tp.partition());
    }

    @Test
    void mapTopicPartition_null_returnsNull() {
        assertNull(CommonRequestMapper.mapTopicPartition(null));
    }

    @Test
    void mapTopicPartition_valid_returnsKafkaTopicPartition() {
        var dto = pro.savel.kafka.common.contract.TopicPartition.of(new TopicPartition("my-topic", 7));
        var result = CommonRequestMapper.mapTopicPartition(dto);
        assertEquals("my-topic", result.topic());
        assertEquals(7, result.partition());
    }
}
