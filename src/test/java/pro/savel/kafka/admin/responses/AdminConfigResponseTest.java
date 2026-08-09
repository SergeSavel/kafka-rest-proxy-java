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

package pro.savel.kafka.admin.responses;

import org.apache.kafka.clients.admin.Config;
import org.apache.kafka.clients.admin.ConfigEntry;
import org.junit.jupiter.api.Test;

import java.util.Collections;
import java.util.Set;

import static org.junit.jupiter.api.Assertions.*;

class AdminConfigResponseTest {

//region of

    @Test
    void of_configEntryWithNullTypeAndDocumentation_mapsWithoutError() {
        var entry = new ConfigEntry("cleanup.policy", "delete",
                ConfigEntry.ConfigSource.DYNAMIC_TOPIC_CONFIG, false, false,
                Collections.emptyList(), null, null);
        var response = AdminConfigResponse.of(new Config(Set.of(entry)));

        assertEquals(1, response.size());
        var mapped = response.get(0);
        assertEquals("cleanup.policy", mapped.getName());
        assertEquals("delete", mapped.getValue());
        assertEquals("DYNAMIC_TOPIC_CONFIG", mapped.getSource());
        assertFalse(mapped.isDefault());
        assertFalse(mapped.isSensitive());
        assertFalse(mapped.isReadOnly());
        assertNull(mapped.getType());
        assertNull(mapped.getDocumentation());
    }

    @Test
    void of_configEntryWithAllFields_mapsEnumNames() {
        var entry = new ConfigEntry("retention.ms", "604800000",
                ConfigEntry.ConfigSource.DEFAULT_CONFIG, true, true,
                Collections.emptyList(), ConfigEntry.ConfigType.LONG, "retention docs");
        var response = AdminConfigResponse.of(new Config(Set.of(entry)));

        assertEquals(1, response.size());
        var mapped = response.get(0);
        assertEquals("retention.ms", mapped.getName());
        assertEquals("604800000", mapped.getValue());
        assertEquals("DEFAULT_CONFIG", mapped.getSource());
        assertTrue(mapped.isDefault());
        assertTrue(mapped.isSensitive());
        assertTrue(mapped.isReadOnly());
        assertEquals("LONG", mapped.getType());
        assertEquals("retention docs", mapped.getDocumentation());
    }

    @Test
    void of_nullConfig_returnsNull() {
        assertNull(AdminConfigResponse.of(null));
    }

//endregion
}
