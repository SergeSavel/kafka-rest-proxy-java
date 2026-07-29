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

import lombok.Getter;
import org.apache.kafka.clients.admin.Config;
import org.apache.kafka.clients.admin.ConfigEntry;

import java.util.ArrayList;

public class AdminConfigResponse extends ArrayList<AdminConfigResponse.Entry> implements AdminResponse {

    private AdminConfigResponse(int initialCapacity) {
        super(initialCapacity);
    }

    public static AdminConfigResponse of(Config source) {
        if (source == null)
            return null;
        var result = new AdminConfigResponse(source.entries().size());
        source.entries().forEach(entry -> result.add(Entry.of(entry)));
        return result;
    }

    @Getter
    public static class Entry {

        private String name;
        private String value;
        private String source;
        private boolean isDefault;
        private boolean isSensitive;
        private boolean isReadOnly;
        private String type;
        private String documentation;

        private Entry() {
        }

        private static Entry of(ConfigEntry source) {
            if (source == null)
                return null;
            var result = new Entry();
            result.name = source.name();
            result.value = source.value();
            result.source = source.source().name();
            result.isDefault = source.isDefault();
            result.isSensitive = source.isSensitive();
            result.isReadOnly = source.isReadOnly();
            result.type = source.type().name();
            result.documentation = source.documentation();
            return result;
        }
    }
}
