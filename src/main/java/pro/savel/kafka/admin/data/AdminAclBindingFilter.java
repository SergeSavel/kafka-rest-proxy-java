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

package pro.savel.kafka.admin.data;

import lombok.Data;

@Data
public class AdminAclBindingFilter {

    private ResourcePatternFilter patternFilter;
    private AccessControlEntryFilter entryFilter;

    @Data
    public static class ResourcePatternFilter {
        private String resourceType;
        private String name;
        private String patternType;
    }

    @Data
    public static class AccessControlEntryFilter {
        private String principal;
        private String host;
        private String operation;
        private String permissionType;
    }
}
