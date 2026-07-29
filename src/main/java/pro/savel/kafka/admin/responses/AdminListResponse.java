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
import pro.savel.kafka.admin.AdminWrapper;

import java.util.ArrayList;
import java.util.Collection;

public class AdminListResponse extends ArrayList<AdminListResponse.AdminListing> implements AdminResponse {

    private AdminListResponse(int initialCapacity) {
        super(initialCapacity);
    }

    public static AdminListResponse of(Collection<AdminWrapper> source) {
        if (source == null)
            return null;
        var result = new AdminListResponse(source.size());
        source.forEach(wrapper -> result.add(AdminListing.of(wrapper)));
        return result;
    }

    @Getter
    public static class AdminListing {

        private String id;
        private String name;
        private String owner;
        private String username;
        private long expiresAt;

        private AdminListing() {
        }

        private static AdminListing of(AdminWrapper source) {
            if (source == null)
                return null;
            var result = new AdminListing();
            result.id = source.getId();
            result.name = source.getName();
            result.owner = source.getOwner();
            result.username = source.getUsername();
            result.expiresAt = source.getExpiresAt();
            return result;
        }
    }
}
