// Copyright 2025 Sergey Savelev
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

import pro.savel.kafka.admin.responses.AdminCreateResponse;
import pro.savel.kafka.admin.responses.AdminListResponse;

import java.util.Collection;

public class AdminResponseMapper {

    public static AdminListResponse mapListResponse(Collection<AdminWrapper> source) {
        if (source == null)
            return null;
        var result = new AdminListResponse(source.size());
        source.forEach(wrapper -> result.add(mapAdmin(wrapper)));
        return result;
    }

    private static AdminListResponse.Admin mapAdmin(AdminWrapper source) {
        if (source == null)
            return null;
        var result = new AdminListResponse.Admin();
        result.setId(source.getId());
        result.setName(source.getName());
        result.setUsername(source.getUsername());
        result.setExpiresAt(source.getExpiresAt());
        return result;
    }

    public static AdminCreateResponse mapCreateResponse(AdminWrapper source) {
        if (source == null)
            return null;
        var result = new AdminCreateResponse();
        result.setId(source.getId());
        result.setToken(source.getToken());
        return result;
    }
}
