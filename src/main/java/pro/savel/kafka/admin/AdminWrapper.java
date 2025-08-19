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

import lombok.EqualsAndHashCode;
import lombok.Getter;
import org.apache.kafka.clients.admin.Admin;
import pro.savel.kafka.common.ClientWrapper;

import java.util.Properties;
import java.util.UUID;

@Getter
@EqualsAndHashCode(callSuper = false)
public class AdminWrapper extends ClientWrapper {

    private final Admin admin;
    private final String token = UUID.randomUUID().toString();

    protected AdminWrapper(String name, Properties config, int expirationTimeout) {
        super(UUID.randomUUID().toString(), name, config, expirationTimeout);
        admin = Admin.create(config);
    }

    @Override
    public void close() {
        admin.close();
    }
}
