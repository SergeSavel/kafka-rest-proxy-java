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

import pro.savel.kafka.common.ClientProvider;

import java.util.Properties;

public class AdminProvider extends ClientProvider<AdminWrapper> {
    public AdminWrapper createAdmin(String name, Properties config, int expirationTimeout) {
        var wrapper = new AdminWrapper(name, config, expirationTimeout);
        addItem(wrapper);
        return wrapper;
    }
}
