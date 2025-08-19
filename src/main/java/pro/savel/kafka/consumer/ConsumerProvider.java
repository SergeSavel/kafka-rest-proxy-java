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

package pro.savel.kafka.consumer;

import pro.savel.kafka.common.ClientProvider;
import pro.savel.kafka.common.exceptions.BadRequestException;
import pro.savel.kafka.common.exceptions.NotFoundException;

import java.util.Properties;

public class ConsumerProvider extends ClientProvider<ConsumerWrapper> {

    public ConsumerWrapper createConsumer(String name, Properties config, int expirationTimeout) {
        var wrapper = new ConsumerWrapper(name, config, expirationTimeout);
        addItem(wrapper);
        return wrapper;
    }

    protected ConsumerWrapper getConsumer(String id, String token) throws NotFoundException, BadRequestException {
        var wrapper = getItem(id);
        if (!wrapper.getToken().equals(token))
            throw new BadRequestException("Invalid token.", null);
        return wrapper;
    }

    public void removeConsumer(String id, String token) throws BadRequestException {
        var wrapper = wrappers.get(id);
        if (wrapper == null)
            return;
        if (!token.equals(wrapper.getToken()))
            throw new BadRequestException("Invalid token.", null);
        removeItem(id);
    }
}
