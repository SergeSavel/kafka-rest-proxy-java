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

package pro.savel.kafka.common;

import pro.savel.kafka.common.exceptions.BadRequestException;
import pro.savel.kafka.common.exceptions.NotFoundException;

import java.util.Collection;
import java.util.concurrent.ConcurrentHashMap;

public abstract class ClientProvider<Wrapper extends ClientWrapper> implements AutoCloseable {

    private final ConcurrentHashMap<String, Wrapper> wrappers = new ConcurrentHashMap<>();
    private final ClientKiller<Wrapper> killer = new ClientKiller<>(this);

    @Override
    public void close() {
        wrappers.forEach((uuid, wrapper) -> wrapper.close());
        wrappers.clear();
    }

    public Collection<Wrapper> getItems() {
        return wrappers.values();
    }

    protected void addItem(Wrapper wrapper) {
        wrappers.put(wrapper.getId(), wrapper);
    }

    public Wrapper getItem(String id) throws NotFoundException {
        var wrapper = wrappers.get(id);
        if (wrapper == null)
            throw new NotFoundException("Client not found.", null);
        return wrapper;
    }

    public Wrapper getItem(String id, String token) throws NotFoundException, BadRequestException {
        var wrapper = getItem(id);
        if (!wrapper.getToken().equals(token))
            throw new BadRequestException("Invalid token.", null);
        return wrapper;
    }

    public void removeItem(String id) {
        var wrapper = wrappers.remove(id);
        if (wrapper != null)
            wrapper.close();
    }

    public void removeItem(String id, String token) throws BadRequestException {
        var wrapper = wrappers.get(id);
        if (wrapper == null)
            return;
        if (!token.equals(wrapper.getToken()))
            throw new BadRequestException("Invalid token.", null);
        wrapper = wrappers.remove(id);
        if (wrapper != null)
            wrapper.close();
    }
}
