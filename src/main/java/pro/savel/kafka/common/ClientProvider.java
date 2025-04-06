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

import pro.savel.kafka.common.exceptions.InstanceNotFoundException;
import pro.savel.kafka.common.exceptions.InvalidTokenException;

import java.util.Collection;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;

public abstract class ClientProvider<Wrapper extends ClientWrapper> implements AutoCloseable {

    private final ConcurrentHashMap<UUID, Wrapper> wrappers = new ConcurrentHashMap<>();
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
        wrappers.put(wrapper.id(), wrapper);
    }

    public Wrapper getItem(UUID id) throws InstanceNotFoundException {
        Wrapper result = null;
        try {
            result = getItem(id, null);
        } catch (InvalidTokenException ignore) {
            // never thrown
        }
        return result;
    }

    public Wrapper getItem(UUID id, String token) throws InstanceNotFoundException, InvalidTokenException {
        var wrapper = wrappers.get(id);
        if (wrapper == null) throw new InstanceNotFoundException();
        if (token != null && !token.equals(wrapper.token())) throw new InvalidTokenException();
        return wrapper;
    }

    public void removeItem(UUID id) {
        var wrapper = wrappers.remove(id);
        if (wrapper != null)
            wrapper.close();
    }

    public void removeItem(UUID id, String token) throws InstanceNotFoundException, InvalidTokenException {
        var wrapper = wrappers.get(id);
        if (wrapper == null) throw new InstanceNotFoundException();
        if (!token.equals(wrapper.token())) throw new InvalidTokenException();
        wrapper = wrappers.remove(id);
        if (wrapper != null)
            wrapper.close();
    }
}
