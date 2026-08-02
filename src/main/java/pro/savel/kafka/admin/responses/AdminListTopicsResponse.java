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
import org.apache.kafka.common.Uuid;

import java.util.ArrayList;
import java.util.Collection;

public class AdminListTopicsResponse extends ArrayList<AdminListTopicsResponse.TopicListing> implements AdminResponse {

    private AdminListTopicsResponse(int initialCapacity) {
        super(initialCapacity);
    }

    public static AdminListTopicsResponse of(Collection<org.apache.kafka.clients.admin.TopicListing> source) {
        if (source == null)
            return null;
        var result = new AdminListTopicsResponse(source.size());
        source.forEach(topicListingSource -> result.add(TopicListing.of(topicListingSource)));
        return result;
    }

    @Getter
    public static class TopicListing {
        private Uuid id;
        private String name;
        private boolean isInternal;

        private TopicListing() {
        }

        public static TopicListing of(org.apache.kafka.clients.admin.TopicListing source) {
            if (source == null)
                return null;
            var result = new TopicListing();
            result.id = source.topicId();
            result.name = source.name();
            result.isInternal = source.isInternal();
            return result;
        }
    }
}
