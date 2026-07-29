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

package pro.savel.kafka.consumer.responses;

import lombok.Getter;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Collection;

public class ConsumerPollStringResponse extends ArrayList<ConsumerPollStringResponse.Message> implements ConsumerResponse {

    private ConsumerPollStringResponse(int initialCapacity) {
        super(initialCapacity);
    }

    public static ConsumerPollStringResponse of(ConsumerPollResponse source) {
        if (source == null)
            return null;
        var result = new ConsumerPollStringResponse(source.size());
        source.forEach(item -> result.add(Message.of(item)));
        return result;
    }

    @Getter
    public static class Message {

        private long timestamp;
        private String topic;
        private int partition;
        private long offset;
        private Collection<Header> headers;
        private String key;
        private String value;

        private Message() {
        }

        private static Message of(ConsumerPollResponse.Message source) {
            if (source == null)
                return null;
            var result = new Message();
            result.timestamp = source.getTimestamp();
            result.topic = source.getTopic();
            result.partition = source.getPartition();
            result.offset = source.getOffset();
            result.headers = Header.of(source.getHeaders());
            result.key = toUtf8(source.getKey());
            result.value = toUtf8(source.getValue());
            return result;
        }

        private static String toUtf8(byte[] source) {
            if (source == null)
                return null;
            return new String(source, StandardCharsets.UTF_8);
        }

        @Getter
        public static class Header {

            private String key;
            private String value;

            private Header() {
            }

            private static Header of(ConsumerPollResponse.Message.Header source) {
                if (source == null)
                    return null;
                var result = new Header();
                result.key = source.getKey();
                result.value = toUtf8(source.getValue());
                return result;
            }

            private static Collection<Header> of(Collection<ConsumerPollResponse.Message.Header> source) {
                if (source == null)
                    return null;
                var result = new ArrayList<Header>(source.size());
                source.forEach(item -> result.add(Header.of(item)));
                return result;
            }
        }
    }
}
