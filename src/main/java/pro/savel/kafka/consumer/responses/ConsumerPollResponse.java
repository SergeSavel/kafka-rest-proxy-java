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
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.common.header.Headers;

import java.util.ArrayList;
import java.util.Collection;

public class ConsumerPollResponse extends ArrayList<ConsumerPollResponse.Message> implements ConsumerResponse {

    private ConsumerPollResponse(int initialCapacity) {
        super(initialCapacity);
    }

    public static ConsumerPollResponse of(ConsumerRecords<byte[], byte[]> source) {
        if (source == null)
            return null;
        var result = new ConsumerPollResponse(source.count());
        source.forEach(record -> result.add(Message.of(record)));
        return result;
    }

    @Getter
    public static class Message {

        private long timestamp;
        private String topic;
        private int partition;
        private long offset;
        private Collection<Header> headers;
        private byte[] key;
        private byte[] value;

        private Message() {
        }

        private static Message of(ConsumerRecord<byte[], byte[]> source) {
            if (source == null)
                return null;
            var result = new Message();
            result.timestamp = source.timestamp();
            result.topic = source.topic();
            result.partition = source.partition();
            result.offset = source.offset();
            result.headers = Header.of(source.headers());
            result.key = source.key();
            result.value = source.value();
            return result;
        }

        @Getter
        public static class Header {

            private String key;
            private byte[] value;

            private Header() {
            }

            private static Header of(org.apache.kafka.common.header.Header source) {
                if (source == null)
                    return null;
                var result = new Header();
                result.key = source.key();
                result.value = source.value();
                return result;
            }

            private static Collection<Header> of(Headers source) {
                if (source == null)
                    return null;
                var result = new ArrayList<Header>();
                source.forEach(header -> result.add(Header.of(header)));
                return result;
            }
        }
    }
}
