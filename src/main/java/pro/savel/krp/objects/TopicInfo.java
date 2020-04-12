// Copyright 2019-2020 Sergey Savelev
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package pro.savel.krp.objects;

import java.util.Collection;
import java.util.Collections;

public class TopicInfo {

    public final String name;
    public final Collection<PartitionInfo> partitions;

    public TopicInfo(String name, Collection<PartitionInfo> partitions) {
        this.name = name;
        this.partitions = Collections.unmodifiableCollection(partitions);
    }

    public static PartitionInfo createPartiton(int name, Long beginningOffset, Long endOffset) {
        return new PartitionInfo(name, beginningOffset, endOffset);
    }

    public static class PartitionInfo {
        public final int name;
        public final Long beginningOffset, endOffset;

        public PartitionInfo(int name, Long beginningOffset, Long endOffset) {
            this.name = name;
            this.beginningOffset = beginningOffset;
            this.endOffset = endOffset;
        }
    }
}
