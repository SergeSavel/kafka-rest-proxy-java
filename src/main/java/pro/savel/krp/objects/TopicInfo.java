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

	public Collection<PartitionInfo> getPartitions() {
		return partitions;
	}

	public static class PartitionInfo {
		public final int name;
		public final Long beginningOffset, endOffset;

		PartitionInfo(int name, Long beginningOffset, Long endOffset) {
			this.name = name;
			this.beginningOffset = beginningOffset;
			this.endOffset = endOffset;
		}
	}
}
