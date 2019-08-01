package pro.savel.krp.objects;

import java.util.Collection;
import java.util.Collections;

public class Topic {

	public final String name;
	public final Collection<Partition> partitions;

	public Topic(String name, Collection<Partition> partitions) {
		this.name = name;
		this.partitions = Collections.unmodifiableCollection(partitions);
	}

	public static Partition createPartiton(int name, Long beginningOffset, Long endOffset) {
		return new Partition(name, beginningOffset, endOffset);
	}

	public static class Partition {
		public final int name;
		public final Long beginningOffset, endOffset;

		public Partition(int name, Long beginningOffset, Long endOffset) {
			this.name = name;
			this.beginningOffset = beginningOffset;
			this.endOffset = endOffset;
		}
	}
}
