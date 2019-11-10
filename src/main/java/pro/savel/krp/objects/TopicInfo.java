package pro.savel.krp.objects;

import javax.xml.bind.annotation.XmlElement;
import javax.xml.bind.annotation.XmlElementWrapper;
import javax.xml.bind.annotation.XmlRootElement;
import javax.xml.bind.annotation.XmlType;
import java.util.Collection;
import java.util.Collections;

@XmlRootElement
public class TopicInfo {

	private String name;
	private Collection<PartitionInfo> partitions;

	public TopicInfo() {
	}

	public TopicInfo(String name, Collection<PartitionInfo> partitions) {
		this.name = name;
		this.partitions = Collections.unmodifiableCollection(partitions);
	}

	public static PartitionInfo createPartiton(int name, Long beginningOffset, Long endOffset) {
		return new PartitionInfo(name, beginningOffset, endOffset);
	}

	@XmlElement
	public String getName() {
		return name;
	}

	@XmlElementWrapper
	public Collection<PartitionInfo> getPartitions() {
		return partitions;
	}

	@XmlType(name = "PartitionInfo")
	public static class PartitionInfo {
		private int name;
		private Long beginningOffset, endOffset;

		public PartitionInfo() {
		}

		PartitionInfo(int name, Long beginningOffset, Long endOffset) {
			this.name = name;
			this.beginningOffset = beginningOffset;
			this.endOffset = endOffset;
		}

		@XmlElement
		public int getName() {
			return name;
		}

		@XmlElement
		public Long getBeginningOffset() {
			return beginningOffset;
		}

		@XmlElement
		public Long getEndOffset() {
			return endOffset;
		}
	}
}
