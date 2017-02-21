package mil.nga.giat.geowave.core.index;

import java.util.Collections;
import java.util.List;

public class SinglePartitionQueryRanges
{
	private final ByteArrayId partitionKey;

	private final List<ByteArrayRange> sortKeyRanges;

	public SinglePartitionQueryRanges(
			final ByteArrayId partitionKey,
			final List<ByteArrayRange> sortKeyRanges ) {
		this.partitionKey = partitionKey;
		this.sortKeyRanges = sortKeyRanges;
	}

	public SinglePartitionQueryRanges(
			final ByteArrayId partitionKey ) {
		this.partitionKey = partitionKey;
		sortKeyRanges = null;
	}

	public SinglePartitionQueryRanges(
			final List<ByteArrayRange> sortKeyRanges ) {
		this.sortKeyRanges = sortKeyRanges;
		partitionKey = null;
	}

	public SinglePartitionQueryRanges(
			final ByteArrayRange singleSortKeyRange ) {
		this.sortKeyRanges = Collections.singletonList(
				singleSortKeyRange);
		partitionKey = null;
	}

	public ByteArrayId getPartitionKey() {
		return partitionKey;
	}

	public List<ByteArrayRange> getSortKeyRanges() {
		return sortKeyRanges;
	}

	public ByteArrayRange getSingleRange() {
		ByteArrayId start = null;
		ByteArrayId end = null;

		for (final ByteArrayRange range : sortKeyRanges) {
			if ((start == null) || (range.getStart().compareTo(
					start) < 0)) {
				start = range.getStart();
			}
			if ((end == null) || (range.getEnd().compareTo(
					end) > 0)) {
				end = range.getEnd();
			}
		}
		return new ByteArrayRange(
				start,
				end);
	}
}
