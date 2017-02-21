package mil.nga.giat.geowave.core.store.filter;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;

import mil.nga.giat.geowave.core.index.ByteArrayId;
import mil.nga.giat.geowave.core.index.InsertionIds;
import mil.nga.giat.geowave.core.index.SinglePartitionInsertionIds;
import mil.nga.giat.geowave.core.store.data.IndexedPersistenceEncoding;
import mil.nga.giat.geowave.core.store.index.CommonIndexModel;

public class RowIdQueryFilter implements
		DistributableQueryFilter
{
	private InsertionIds rowIds;

	protected RowIdQueryFilter() {}

	public RowIdQueryFilter(
			final List<SinglePartitionInsertionIds> rowIds ) {
		this.rowIds = new InsertionIds(
				rowIds);
	}

	public RowIdQueryFilter(
			final InsertionIds rowIds ) {
		this.rowIds = rowIds;
	}

	@Override
	public boolean accept(
			final CommonIndexModel indexModel,
			final IndexedPersistenceEncoding persistenceEncoding ) {
		return rowIds.contains(
				persistenceEncoding.getInsertionPartitionKey(),
				persistenceEncoding.getInsertionSortKey());
	}

	@Override
	public byte[] toBinary() {
		return rowIds.toBinary();
	}

	@Override
	public void fromBinary(
			final byte[] bytes ) {
		rowIds = new InsertionIds();
		rowIds.fromBinary(
				bytes);
	}

}
