package mil.nga.giat.geowave.core.store.query;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import mil.nga.giat.geowave.core.index.ByteArrayId;
import mil.nga.giat.geowave.core.index.InsertionIds;
import mil.nga.giat.geowave.core.index.NumericIndexStrategy;
import mil.nga.giat.geowave.core.index.SinglePartitionInsertionIds;
import mil.nga.giat.geowave.core.index.sfc.data.MultiDimensionalNumericData;
import mil.nga.giat.geowave.core.store.filter.QueryFilter;
import mil.nga.giat.geowave.core.store.filter.RowIdQueryFilter;
import mil.nga.giat.geowave.core.store.index.CommonIndexModel;
import mil.nga.giat.geowave.core.store.index.Index;

public class RowIdQuery implements
		Query
{
	private final InsertionIds rowIds;

	public RowIdQuery(
			final ByteArrayId partitionKey,
			final ByteArrayId sortKey ) {
		rowIds = new InsertionIds(
						new SinglePartitionInsertionIds(
								partitionKey,
								sortKey));
	}

	public RowIdQuery(
			final List<SinglePartitionInsertionIds> rowIds ) {
		this.rowIds = new InsertionIds(
				rowIds);
	}

	public RowIdQuery(
			final InsertionIds rowIds ) {
		this.rowIds = rowIds;
	}

	public InsertionIds getRowIds() {
		return rowIds;
	}

	@Override
	public List<QueryFilter> createFilters(
			final CommonIndexModel indexModel ) {
		final List<QueryFilter> filters = new ArrayList<QueryFilter>();
		filters.add(
				new RowIdQueryFilter(
						rowIds));
		return filters;
	}

	@Override
	public boolean isSupported(
			final Index index ) {
		return true;
	}

	@Override
	public List<MultiDimensionalNumericData> getIndexConstraints(
			final NumericIndexStrategy indexStrategy ) {
		return Collections.emptyList();
	}

}
