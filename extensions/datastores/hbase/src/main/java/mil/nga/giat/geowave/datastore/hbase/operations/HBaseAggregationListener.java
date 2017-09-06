package mil.nga.giat.geowave.datastore.hbase.operations;

import mil.nga.giat.geowave.core.index.ByteArrayId;
import mil.nga.giat.geowave.core.index.Mergeable;

public interface HBaseAggregationListener
{
	public void aggregationUpdate(
			ByteArrayId regionId,
			Mergeable value );
}
