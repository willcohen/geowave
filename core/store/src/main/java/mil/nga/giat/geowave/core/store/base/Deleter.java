package mil.nga.giat.geowave.core.store.base;

import mil.nga.giat.geowave.core.store.adapter.DataAdapter;
import mil.nga.giat.geowave.core.store.entities.GeoWaveRow;

public interface Deleter<R extends GeoWaveRow> extends
		AutoCloseable
{
	public void delete(
			R row,
			DataAdapter<?> adapter );
}
