package mil.nga.giat.geowave.core.store.adapter.statistics;

import mil.nga.giat.geowave.core.index.ByteArrayId;
import mil.nga.giat.geowave.core.index.Mergeable;
import mil.nga.giat.geowave.core.store.callback.IngestCallback;
import mil.nga.giat.geowave.core.store.entities.GeoWaveRow;

public interface DataStatistics<T> extends
		Mergeable,
		IngestCallback<T, GeoWaveRow>
{
	public ByteArrayId getDataAdapterId();

	public void setDataAdapterId(
			ByteArrayId dataAdapterId );

	public ByteArrayId getStatisticsId();

	public void setVisibility(
			byte[] visibility );

	public byte[] getVisibility();
}
