package mil.nga.giat.geowave.mapreduce.splits;

import java.util.List;

import org.apache.commons.lang3.tuple.Pair;

import mil.nga.giat.geowave.core.index.ByteArrayId;
import mil.nga.giat.geowave.core.store.adapter.DataAdapter;
import mil.nga.giat.geowave.core.store.index.PrimaryIndex;
import mil.nga.giat.geowave.core.store.query.aggregate.Aggregation;

public class RecordReaderParams
{
	private final PrimaryIndex index;
	private final List<ByteArrayId> adapterIds;
	private final double[] maxResolutionSubsamplingPerDimension;
	private final Pair<DataAdapter<?>, Aggregation<?, ?, ?>> aggregation;
	private final Pair<List<String>, DataAdapter<?>> fieldSubsets;
	private final boolean isMixedVisibility;
	private final GeoWaveRowRange rowRange;
	private final Integer limit;
	private final String[] additionalAuthorizations;

	public RecordReaderParams(
			final PrimaryIndex index,
			final List<ByteArrayId> adapterIds,
			final double[] maxResolutionSubsamplingPerDimension,
			final Pair<DataAdapter<?>, Aggregation<?, ?, ?>> aggregation,
			final Pair<List<String>, DataAdapter<?>> fieldSubsets,
			final boolean isMixedVisibility,
			final GeoWaveRowRange rowRange,
			final Integer limit,
			final String... additionalAuthorizations ) {
		this.index = index;
		this.adapterIds = adapterIds;
		this.maxResolutionSubsamplingPerDimension = maxResolutionSubsamplingPerDimension;
		this.aggregation = aggregation;
		this.fieldSubsets = fieldSubsets;
		this.isMixedVisibility = isMixedVisibility;
		this.rowRange = rowRange;
		this.limit = limit;
		this.additionalAuthorizations = additionalAuthorizations;
	}

	public PrimaryIndex getIndex() {
		return index;
	}

	public List<ByteArrayId> getAdapterIds() {
		return adapterIds;
	}

	public double[] getMaxResolutionSubsamplingPerDimension() {
		return maxResolutionSubsamplingPerDimension;
	}

	public Pair<DataAdapter<?>, Aggregation<?, ?, ?>> getAggregation() {
		return aggregation;
	}

	public Pair<List<String>, DataAdapter<?>> getFieldSubsets() {
		return fieldSubsets;
	}

	public boolean isMixedVisibility() {
		return isMixedVisibility;
	}

	public GeoWaveRowRange getRowRange() {
		return rowRange;
	}

	public boolean isAggregation() {
		return ((aggregation != null) && (aggregation.getRight() != null));
	}

	public Integer getLimit() {
		return limit;
	}

	public String[] getAdditionalAuthorizations() {
		return additionalAuthorizations;
	}
}
