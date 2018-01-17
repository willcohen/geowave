package mil.nga.giat.geowave.datastore.dynamodb.mapreduce;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.TreeSet;

import org.apache.log4j.Logger;

import mil.nga.giat.geowave.core.index.ByteArrayId;
import mil.nga.giat.geowave.core.index.ByteArrayRange;
import mil.nga.giat.geowave.core.index.NumericIndexStrategy;
import mil.nga.giat.geowave.core.index.sfc.data.MultiDimensionalNumericData;
import mil.nga.giat.geowave.core.store.adapter.AdapterStore;
import mil.nga.giat.geowave.core.store.adapter.DataAdapter;
import mil.nga.giat.geowave.core.store.adapter.statistics.DataStatisticsStore;
import mil.nga.giat.geowave.core.store.adapter.statistics.RowRangeHistogramStatistics;
import mil.nga.giat.geowave.core.store.index.PrimaryIndex;
import mil.nga.giat.geowave.core.store.operations.DataStoreOperations;
import mil.nga.giat.geowave.core.store.query.DistributableQuery;
import mil.nga.giat.geowave.core.store.util.DataStoreUtils;
import mil.nga.giat.geowave.datastore.dynamodb.operations.DynamoDBOperations;
import mil.nga.giat.geowave.mapreduce.splits.GeoWaveRowRange;
import mil.nga.giat.geowave.mapreduce.splits.IntermediateSplitInfo;
import mil.nga.giat.geowave.mapreduce.splits.RangeLocationPair;
import mil.nga.giat.geowave.mapreduce.splits.SplitInfo;
import mil.nga.giat.geowave.mapreduce.splits.SplitsProvider;

public class DynamoDBSplitsProvider extends
		SplitsProvider
{
	private final static Logger LOGGER = Logger.getLogger(DynamoDBSplitsProvider.class);

	@Override
	protected TreeSet<IntermediateSplitInfo> populateIntermediateSplits(
			TreeSet<IntermediateSplitInfo> splits,
			DataStoreOperations operations,
			PrimaryIndex index,
			List<DataAdapter<Object>> adapters,
			Map<PrimaryIndex, RowRangeHistogramStatistics<?>> statsCache,
			AdapterStore adapterStore,
			DataStatisticsStore statsStore,
			Integer maxSplits,
			DistributableQuery query,
			String[] authorizations )
			throws IOException {

		DynamoDBOperations dynamoDBOperations = null;
		if (operations instanceof DynamoDBOperations) {
			dynamoDBOperations = (DynamoDBOperations) operations;
		}
		else {
			LOGGER.error("DynamoDBSplitsProvider requires DynamoDBOperations object.");
			return splits;
		}

		if ((query != null) && !query.isSupported(index)) {
			return splits;
		}

		final NumericIndexStrategy indexStrategy = index.getIndexStrategy();
		final int partitionKeyLength = indexStrategy.getPartitionKeyLength();

		final String tableName = dynamoDBOperations.getQualifiedTableName(index.getId().getString());

		// Build list of row ranges from query
		List<ByteArrayRange> ranges = null;
		if (query != null) {
			final List<MultiDimensionalNumericData> indexConstraints = query.getIndexConstraints(indexStrategy);
			if ((maxSplits != null) && (maxSplits > 0)) {
				ranges = DataStoreUtils.constraintsToQueryRanges(
						indexConstraints,
						indexStrategy,
						maxSplits).getCompositeQueryRanges();
			}
			else {
				ranges = DataStoreUtils.constraintsToQueryRanges(
						indexConstraints,
						indexStrategy,
						-1).getCompositeQueryRanges();
			}
		}

		if (ranges == null) {
			return splits;
		}

		final Map<ByteArrayId, SplitInfo> splitInfo = new HashMap<ByteArrayId, SplitInfo>();
		final List<RangeLocationPair> rangeList = new ArrayList<RangeLocationPair>();
		for (final ByteArrayRange range : ranges) {
			GeoWaveRowRange gwRange = SplitsProvider.toRowRange(
					range,
					partitionKeyLength);

			final double cardinality = getCardinality(
					getHistStats(
							index,
							adapters,
							adapterStore,
							statsStore,
							statsCache,
							authorizations),
					gwRange,
					index.getIndexStrategy().getPartitionKeyLength());

			rangeList.add(new RangeLocationPair(
					gwRange,
					tableName,
					cardinality < 1 ? 1.0 : cardinality));
		}

		if (!rangeList.isEmpty()) {
			splitInfo.put(
					index.getId(),
					new SplitInfo(
							index,
							rangeList));
			splits.add(new IntermediateSplitInfo(
					splitInfo,
					this));
		}

		return splits;
	}
}
