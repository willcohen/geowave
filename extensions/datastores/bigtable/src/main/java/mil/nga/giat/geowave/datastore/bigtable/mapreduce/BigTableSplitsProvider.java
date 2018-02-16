package mil.nga.giat.geowave.datastore.bigtable.mapreduce;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.TreeSet;

import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.HRegionInfo;
import org.apache.hadoop.hbase.HRegionLocation;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.cloud.bigtable.hbase.BigtableRegionLocator;

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
import mil.nga.giat.geowave.datastore.bigtable.operations.BigTableOperations;
import mil.nga.giat.geowave.datastore.hbase.mapreduce.HBaseSplitsProvider;
import mil.nga.giat.geowave.datastore.hbase.operations.HBaseOperations;
import mil.nga.giat.geowave.mapreduce.splits.GeoWaveRowRange;
import mil.nga.giat.geowave.mapreduce.splits.IntermediateSplitInfo;
import mil.nga.giat.geowave.mapreduce.splits.RangeLocationPair;
import mil.nga.giat.geowave.mapreduce.splits.SplitInfo;

public class BigTableSplitsProvider extends
		HBaseSplitsProvider
{
	private final static Logger LOGGER = LoggerFactory.getLogger(BigTableSplitsProvider.class);

	@Override
	protected TreeSet<IntermediateSplitInfo> populateIntermediateSplits(
			final TreeSet<IntermediateSplitInfo> splits,
			final DataStoreOperations operations,
			final PrimaryIndex index,
			final List<DataAdapter<Object>> adapters,
			final Map<PrimaryIndex, RowRangeHistogramStatistics<?>> statsCache,
			final AdapterStore adapterStore,
			final DataStatisticsStore statsStore,
			final Integer maxSplits,
			final DistributableQuery query,
			final String[] authorizations )
			throws IOException {

		BigTableOperations bigtableOperations = null;
		if (operations instanceof HBaseOperations) {
			bigtableOperations = (BigTableOperations) operations;
		}
		else {
			LOGGER.error("BigTableSplitsProvider requires BigTableOperations object.");
			return splits;
		}

		if ((query != null) && !query.isSupported(index)) {
			return splits;
		}

		final NumericIndexStrategy indexStrategy = index.getIndexStrategy();
		final int partitionKeyLength = indexStrategy.getPartitionKeyLength();

		final String tableName = bigtableOperations.getQualifiedTableName(index.getId().getString());

		// Build list of row ranges from query
		List<ByteArrayRange> ranges = null;
		if (query != null) {
			final List<MultiDimensionalNumericData> indexConstraints = query.getIndexConstraints(index);
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

		final Map<HRegionLocation, Map<HRegionInfo, List<ByteArrayRange>>> binnedRanges = new HashMap<HRegionLocation, Map<HRegionInfo, List<ByteArrayRange>>>();
		final BigtableRegionLocator regionLocator = (BigtableRegionLocator) bigtableOperations
				.getRegionLocator(tableName);

		if (regionLocator == null) {
			LOGGER.error("Unable to retrieve RegionLocator for " + tableName);
			return splits;
		}

		RowRangeHistogramStatistics<?> stats = getHistStats(
				index,
				adapters,
				adapterStore,
				statsStore,
				statsCache,
				authorizations);

		if (ranges == null) { // get partition ranges from stats
			if (stats != null) {
				ranges = new ArrayList();

				ByteArrayId prevKey = new ByteArrayId(
						HConstants.EMPTY_BYTE_ARRAY);

				for (ByteArrayId partitionKey : stats.getPartitionKeys()) {
					ByteArrayRange range = new ByteArrayRange(
							prevKey,
							partitionKey);

					ranges.add(range);

					prevKey = partitionKey;
				}

				ranges.add(new ByteArrayRange(
						prevKey,
						new ByteArrayId(
								HConstants.EMPTY_BYTE_ARRAY)));

				binRanges(
						ranges,
						binnedRanges,
						regionLocator);
			}
			else {
				binFullRange(
						binnedRanges,
						regionLocator);
			}

		}
		else {
			while (!ranges.isEmpty()) {
				ranges = binRanges(
						ranges,
						binnedRanges,
						regionLocator);
			}
		}

		for (final Entry<HRegionLocation, Map<HRegionInfo, List<ByteArrayRange>>> locationEntry : binnedRanges
				.entrySet()) {
			final String hostname = locationEntry.getKey().getHostname();

			for (final Entry<HRegionInfo, List<ByteArrayRange>> regionEntry : locationEntry.getValue().entrySet()) {
				final Map<ByteArrayId, SplitInfo> splitInfo = new HashMap<ByteArrayId, SplitInfo>();
				final List<RangeLocationPair> rangeList = new ArrayList<RangeLocationPair>();

				for (final ByteArrayRange range : regionEntry.getValue()) {
					GeoWaveRowRange gwRange = fromHBaseRange(
							range,
							partitionKeyLength);

					final double cardinality = getCardinality(
							stats,
							gwRange,
							index.getIndexStrategy().getPartitionKeyLength());

					rangeList.add(new RangeLocationPair(
							gwRange,
							hostname,
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
			}
		}

		return splits;
	}
}
