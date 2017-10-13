/**
 *
 */
package mil.nga.giat.geowave.datastore.hbase;

import java.io.IOException;
import java.util.List;

import org.apache.hadoop.mapreduce.InputSplit;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import mil.nga.giat.geowave.core.store.adapter.AdapterIndexMappingStore;
import mil.nga.giat.geowave.core.store.adapter.AdapterStore;
import mil.nga.giat.geowave.core.store.adapter.DataAdapter;
import mil.nga.giat.geowave.core.store.adapter.RowMergingDataAdapter;
import mil.nga.giat.geowave.core.store.adapter.statistics.DataStatisticsStore;
import mil.nga.giat.geowave.core.store.index.IndexStore;
import mil.nga.giat.geowave.core.store.index.PrimaryIndex;
import mil.nga.giat.geowave.core.store.metadata.AdapterIndexMappingStoreImpl;
import mil.nga.giat.geowave.core.store.metadata.AdapterStoreImpl;
import mil.nga.giat.geowave.core.store.metadata.DataStatisticsStoreImpl;
import mil.nga.giat.geowave.core.store.metadata.IndexStoreImpl;
import mil.nga.giat.geowave.core.store.query.DistributableQuery;
import mil.nga.giat.geowave.core.store.query.QueryOptions;
import mil.nga.giat.geowave.datastore.hbase.cli.config.HBaseOptions;
import mil.nga.giat.geowave.datastore.hbase.coprocessors.MergingRegionObserver;
import mil.nga.giat.geowave.datastore.hbase.index.secondary.HBaseSecondaryIndexDataStore;
import mil.nga.giat.geowave.datastore.hbase.mapreduce.HBaseSplitsProvider;
import mil.nga.giat.geowave.datastore.hbase.operations.HBaseOperations;
import mil.nga.giat.geowave.mapreduce.BaseMapReduceDataStore;

public class HBaseDataStore extends
		BaseMapReduceDataStore
{
	private final static Logger LOGGER = LoggerFactory.getLogger(HBaseDataStore.class);

	private final HBaseSplitsProvider splitsProvider = new HBaseSplitsProvider();
	private final HBaseOperations hbaseOperations;

	public HBaseDataStore(
			final HBaseOperations operations,
			final HBaseOptions options ) {
		this(
				new IndexStoreImpl(
						operations,
						options),
				new AdapterStoreImpl(
						operations,
						options),
				new DataStatisticsStoreImpl(
						operations,
						options),
				new AdapterIndexMappingStoreImpl(
						operations,
						options),
				new HBaseSecondaryIndexDataStore(
						operations,
						options),
				operations,
				options);
	}

	public HBaseDataStore(
			final IndexStore indexStore,
			final AdapterStore adapterStore,
			final DataStatisticsStore statisticsStore,
			final AdapterIndexMappingStore indexMappingStore,
			final HBaseSecondaryIndexDataStore secondaryIndexDataStore,
			final HBaseOperations operations,
			final HBaseOptions options ) {
		super(
				indexStore,
				adapterStore,
				statisticsStore,
				indexMappingStore,
				secondaryIndexDataStore,
				operations,
				options);

		secondaryIndexDataStore.setDataStore(this);

		hbaseOperations = operations;
	}

	@Override
	protected void initOnIndexWriterCreate(
			final DataAdapter adapter,
			final PrimaryIndex index ) {
		if (adapter instanceof RowMergingDataAdapter) {
			hbaseOperations.stageMergingAdapterForObserver(
					index.getId(),
					adapter.getAdapterId());
		}
	}

	@Override
	public List<InputSplit> getSplits(
			final DistributableQuery query,
			final QueryOptions queryOptions,
			final AdapterStore adapterStore,
			final DataStatisticsStore statsStore,
			final IndexStore indexStore,
			final Integer minSplits,
			final Integer maxSplits )
			throws IOException,
			InterruptedException {
		return splitsProvider.getSplits(
				baseOperations,
				query,
				queryOptions,
				adapterStore,
				statsStore,
				indexStore,
				indexMappingStore,
				minSplits,
				maxSplits);
	}
}
