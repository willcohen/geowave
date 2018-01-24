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
import mil.nga.giat.geowave.core.store.server.ServerOpHelper;
import mil.nga.giat.geowave.core.store.server.ServerSideOperations;
import mil.nga.giat.geowave.datastore.hbase.cli.config.HBaseOptions;
import mil.nga.giat.geowave.datastore.hbase.index.secondary.HBaseSecondaryIndexDataStore;
import mil.nga.giat.geowave.datastore.hbase.mapreduce.HBaseSplitsProvider;
import mil.nga.giat.geowave.datastore.hbase.operations.HBaseOperations;
import mil.nga.giat.geowave.datastore.hbase.server.RowMergingServerOp;
import mil.nga.giat.geowave.datastore.hbase.server.RowMergingVisibilityServerOp;
import mil.nga.giat.geowave.mapreduce.BaseMapReduceDataStore;

public class HBaseDataStore extends
		BaseMapReduceDataStore
{
	private final static Logger LOGGER = LoggerFactory.getLogger(HBaseDataStore.class);

	private final HBaseSplitsProvider splitsProvider;

	public HBaseDataStore(
			final HBaseSplitsProvider splitsProvider,
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
				splitsProvider,
				operations,
				options);
	}

	public HBaseDataStore(
			final IndexStore indexStore,
			final AdapterStore adapterStore,
			final DataStatisticsStore statisticsStore,
			final AdapterIndexMappingStore indexMappingStore,
			final HBaseSecondaryIndexDataStore secondaryIndexDataStore,
			final HBaseSplitsProvider splitsProvider,
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

		this.splitsProvider = splitsProvider;
	}

	@Override
	protected void initOnIndexWriterCreate(
			final DataAdapter adapter,
			final PrimaryIndex index ) {
		final String indexName = index.getId().getString();
		final String columnFamily = adapter.getAdapterId().getString();
		final boolean rowMerging = adapter instanceof RowMergingDataAdapter;
		if (rowMerging) {
			if (!((HBaseOperations) baseOperations).isRowMergingEnabled(
					adapter.getAdapterId(),
					indexName)) {
				if (baseOptions.isCreateTable()) {
					((HBaseOperations) baseOperations).createTable(
							index.getId(),
							false,
							adapter.getAdapterId());
				}
				if (baseOptions.isServerSideLibraryEnabled()) {
					((HBaseOperations) baseOperations).ensureServerSideOperationsObserverAttached(index.getId());
					ServerOpHelper.addServerSideRowMerging(
							((RowMergingDataAdapter<?, ?>) adapter),
							(ServerSideOperations) baseOperations,
							RowMergingServerOp.class.getName(),
							RowMergingVisibilityServerOp.class.getName(),
							indexName);
				}
			}
		}

		((HBaseOperations) baseOperations).verifyColumnFamily(
				columnFamily,
				!rowMerging,
				indexName,
				true);

	}

	@Override
	public List<InputSplit> getSplits(
			final DistributableQuery query,
			final QueryOptions queryOptions,
			final AdapterStore adapterStore,
			final AdapterIndexMappingStore aimStore,
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
				aimStore,
				minSplits,
				maxSplits);
	}
}
