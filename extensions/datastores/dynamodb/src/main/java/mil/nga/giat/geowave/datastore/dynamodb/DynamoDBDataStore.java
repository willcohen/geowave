package mil.nga.giat.geowave.datastore.dynamodb;

import java.io.IOException;
import java.util.List;

import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.log4j.Logger;

import mil.nga.giat.geowave.core.store.DataStoreOptions;
import mil.nga.giat.geowave.core.store.adapter.AdapterIndexMappingStore;
import mil.nga.giat.geowave.core.store.adapter.AdapterStore;
import mil.nga.giat.geowave.core.store.adapter.DataAdapter;
import mil.nga.giat.geowave.core.store.adapter.statistics.DataStatisticsStore;
import mil.nga.giat.geowave.core.store.index.IndexStore;
import mil.nga.giat.geowave.core.store.index.PrimaryIndex;
import mil.nga.giat.geowave.core.store.metadata.AdapterIndexMappingStoreImpl;
import mil.nga.giat.geowave.core.store.metadata.AdapterStoreImpl;
import mil.nga.giat.geowave.core.store.metadata.DataStatisticsStoreImpl;
import mil.nga.giat.geowave.core.store.metadata.IndexStoreImpl;
import mil.nga.giat.geowave.core.store.query.DistributableQuery;
import mil.nga.giat.geowave.core.store.query.QueryOptions;
import mil.nga.giat.geowave.datastore.dynamodb.index.secondary.DynamoDBSecondaryIndexDataStore;
import mil.nga.giat.geowave.datastore.dynamodb.mapreduce.DynamoDBSplitsProvider;
import mil.nga.giat.geowave.datastore.dynamodb.operations.DynamoDBOperations;
import mil.nga.giat.geowave.mapreduce.BaseMapReduceDataStore;

public class DynamoDBDataStore extends
		BaseMapReduceDataStore
{
	public final static String TYPE = "dynamodb";
	public static final Integer PARTITIONS = 1;

	private final static Logger LOGGER = Logger.getLogger(DynamoDBDataStore.class);
	private final DynamoDBOperations dynamodbOperations;
	private static int counter = 0;

	private final DynamoDBSplitsProvider splitsProvider = new DynamoDBSplitsProvider();

	public DynamoDBDataStore(
			final DynamoDBOperations operations ) {
		this(
				new IndexStoreImpl(
						operations,
						operations.getOptions().getBaseOptions()),
				new AdapterStoreImpl(
						operations,
						operations.getOptions().getBaseOptions()),
				new DataStatisticsStoreImpl(
						operations,
						operations.getOptions().getBaseOptions()),
				new AdapterIndexMappingStoreImpl(
						operations,
						operations.getOptions().getBaseOptions()),
				new DynamoDBSecondaryIndexDataStore(
						operations),
				operations,
				operations.getOptions().getBaseOptions());
	}

	public DynamoDBDataStore(
			final IndexStore indexStore,
			final AdapterStore adapterStore,
			final DataStatisticsStore statisticsStore,
			final AdapterIndexMappingStore indexMappingStore,
			final DynamoDBSecondaryIndexDataStore secondaryIndexDataStore,
			final DynamoDBOperations operations,
			final DataStoreOptions options ) {
		super(
				indexStore,
				adapterStore,
				statisticsStore,
				indexMappingStore,
				secondaryIndexDataStore,
				operations,
				options);

		secondaryIndexDataStore.setDataStore(this);

		dynamodbOperations = operations;
	}

	@Override
	protected void initOnIndexWriterCreate(
			final DataAdapter adapter,
			final PrimaryIndex index ) {
		// TODO
	}

	@Override
	public List<InputSplit> getSplits(
			DistributableQuery query,
			QueryOptions queryOptions,
			AdapterStore adapterStore,
			AdapterIndexMappingStore aimStore,
			DataStatisticsStore statsStore,
			IndexStore indexStore,
			Integer minSplits,
			Integer maxSplits )
			throws IOException,
			InterruptedException {
		return splitsProvider.getSplits(
				dynamodbOperations,
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