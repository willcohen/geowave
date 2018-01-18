package mil.nga.giat.geowave.datastore.dynamodb.index.secondary;

import mil.nga.giat.geowave.core.store.StoreFactoryHelper;
import mil.nga.giat.geowave.core.store.StoreFactoryOptions;
import mil.nga.giat.geowave.core.store.index.SecondaryIndexDataStore;
import mil.nga.giat.geowave.core.store.metadata.SecondaryIndexStoreFactory;
import mil.nga.giat.geowave.core.store.operations.DataStoreOperations;
import mil.nga.giat.geowave.datastore.dynamodb.DynamoDBOptions;
import mil.nga.giat.geowave.datastore.dynamodb.operations.DynamoDBOperations;

public class DynamoDBSecondaryIndexDataStoreFactory extends
		SecondaryIndexStoreFactory
{

	public DynamoDBSecondaryIndexDataStoreFactory(
			String typeName,
			String description,
			StoreFactoryHelper helper ) {
		super(
				typeName,
				description,
				helper);
	}

	@Override
	public SecondaryIndexDataStore createStore(
			final StoreFactoryOptions options ) {
		if (!(options instanceof DynamoDBOptions)) {
			throw new AssertionError(
					"Expected " + DynamoDBOptions.class.getSimpleName());
		}
		final DynamoDBOptions opts = (DynamoDBOptions) options;
		final DataStoreOperations operations = helper.createOperations(
				opts);

		return new DynamoDBSecondaryIndexDataStore(
				(DynamoDBOperations) operations);
	}
}
