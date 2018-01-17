package mil.nga.giat.geowave.datastore.dynamodb.index.secondary;

import java.io.IOException;

import mil.nga.giat.geowave.core.index.ByteArrayId;
import mil.nga.giat.geowave.core.store.CloseableIterator;
import mil.nga.giat.geowave.core.store.DataStore;
import mil.nga.giat.geowave.core.store.adapter.DataAdapter;
import mil.nga.giat.geowave.core.store.entities.GeoWaveRow;
import mil.nga.giat.geowave.core.store.index.BaseSecondaryIndexDataStore;
import mil.nga.giat.geowave.core.store.index.PrimaryIndex;
import mil.nga.giat.geowave.core.store.index.SecondaryIndex;
import mil.nga.giat.geowave.core.store.operations.Writer;
import mil.nga.giat.geowave.core.store.query.DistributableQuery;
import mil.nga.giat.geowave.datastore.dynamodb.operations.DynamoDBOperations;

// TODO implement secondary indexing for DynamoDB
public class DynamoDBSecondaryIndexDataStore extends
		BaseSecondaryIndexDataStore
{
	private final DynamoDBOperations operations;

	public DynamoDBSecondaryIndexDataStore(
			final DynamoDBOperations operations ) {
		this.operations = operations;
	}

	@Override
	public void setDataStore(
			DataStore dataStore ) {
		// TODO Auto-generated method stub

	}

	@Override
	public <T> CloseableIterator<T> query(
			SecondaryIndex<T> secondaryIndex,
			ByteArrayId indexedAttributeFieldId,
			DataAdapter<T> adapter,
			PrimaryIndex primaryIndex,
			DistributableQuery query,
			String... authorizations ) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	protected GeoWaveRow buildJoinMutation(
			byte[] secondaryIndexRowId,
			byte[] adapterId,
			byte[] indexedAttributeFieldId,
			byte[] primaryIndexId,
			byte[] primaryIndexPartitionKey,
			byte[] primaryIndexSortKey,
			byte[] attributeVisibility )
			throws IOException {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	protected GeoWaveRow buildMutation(
			byte[] secondaryIndexRowId,
			byte[] adapterId,
			byte[] indexedAttributeFieldId,
			byte[] dataId,
			byte[] fieldId,
			byte[] fieldValue,
			byte[] fieldVisibility )
			throws IOException {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	protected GeoWaveRow buildJoinDeleteMutation(
			byte[] secondaryIndexRowId,
			byte[] adapterId,
			byte[] indexedAttributeFieldId,
			byte[] primaryIndexId,
			byte[] primaryIndexRowId )
			throws IOException {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	protected GeoWaveRow buildFullDeleteMutation(
			byte[] secondaryIndexRowId,
			byte[] adapterId,
			byte[] indexedAttributeFieldId,
			byte[] dataId,
			byte[] fieldId )
			throws IOException {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	protected Writer getWriter(
			ByteArrayId secondaryIndexId ) {
		// TODO Auto-generated method stub
		return null;
	}

}
