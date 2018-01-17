package mil.nga.giat.geowave.datastore.dynamodb.operations;

import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.Map;

import com.amazonaws.services.dynamodbv2.AmazonDynamoDBAsyncClient;
import com.amazonaws.services.dynamodbv2.model.AttributeValue;
import com.amazonaws.services.dynamodbv2.model.PutItemRequest;

import mil.nga.giat.geowave.core.store.entities.GeoWaveMetadata;
import mil.nga.giat.geowave.core.store.operations.MetadataWriter;

public class DynamoDBMetadataWriter implements
		MetadataWriter
{
	private final AmazonDynamoDBAsyncClient client;
	private final String tableName;

	public DynamoDBMetadataWriter(
			final DynamoDBOperations dynamoDBOperations,
			final AmazonDynamoDBAsyncClient client,
			final String tableName ) {
		this.client = client;
		this.tableName = tableName;
	}

	@Override
	public void close()
			throws Exception {
		// TODO Auto-generated method stub

	}

	@Override
	public void write(
			GeoWaveMetadata metadata ) {
		final Map<String, AttributeValue> map = new HashMap<>();
		map.put(
				DynamoDBOperations.METADATA_PRIMARY_ID_KEY,
				new AttributeValue().withB(ByteBuffer.wrap(metadata.getPrimaryId())));
		
		if (metadata.getSecondaryId() != null) {
			map.put(
					DynamoDBOperations.METADATA_SECONDARY_ID_KEY,
					new AttributeValue().withB(ByteBuffer.wrap(metadata.getSecondaryId())));
		}
		
		map.put(
				DynamoDBOperations.METADATA_TIMESTAMP_KEY,
				new AttributeValue().withN(Long.toString(System.currentTimeMillis())));
		map.put(
				DynamoDBOperations.METADATA_VALUE_KEY,
				new AttributeValue().withB(ByteBuffer.wrap(metadata.getValue())));
		
		client.putItem(new PutItemRequest(
				tableName,
				map));

	}

	@Override
	public void flush() {
		// TODO Auto-generated method stub

	}

}
