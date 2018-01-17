package mil.nga.giat.geowave.datastore.dynamodb.operations;

import java.util.HashMap;
import java.util.Map;

import com.amazonaws.services.dynamodbv2.model.AttributeValue;
import com.amazonaws.services.dynamodbv2.model.PutRequest;
import com.amazonaws.services.dynamodbv2.model.WriteRequest;

import mil.nga.giat.geowave.core.store.entities.GeoWaveMetadata;
import mil.nga.giat.geowave.core.store.operations.MetadataWriter;
import mil.nga.giat.geowave.datastore.dynamodb.DynamoDBRow;

public class DynamoDBMetadataWriter implements
		MetadataWriter
{

	@Override
	public void close()
			throws Exception {
		// TODO Auto-generated method stub

	}

	@Override
	public void write(
			GeoWaveMetadata metadata ) {
		final Map<String, AttributeValue> map = new HashMap<String, AttributeValue>();

		map.put(
				DynamoDBRow.GW_PARTITION_ID_KEY,
				new AttributeValue().withN(
						partitionId));

		map.put(
				DynamoDBRow.GW_RANGE_KEY,
				new AttributeValue().withB(
						rangeKeyBuffer));

		map.put(
				DynamoDBRow.GW_FIELD_MASK_KEY,
				new AttributeValue().withB(
						fieldMaskBuffer));

		map.put(
				DynamoDBRow.GW_VALUE_KEY,
				new AttributeValue().withB(
						valueBuffer));

		WriteRequest writeRequest = new WriteRequest(
				new PutRequest(
						map));

	}

	@Override
	public void flush() {
		// TODO Auto-generated method stub

	}

}
