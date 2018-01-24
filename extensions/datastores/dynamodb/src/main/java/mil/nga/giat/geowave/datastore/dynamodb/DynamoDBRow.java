package mil.nga.giat.geowave.datastore.dynamodb;

import java.nio.ByteBuffer;
import java.util.Map;

import com.amazonaws.services.dynamodbv2.model.AttributeValue;
import com.google.common.base.Function;

import mil.nga.giat.geowave.core.index.StringUtils;
import mil.nga.giat.geowave.core.store.entities.GeoWaveKey;
import mil.nga.giat.geowave.core.store.entities.GeoWaveKeyImpl;
import mil.nga.giat.geowave.core.store.entities.GeoWaveRow;
import mil.nga.giat.geowave.core.store.entities.GeoWaveValue;
import mil.nga.giat.geowave.core.store.entities.GeoWaveValueImpl;

public class DynamoDBRow implements
		GeoWaveRow
{
	public static final String GW_PARTITION_ID_KEY = "P";
	public static final String GW_RANGE_KEY = "R";
	public static final String GW_FIELD_MASK_KEY = "F";
	public static final String GW_VALUE_KEY = "V";

	private final GeoWaveKey key;
	private final GeoWaveValue[] fieldValues;

	private final Map<String, AttributeValue> objMap;
	private String partitionId;

	public DynamoDBRow(
			final String partitionId,
			final byte[] dataId,
			final byte[] adapterId,
			final byte[] sortKey,
			final byte[] fieldMask,
			final byte[] value,
			final int numberOfDuplicates ) {
		this.partitionId = partitionId;
		this.objMap = null; // not needed for ingest
		byte[] partitionKey = StringUtils.stringToBinary(this.partitionId);

		this.key = new GeoWaveKeyImpl(
				dataId,
				adapterId,
				partitionKey,
				sortKey,
				numberOfDuplicates);

		this.fieldValues = new GeoWaveValueImpl[1];
		this.fieldValues[0] = new GeoWaveValueImpl(
				fieldMask,
				null,
				value);
	}

	public DynamoDBRow(
			final Map<String, AttributeValue> objMap ) {
		final byte[] rowId = objMap.get(
				GW_RANGE_KEY).getB().array();
		final int length = rowId.length;
		final int offset = 0;

		final ByteBuffer metadataBuf = ByteBuffer.wrap(
				rowId,
				length + offset - 12,
				12);
		final int adapterIdLength = metadataBuf.getInt();
		final int dataIdLength = metadataBuf.getInt();
		final int numberOfDuplicates = metadataBuf.getInt();

		final ByteBuffer buf = ByteBuffer.wrap(
				rowId,
				offset,
				length - 12);
		final byte[] sortKey = new byte[length - 12 - adapterIdLength - dataIdLength];
		final byte[] adapterId = new byte[adapterIdLength];
		final byte[] dataId = new byte[dataIdLength];
		// get adapterId first
		buf.get(adapterId);
		buf.get(sortKey);
		buf.get(dataId);

		this.objMap = objMap;

		this.partitionId = objMap.get(
				GW_PARTITION_ID_KEY).getN();

		byte[] partitionKey = StringUtils.stringToBinary(this.partitionId);

		this.key = new GeoWaveKeyImpl(
				dataId,
				adapterId,
				partitionKey,
				sortKey,
				numberOfDuplicates);

		byte[] fieldMask = objMap.get(
				GW_FIELD_MASK_KEY).getB().array();

		byte[] value = objMap.get(
				GW_VALUE_KEY).getB().array();

		this.fieldValues = new GeoWaveValueImpl[1];
		this.fieldValues[0] = new GeoWaveValueImpl(
				fieldMask,
				null,
				value);

	}

	public Map<String, AttributeValue> getAttributeMapping() {
		return objMap;
	}

	public String getPartitionId() {
		return partitionId;
	}

	public static class GuavaRowTranslationHelper implements
			Function<Map<String, AttributeValue>, DynamoDBRow>
	{
		@Override
		public DynamoDBRow apply(
				final Map<String, AttributeValue> input ) {
			return new DynamoDBRow(
					input);
		}

	}

	@Override
	public byte[] getDataId() {
		return key.getDataId();
	}

	@Override
	public byte[] getAdapterId() {
		return key.getAdapterId();
	}

	@Override
	public byte[] getSortKey() {
		return key.getSortKey();
	}

	@Override
	public byte[] getPartitionKey() {
		return key.getPartitionKey();
	}

	@Override
	public int getNumberOfDuplicates() {
		return key.getNumberOfDuplicates();
	}

	@Override
	public GeoWaveValue[] getFieldValues() {
		return fieldValues;
	}
}
