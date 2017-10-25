package mil.nga.giat.geowave.datastore.hbase.coprocessors;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.HashMap;

import org.apache.commons.lang.SerializationUtils;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.exceptions.DeserializationException;
import org.apache.hadoop.hbase.filter.FilterBase;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;

import mil.nga.giat.geowave.core.index.ByteArrayId;
import mil.nga.giat.geowave.core.index.PersistenceUtils;
import mil.nga.giat.geowave.core.store.adapter.RowMergingDataAdapter.RowTransform;

public class MergeDataMessage extends
		FilterBase
{
	private final static Logger LOGGER = Logger.getLogger(MergeDataMessage.class);

	// TEST ONLY!
	static {
		LOGGER.setLevel(Level.DEBUG);
	}

	private ByteArrayId tableName;
	private ByteArrayId adapterId;
	private RowTransform transformData;
	private HashMap<String, String> options;

	public MergeDataMessage() {

	}

	public static MergeDataMessage parseFrom(
			final byte[] pbBytes )
			throws DeserializationException {
		final ByteBuffer buf = ByteBuffer.wrap(pbBytes);

		final int tableLength = buf.getInt();
		final int adapterLength = buf.getInt();
		final int transformLength = buf.getInt();

		final byte[] tableBytes = new byte[tableLength];
		buf.get(tableBytes);

		final byte[] adapterBytes = new byte[adapterLength];
		buf.get(adapterBytes);

		final byte[] transformBytes = new byte[transformLength];
		buf.get(transformBytes);

		final byte[] optionsBytes = new byte[pbBytes.length - transformLength - adapterLength - tableLength - 12];
		buf.get(optionsBytes);

		MergeDataMessage mergingFilter = new MergeDataMessage();

		mergingFilter.setTableName(new ByteArrayId(
				tableBytes));

		mergingFilter.setAdapterId(new ByteArrayId(
				adapterBytes));

		RowTransform rowTransform = PersistenceUtils.fromBinary(
				transformBytes,
				RowTransform.class);

		mergingFilter.setTransformData(rowTransform);

		HashMap<String, String> options = (HashMap<String, String>) SerializationUtils.deserialize(optionsBytes);

		mergingFilter.setOptions(options);

		return mergingFilter;
	}

	@Override
	public byte[] toByteArray()
			throws IOException {
		final byte[] tableBinary = tableName.getBytes();
		final byte[] adapterBinary = adapterId.getBytes();
		final byte[] transformBinary = PersistenceUtils.toBinary(transformData);
		final byte[] optionsBinary = SerializationUtils.serialize(options);

		final ByteBuffer buf = ByteBuffer.allocate(tableBinary.length + adapterBinary.length + transformBinary.length
				+ optionsBinary.length + 12);

		buf.putInt(tableBinary.length);
		buf.putInt(adapterBinary.length);
		buf.putInt(transformBinary.length);
		buf.put(tableBinary);
		buf.put(adapterBinary);
		buf.put(transformBinary);
		buf.put(optionsBinary);

		return buf.array();
	}

	public ByteArrayId getTableName() {
		return tableName;
	}

	public void setTableName(
			ByteArrayId tableName ) {
		this.tableName = tableName;
	}

	public ByteArrayId getAdapterId() {
		return adapterId;
	}

	public void setAdapterId(
			ByteArrayId adapterId ) {
		this.adapterId = adapterId;
	}

	public RowTransform getTransformData() {
		return transformData;
	}

	public void setTransformData(
			RowTransform transformData ) {
		this.transformData = transformData;
	}

	public HashMap<String, String> getOptions() {
		return options;
	}

	public void setOptions(
			HashMap<String, String> options ) {
		this.options = options;
	}

	@Override
	public ReturnCode filterKeyValue(
			Cell v )
			throws IOException {
		// This is a no-op, since we're not a real filter
		return ReturnCode.SKIP;
	}
}
