package mil.nga.giat.geowave.datastore.hbase.coprocessors;

import java.io.IOException;
import java.nio.ByteBuffer;

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

	private ByteArrayId adapterId;
	private RowTransform transformData;

	public MergeDataMessage() {

	}

	public static MergeDataMessage parseFrom(
			final byte[] pbBytes )
			throws DeserializationException {
		final ByteBuffer buf = ByteBuffer.wrap(pbBytes);

		final int adapterLength = buf.getInt();

		final byte[] adapterBytes = new byte[adapterLength];
		buf.get(adapterBytes);

		final byte[] transformBytes = new byte[pbBytes.length - adapterLength - 4];
		buf.get(transformBytes);

		MergeDataMessage mergingFilter = new MergeDataMessage();
		mergingFilter.setAdapterId(new ByteArrayId(
				adapterBytes));

		RowTransform rowTransform = PersistenceUtils.fromBinary(
				transformBytes,
				RowTransform.class);

		mergingFilter.setTransformData(rowTransform);

		return mergingFilter;
	}

	@Override
	public byte[] toByteArray()
			throws IOException {
		final byte[] adapterBinary = adapterId.getBytes();
		final byte[] transformBinary = PersistenceUtils.toBinary(transformData);

		final ByteBuffer buf = ByteBuffer.allocate(adapterBinary.length + transformBinary.length + 4);

		buf.putInt(adapterBinary.length);
		buf.put(adapterBinary);
		buf.put(transformBinary);

		return buf.array();
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

	@Override
	public ReturnCode filterKeyValue(
			Cell v )
			throws IOException {
		// This is a no-op, since we're not a real filter
		return ReturnCode.SKIP;
	}
}
