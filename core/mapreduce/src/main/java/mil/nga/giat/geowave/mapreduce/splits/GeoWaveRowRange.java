package mil.nga.giat.geowave.mapreduce.splits;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.Writable;

public class GeoWaveRowRange implements
		Writable
{
	private byte[] startKey;
	private byte[] endKey;
	private boolean startKeyInclusive;
	private boolean endKeyInclusive;

	protected GeoWaveRowRange() {}

	public GeoWaveRowRange(
			final byte[] startKey,
			final byte[] endKey,
			final boolean startKeyInclusive,
			final boolean endKeyInclusive ) {
		this.startKey = startKey;
		this.endKey = endKey;
		this.startKeyInclusive = startKeyInclusive;
		this.endKeyInclusive = endKeyInclusive;
	}

	@Override
	public void write(
			final DataOutput out )
			throws IOException {
		out.writeBoolean(startKey == null);
		out.writeBoolean(endKey == null);
		if (startKey != null) {
			out.writeShort(startKey.length);
			out.write(startKey);
		}
		if (endKey != null) {
			out.writeShort(endKey.length);
			out.write(endKey);
		}
		out.writeBoolean(startKeyInclusive);
		out.writeBoolean(endKeyInclusive);
	}

	@Override
	public void readFields(
			final DataInput in )
			throws IOException {
		final boolean infiniteStartKey = in.readBoolean();
		final boolean infiniteEndKey = in.readBoolean();
		if (!infiniteStartKey) {
			startKey = new byte[in.readShort()];
			in.readFully(startKey);
		}
		else {
			startKey = null;
		}

		if (!infiniteEndKey) {
			endKey = new byte[in.readShort()];
			in.readFully(endKey);
		}
		else {
			endKey = null;
		}

		startKeyInclusive = in.readBoolean();
		endKeyInclusive = in.readBoolean();
	}

	public byte[] getStartKey() {
		return startKey;
	}

	public byte[] getEndKey() {
		return endKey;
	}

	public boolean isStartKeyInclusive() {
		return startKeyInclusive;
	}

	public boolean isEndKeyInclusive() {
		return endKeyInclusive;
	}

	public boolean isInfiniteStartKey() {
		return startKey == null;
	}

	public boolean isInfiniteStopKey() {
		return endKey == null;
	}
}
