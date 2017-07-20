package mil.nga.giat.geowave.mapreduce.input;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.commons.lang3.ArrayUtils;
import org.apache.hadoop.io.WritableComparator;

import mil.nga.giat.geowave.core.index.ByteArrayId;
import mil.nga.giat.geowave.core.store.entities.GeoWaveRow;
import mil.nga.giat.geowave.mapreduce.GeoWaveKey;

/**
 * This class encapsulates the unique identifier for GeoWave input data using a
 * map-reduce GeoWave input format. The combination of the the adapter ID and
 * the data ID should be unique.
 */
public class GeoWaveInputKey extends
		GeoWaveKey
{
	/**
	 *
	 */
	private static final long serialVersionUID = 1L;
	private ByteArrayId dataId;
	private transient GeoWaveRow row;

	public GeoWaveInputKey() {
		super();
	}

	public GeoWaveInputKey(
			final GeoWaveRow row,
			final ByteArrayId indexId ) {
		super(
				new ByteArrayId(
						row.getAdapterId()));
		if (row.getNumberOfDuplicates() > 0) {
			dataId = new ByteArrayId(
					row.getDataId());
		}
		else {
			// if deduplication should be disabled, prefix the actual data
			// ID with the index ID concatenated with the insertion
			// ID to gaurantee uniqueness and effectively disable
			// aggregating by only the data ID
			byte[] idBytes = row.getDataId();
			if (row.getSortKey() != null) {
				idBytes = ArrayUtils.addAll(
						row.getSortKey(),
						idBytes);
			}
			if (row.getPartitionKey() != null) {
				idBytes = ArrayUtils.addAll(
						row.getPartitionKey(),
						idBytes);
			}
			if (indexId != null) {
				idBytes = ArrayUtils.addAll(
						indexId.getBytes(),
						idBytes);
			}
			dataId = new ByteArrayId(
					idBytes);
		}
		this.row = row;
	}

	public GeoWaveInputKey(
			final ByteArrayId adapterId,
			final ByteArrayId dataId ) {
		super(
				adapterId);
		this.dataId = dataId;
	}

	public GeoWaveRow getRow() {
		return row;
	}

	public void setRow(
			GeoWaveRow row ) {
		this.row = row;
	}

	public void setDataId(
			final ByteArrayId dataId ) {
		this.dataId = dataId;
	}

	public ByteArrayId getDataId() {
		return dataId;
	}

	@Override
	public int compareTo(
			final GeoWaveKey o ) {
		final int baseCompare = super.compareTo(o);
		if (baseCompare != 0) {
			return baseCompare;
		}
		if (o instanceof GeoWaveInputKey) {
			final GeoWaveInputKey other = (GeoWaveInputKey) o;
			return WritableComparator.compareBytes(
					dataId.getBytes(),
					0,
					dataId.getBytes().length,
					other.dataId.getBytes(),
					0,
					other.dataId.getBytes().length);
		}
		return 1;
	}

	@Override
	public int hashCode() {
		final int prime = 31;
		int result = super.hashCode();
		result = (prime * result) + ((dataId == null) ? 0 : dataId.hashCode());
		return result;
	}

	@Override
	public boolean equals(
			final Object obj ) {
		if (this == obj) {
			return true;
		}
		if (!super.equals(obj)) {
			return false;
		}
		if (getClass() != obj.getClass()) {
			return false;
		}
		final GeoWaveInputKey other = (GeoWaveInputKey) obj;
		if (dataId == null) {
			if (other.dataId != null) {
				return false;
			}
		}
		else if (!dataId.equals(other.dataId)) {
			return false;
		}
		return true;
	}

	@Override
	public void readFields(
			final DataInput input )
			throws IOException {
		super.readFields(input);
		final int dataIdLength = input.readInt();
		final byte[] dataIdBytes = new byte[dataIdLength];
		input.readFully(dataIdBytes);
		dataId = new ByteArrayId(
				dataIdBytes);
	}

	@Override
	public void write(
			final DataOutput output )
			throws IOException {
		super.write(output);
		output.writeInt(dataId.getBytes().length);
		output.write(dataId.getBytes());
	}
}
