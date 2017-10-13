package mil.nga.giat.geowave.datastore.hbase.coprocessors;

import java.io.IOException;

import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.exceptions.DeserializationException;
import org.apache.hadoop.hbase.filter.FilterBase;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;

import mil.nga.giat.geowave.core.index.StringUtils;

public class MergeDataMessage extends
		FilterBase
{
	private final static Logger LOGGER = Logger.getLogger(MergeDataMessage.class);

	// TEST ONLY!
	static {
		LOGGER.setLevel(Level.DEBUG);
	}

	private String mergeData;

	public MergeDataMessage() {

	}

	public static MergeDataMessage parseFrom(
			final byte[] pbBytes )
			throws DeserializationException {
		MergeDataMessage mergingFilter = new MergeDataMessage();

		String mergeData = StringUtils.stringFromBinary(pbBytes);
		mergingFilter.setMergeData(mergeData);

		return mergingFilter;
	}

	@Override
	public byte[] toByteArray()
			throws IOException {
		return StringUtils.stringToBinary(mergeData);
	}

	public String getMergeData() {
		return mergeData;
	}

	public void setMergeData(
			String mergeData ) {
		this.mergeData = mergeData;
	}

	@Override
	public ReturnCode filterKeyValue(
			Cell v )
			throws IOException {
		// This is a no-op, since we're not a real filter
		return ReturnCode.SKIP;
	}
}
