package mil.nga.giat.geowave.datastore.hbase.coprocessors;

import java.io.IOException;
import java.util.HashMap;
import java.util.List;

import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.regionserver.InternalScanner;
import org.apache.hadoop.hbase.regionserver.ScannerContext;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;

import mil.nga.giat.geowave.core.index.ByteArrayId;
import mil.nga.giat.geowave.core.store.adapter.RowMergingDataAdapter.RowTransform;

public class MergingInternalScanner implements
		InternalScanner
{
	private final static Logger LOGGER = Logger.getLogger(MergingInternalScanner.class);

	private final InternalScanner delegate;
	private HashMap<ByteArrayId, RowTransform> mergingTransformMap;

	// TEST ONLY!
	static {
		LOGGER.setLevel(Level.DEBUG);
	}

	public MergingInternalScanner(
			final InternalScanner delegate ) {
		this.delegate = delegate;
	}

	@Override
	public boolean next(
			List<Cell> results )
			throws IOException {
		LOGGER.debug("MERGING SCANNER > next(1)");

		boolean done = delegate.next(results);

		return done;
	}

	@Override
	public boolean next(
			List<Cell> result,
			ScannerContext scannerContext )
			throws IOException {
		// TODO: actual merging goes here...

		boolean done = delegate.next(
				result,
				scannerContext);

		LOGGER.debug("MERGING SCANNER > next(2): got " + result.size() + " cells.");

		return done;
	}

	@Override
	public void close()
			throws IOException {
		delegate.close();
	}

	public void setTransformMap(
			HashMap<ByteArrayId, RowTransform> mergingTransformMap ) {
		this.mergingTransformMap = mergingTransformMap;
	}
}
