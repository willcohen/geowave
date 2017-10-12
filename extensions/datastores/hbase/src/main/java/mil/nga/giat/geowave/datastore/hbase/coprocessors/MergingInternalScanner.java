package mil.nga.giat.geowave.datastore.hbase.coprocessors;

import java.io.IOException;
import java.util.List;

import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.regionserver.InternalScanner;
import org.apache.hadoop.hbase.regionserver.ScannerContext;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;

public class MergingInternalScanner implements
		InternalScanner
{
	private final static Logger LOGGER = Logger.getLogger(MergingInternalScanner.class);

	private final InternalScanner delegate;

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

		return delegate.next(results);
	}

	@Override
	public boolean next(
			List<Cell> result,
			ScannerContext scannerContext )
			throws IOException {
		LOGGER.debug("MERGING SCANNER > next(2)");

		return delegate.next(
				result,
				scannerContext);
	}

	@Override
	public void close()
			throws IOException {
		delegate.close();
	}
}
