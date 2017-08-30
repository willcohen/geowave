package mil.nga.giat.geowave.datastore.hbase.operations;

import java.util.Iterator;

import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.log4j.Logger;

import mil.nga.giat.geowave.core.store.entities.GeoWaveRow;
import mil.nga.giat.geowave.core.store.operations.Reader;
import mil.nga.giat.geowave.datastore.hbase.HBaseRow;

public class HBaseReader implements
		Reader
{
	private final static Logger LOGGER = Logger.getLogger(HBaseReader.class);
	private final ResultScanner scanner;
	private final Iterator<Result> it;

	private final boolean wholeRowEncoding;
	private final int partitionKeyLength;

	public HBaseReader(
			final ResultScanner scanner,
			final int partitionKeyLength,
			final boolean wholeRowEncoding ) {
		this.scanner = scanner;
		this.partitionKeyLength = partitionKeyLength;
		this.wholeRowEncoding = wholeRowEncoding;

		it = scanner.iterator();
	}

	@Override
	public void close()
			throws Exception {
		scanner.close();
	}

	@Override
	public boolean hasNext() {
		return it.hasNext();
	}

	@Override
	public GeoWaveRow next() {
		final Result entry = it.next();

		return new HBaseRow(
				entry,
				partitionKeyLength);
	}

}
