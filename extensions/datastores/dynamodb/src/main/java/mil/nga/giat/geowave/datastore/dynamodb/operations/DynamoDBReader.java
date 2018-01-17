package mil.nga.giat.geowave.datastore.dynamodb.operations;

import java.util.Iterator;

import org.apache.log4j.Logger;

import com.amazonaws.services.dynamodbv2.model.ScanRequest;

import mil.nga.giat.geowave.core.store.entities.GeoWaveRow;
import mil.nga.giat.geowave.core.store.operations.Reader;
import mil.nga.giat.geowave.core.store.operations.ReaderParams;
import mil.nga.giat.geowave.mapreduce.splits.RecordReaderParams;

public class DynamoDBReader implements
		Reader
{
	private final static Logger LOGGER = Logger.getLogger(
			DynamoDBReader.class);

	private final ReaderParams readerParams;
	private final RecordReaderParams recordReaderParams;
	private final DynamoDBOperations operations;
	private final boolean clientSideRowMerging;

	private final boolean wholeRowEncoding;
	private final int partitionKeyLength;

	private Iterator<GeoWaveRow> iterator;

	public DynamoDBReader(
			final ReaderParams readerParams,
			final DynamoDBOperations operations ) {
		this.readerParams = readerParams;
		this.recordReaderParams = null;
		this.operations = operations;

		this.partitionKeyLength = readerParams.getIndex().getIndexStrategy().getPartitionKeyLength();
		this.wholeRowEncoding = readerParams.isMixedVisibility() && !readerParams.isServersideAggregation();
		this.clientSideRowMerging = readerParams.isClientsideRowMerging();

		initScanner();
	}

	public DynamoDBReader(
			final RecordReaderParams recordReaderParams,
			final DynamoDBOperations operations ) {
		this.readerParams = null;
		this.recordReaderParams = recordReaderParams;
		this.operations = operations;

		this.partitionKeyLength = recordReaderParams.getIndex().getIndexStrategy().getPartitionKeyLength();
		this.wholeRowEncoding = recordReaderParams.isMixedVisibility() && !recordReaderParams.isServersideAggregation();
		this.clientSideRowMerging = false;

		initRecordScanner();
	}

	protected void initScanner() {
		String tableName = operations.getQualifiedTableName(
				readerParams.getIndex().getId().getString());

		final ScanRequest request = new ScanRequest(
				tableName);

		iterator = operations.getScannedResults(
				request);
	}

	protected void initRecordScanner() {}

	@Override
	public void close()
			throws Exception {

	}

	@Override
	public boolean hasNext() {
		return iterator.hasNext();
	}

	@Override
	public GeoWaveRow next() {
		return iterator.next();
	}

}
