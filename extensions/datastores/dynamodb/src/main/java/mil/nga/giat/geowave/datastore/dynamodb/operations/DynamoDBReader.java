package mil.nga.giat.geowave.datastore.dynamodb.operations;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.function.Consumer;

import org.apache.log4j.Logger;

import com.amazonaws.services.dynamodbv2.model.AttributeValue;
import com.amazonaws.services.dynamodbv2.model.ComparisonOperator;
import com.amazonaws.services.dynamodbv2.model.Condition;
import com.amazonaws.services.dynamodbv2.model.QueryRequest;
import com.amazonaws.services.dynamodbv2.model.QueryResult;
import com.amazonaws.services.dynamodbv2.model.ScanRequest;
import com.amazonaws.services.dynamodbv2.model.ScanResult;
import com.google.common.collect.Iterators;

import mil.nga.giat.geowave.core.index.ByteArrayId;
import mil.nga.giat.geowave.core.index.ByteArrayRange;
import mil.nga.giat.geowave.core.index.ByteArrayUtils;
import mil.nga.giat.geowave.core.index.StringUtils;
import mil.nga.giat.geowave.core.store.CloseableIterator;
import mil.nga.giat.geowave.core.store.adapter.AdapterStore;
import mil.nga.giat.geowave.core.store.adapter.DataAdapter;
import mil.nga.giat.geowave.core.store.entities.GeoWaveRow;
import mil.nga.giat.geowave.core.store.index.PrimaryIndex;
import mil.nga.giat.geowave.core.store.operations.Reader;
import mil.nga.giat.geowave.core.store.operations.ReaderParams;
import mil.nga.giat.geowave.datastore.dynamodb.DynamoDBDataStore;
import mil.nga.giat.geowave.datastore.dynamodb.DynamoDBRow;
import mil.nga.giat.geowave.datastore.dynamodb.DynamoDBRow.GuavaRowTranslationHelper;
import mil.nga.giat.geowave.datastore.dynamodb.util.AsyncPaginatedQuery;
import mil.nga.giat.geowave.datastore.dynamodb.util.AsyncPaginatedScan;
import mil.nga.giat.geowave.datastore.dynamodb.util.LazyPaginatedQuery;
import mil.nga.giat.geowave.datastore.dynamodb.util.LazyPaginatedScan;
import mil.nga.giat.geowave.mapreduce.splits.RecordReaderParams;
import mil.nga.giat.geowave.mapreduce.splits.SplitsProvider;

public class DynamoDBReader implements
		Reader
{
	private final static Logger LOGGER = Logger.getLogger(DynamoDBReader.class);

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

		final ScanRequest scanRequest = new ScanRequest(
				tableName);
		
		ArrayList<ByteArrayId> adapterIds = new ArrayList();
		if ((readerParams.getAdapterIds() != null) && !readerParams.getAdapterIds().isEmpty()) {
			for (final ByteArrayId adapterId : readerParams.getAdapterIds()) {
				adapterIds.add(adapterId);
			}
		}
		
		if ((readerParams.getLimit() != null) && (readerParams.getLimit() > 0)) {
			
		}

		final List<QueryRequest> requests = new ArrayList<>();

		final List<ByteArrayRange> ranges = readerParams.getQueryRanges().getCompositeQueryRanges();		
		if ((ranges != null) && !ranges.isEmpty()) {
			if ((ranges.size() == 1) && (adapterIds.size() == 1)) {
				final List<QueryRequest> queries = getPartitionRequests(
						tableName);
				final ByteArrayRange range = ranges.get(
						0);
				if (range.isSingleValue()) {
					for (final QueryRequest query : queries) {
						for (final ByteArrayId adapterID : adapterIds) {
							final byte[] start = ByteArrayUtils.combineArrays(
									adapterID.getBytes(),
									range.getStart().getBytes());
							query.addQueryFilterEntry(
									DynamoDBRow.GW_RANGE_KEY,
									new Condition()
											.withAttributeValueList(
													new AttributeValue().withB(
															ByteBuffer.wrap(
																	start)))
											.withComparisonOperator(
													ComparisonOperator.EQ));
						}
					}
				}
				else {
					for (final QueryRequest query : queries) {
						for (final ByteArrayId adapterID : adapterIds) {
							addQueryRange(
									range,
									query,
									adapterID);
						}
					}
				}
				requests.addAll(
						queries);
			}
			else {
				ranges.forEach(
						(queryRequest -> requests.addAll(
								addQueryRanges(
										tableName,
										queryRequest,
										adapterIds,
										operations.getAdapterStore()))));
			}

		}
		else if ((adapterIds != null) && !adapterIds.isEmpty()) {
			requests.addAll(getAdapterOnlyQueryRequests(
					tableName,
					adapterIds));
		}
		
		
		
		ScanResult scanResult = operations.getClient().scan(
				scanRequest);

		iterator = Iterators.transform(
				new LazyPaginatedScan(
						scanResult,
						scanRequest,
						operations.getClient()),
				new GuavaRowTranslationHelper());
	}

	protected void initRecordScanner() {
		String tableName = operations.getQualifiedTableName(
				recordReaderParams.getIndex().getId().getString());

		final ScanRequest scanRequest = new ScanRequest(
				tableName);

		ArrayList<ByteArrayId> adapterIds = new ArrayList();
		if ((recordReaderParams.getAdapterIds() != null) && !recordReaderParams.getAdapterIds().isEmpty()) {
			for (final ByteArrayId adapterId : recordReaderParams.getAdapterIds()) {
				adapterIds.add(adapterId);
			}
		}
		
		final List<QueryRequest> requests = new ArrayList<>();

		final List<ByteArrayRange> ranges = new ArrayList<>();
		final ByteArrayRange range = SplitsProvider.fromRowRange(recordReaderParams.getRowRange());
		
		// Use this instead of setStartRow/setStopRow for single rowkeys
		if (Arrays.equals(
				range.getStart().getBytes(),
				range.getEnd().getBytes())) {
			ranges.add(range);
		}
		else {
			ByteArrayId startRow = range.getStart();
			ByteArrayId stopRow;
			
			if (recordReaderParams.getRowRange().isEndSortKeyInclusive()) {
				stopRow = new ByteArrayId(SplitsProvider.getInclusiveEndKey(range.getEnd().getBytes()));
			}
			else {
				stopRow = range.getEnd();
			}
			
			ranges.add( new ByteArrayRange(startRow, stopRow));
		}

		if ((ranges != null) && !ranges.isEmpty()) {
			if ((ranges.size() == 1) && (adapterIds.size() == 1)) {
				final List<QueryRequest> queries = getPartitionRequests(
						tableName);
				final ByteArrayRange rowRange = ranges.get(
						0);
				if (rowRange.isSingleValue()) {
					for (final QueryRequest query : queries) {
						for (final ByteArrayId adapterID : adapterIds) {
							final byte[] start = ByteArrayUtils.combineArrays(
									adapterID.getBytes(),
									rowRange.getStart().getBytes());
							query.addQueryFilterEntry(
									DynamoDBRow.GW_RANGE_KEY,
									new Condition()
											.withAttributeValueList(
													new AttributeValue().withB(
															ByteBuffer.wrap(
																	start)))
											.withComparisonOperator(
													ComparisonOperator.EQ));
						}
					}
				}
				else {
					for (final QueryRequest query : queries) {
						for (final ByteArrayId adapterID : adapterIds) {
							addQueryRange(
									rowRange,
									query,
									adapterID);
						}
					}
				}
				requests.addAll(
						queries);
			}
			else {
				ranges.forEach(
						(queryRequest -> requests.addAll(
								addQueryRanges(
										tableName,
										queryRequest,
										adapterIds,
										operations.getAdapterStore()))));
			}

		}
		else if ((adapterIds != null) && !adapterIds.isEmpty()) {
			requests.addAll(getAdapterOnlyQueryRequests(
					tableName,
					adapterIds));
		}	
		
		ScanResult scanResult = operations.getClient().scan(
				scanRequest);

		iterator = Iterators.transform(
				new LazyPaginatedScan(
						scanResult,
						scanRequest,
						operations.getClient()),
				new GuavaRowTranslationHelper());
		
	}

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

	private static List<QueryRequest> getPartitionRequests(
			final String tableName ) {
		final List<QueryRequest> requests = new ArrayList<>(
				DynamoDBDataStore.PARTITIONS);
		for (long p = 0; p < (DynamoDBDataStore.PARTITIONS); p++) {
			requests.add(new QueryRequest(
					tableName).addKeyConditionsEntry(
					DynamoDBRow.GW_PARTITION_ID_KEY,
					new Condition().withComparisonOperator(
							ComparisonOperator.EQ).withAttributeValueList(
							new AttributeValue().withN(Long.toString(p)))));
		}
		return requests;
	}

	private List<QueryRequest> getAdapterOnlyQueryRequests(
			final String tableName,
			ArrayList<ByteArrayId> adapterIds ) {
		final List<QueryRequest> allQueries = new ArrayList<>();

		for (final ByteArrayId adapterId : adapterIds) {
			final List<QueryRequest> singleAdapterQueries = getPartitionRequests(tableName);
			final byte[] start = adapterId.getBytes();
			final byte[] end = adapterId.getNextPrefix();
			for (final QueryRequest queryRequest : singleAdapterQueries) {
				queryRequest.addKeyConditionsEntry(
						DynamoDBRow.GW_RANGE_KEY,
						new Condition().withComparisonOperator(
								ComparisonOperator.BETWEEN).withAttributeValueList(
								new AttributeValue().withB(ByteBuffer.wrap(start)),
								new AttributeValue().withB(ByteBuffer.wrap(end))));
			}
			allQueries.addAll(singleAdapterQueries);
		}
		return allQueries;
	}

	private void addQueryRange(
			final ByteArrayRange r,
			final QueryRequest query,
			final ByteArrayId adapterID ) {
		final byte[] start = ByteArrayUtils.combineArrays(
				adapterID.getBytes(),
				r.getStart().getBytes());
		final byte[] end = ByteArrayUtils.combineArrays(
				adapterID.getBytes(),
				r.getEndAsNextPrefix().getBytes());
		query.addKeyConditionsEntry(
				DynamoDBRow.GW_RANGE_KEY,
				new Condition().withComparisonOperator(
						ComparisonOperator.BETWEEN).withAttributeValueList(
						new AttributeValue().withB(ByteBuffer.wrap(start)),
						new AttributeValue().withB(ByteBuffer.wrap(end))));
	}

	private List<QueryRequest> addQueryRanges(
			final String tableName,
			final ByteArrayRange r,
			final List<ByteArrayId> adapterIds,
			final AdapterStore adapterStore ) {
		List<QueryRequest> retVal = null;
		if (adapterIds.isEmpty() && adapterStore != null) {
			final CloseableIterator<DataAdapter<?>> adapters = adapterStore.getAdapters();
			final List<ByteArrayId> adapterIDList = new ArrayList<ByteArrayId>();
			adapters.forEachRemaining(new Consumer<DataAdapter<?>>() {
				@Override
				public void accept(
						final DataAdapter<?> t ) {
					adapterIDList.add(t.getAdapterId());
				}
			});
			adapterIds.addAll(adapterIDList);
		}

		for (final ByteArrayId adapterId : adapterIds) {
			final List<QueryRequest> internalRequests = getPartitionRequests(tableName);
			for (final QueryRequest queryRequest : internalRequests) {
				addQueryRange(
						r,
						queryRequest,
						adapterId);
			}
			if (retVal == null) {
				retVal = internalRequests;
			}
			else {
				retVal.addAll(internalRequests);
			}
		}
		if (retVal == null) {
			return Collections.EMPTY_LIST;
		}
		return retVal;
	}

	// KAM: For reference only
	protected Iterator<Map<String, AttributeValue>> getResults(
			final double[] maxResolutionSubsamplingPerDimension,
			final Integer limit,
			final AdapterStore adapterStore,
			final List<ByteArrayId> adapterIds,
			final PrimaryIndex index,
			final List<ByteArrayRange> ranges,
			final boolean async) {
		final String tableName = operations.getQualifiedTableName(
				StringUtils.stringFromBinary(
						index.getId().getBytes()));
		if ((ranges != null) && !ranges.isEmpty()) {
			final List<QueryRequest> requests = new ArrayList<>();
			if ((ranges.size() == 1) && (adapterIds.size() == 1)) {
				final List<QueryRequest> queries = getPartitionRequests(
						tableName);
				final ByteArrayRange r = ranges.get(
						0);
				if (r.isSingleValue()) {
					for (final QueryRequest query : queries) {
						for (final ByteArrayId adapterID : adapterIds) {
							final byte[] start = ByteArrayUtils.combineArrays(
									adapterID.getBytes(),
									r.getStart().getBytes());
							query.addQueryFilterEntry(
									DynamoDBRow.GW_RANGE_KEY,
									new Condition()
											.withAttributeValueList(
													new AttributeValue().withB(
															ByteBuffer.wrap(
																	start)))
											.withComparisonOperator(
													ComparisonOperator.EQ));
						}
					}
				}
				else {
					for (final QueryRequest query : queries) {
						for (final ByteArrayId adapterID : adapterIds) {
							addQueryRange(
									r,
									query,
									adapterID);
						}
					}
				}
				requests.addAll(
						queries);
			}
			else {
				ranges.forEach(
						(r -> requests.addAll(
								addQueryRanges(
										tableName,
										r,
										adapterIds,
										adapterStore))));
			}
            if(async){
                return Iterators.concat(
                        requests.parallelStream().map(
                                this::executeAsyncQueryRequest).iterator()); 
            }
            else{
                return Iterators.concat(
                        requests.parallelStream().map(
                                this::executeQueryRequest).iterator()); 
            }

		}
		else if ((adapterIds != null) && !adapterIds.isEmpty()) {
			final List<QueryRequest> queries = getAdapterOnlyQueryRequests(
					tableName,
					adapterIds);
		}
		
		if(async){
			final ScanRequest request = new ScanRequest(
					tableName);
			return new AsyncPaginatedScan(
					request,
					operations.getClient());
		}
		else{
			// query everything
			final ScanRequest request = new ScanRequest(
					tableName);
			final ScanResult scanResult = operations.getClient().scan(
					request);
			return new LazyPaginatedScan(
					scanResult,
					request,
					operations.getClient());
		}

	}

	private List<QueryRequest> getAdapterOnlyQueryRequests(
			final String tableName,
			final List<ByteArrayId> adapterIds ) {
		final List<QueryRequest> allQueries = new ArrayList<>();

		for (final ByteArrayId adapterId : adapterIds) {
			final List<QueryRequest> singleAdapterQueries = getPartitionRequests(tableName);
			final byte[] start = adapterId.getBytes();
			final byte[] end = adapterId.getNextPrefix();
			for (final QueryRequest queryRequest : singleAdapterQueries) {
				queryRequest.addKeyConditionsEntry(
						DynamoDBRow.GW_RANGE_KEY,
						new Condition().withComparisonOperator(
								ComparisonOperator.BETWEEN).withAttributeValueList(
								new AttributeValue().withB(ByteBuffer.wrap(start)),
								new AttributeValue().withB(ByteBuffer.wrap(end))));
			}
			allQueries.addAll(singleAdapterQueries);
		}
		return allQueries;
	}

	private Iterator<Map<String, AttributeValue>> executeQueryRequest(
			final QueryRequest queryRequest ) {
		final QueryResult result = operations.getClient().query(
				queryRequest);
		return new LazyPaginatedQuery(
				result,
				queryRequest,
				operations.getClient());
	}

	/**
	 * Asynchronous version of the query request. Does not block
	 */
	public Iterator<Map<String, AttributeValue>> executeAsyncQueryRequest(
			final QueryRequest queryRequest ) {
		return new AsyncPaginatedQuery(
				queryRequest,
				operations.getClient());
	}
}
