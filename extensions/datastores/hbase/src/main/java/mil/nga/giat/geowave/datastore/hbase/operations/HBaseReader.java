package mil.nga.giat.geowave.datastore.hbase.operations;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.filter.FilterList;
import org.apache.hadoop.hbase.filter.MultiRowRangeFilter;
import org.apache.hadoop.hbase.filter.MultiRowRangeFilter.RowRange;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import mil.nga.giat.geowave.core.index.ByteArrayId;
import mil.nga.giat.geowave.core.index.ByteArrayRange;
import mil.nga.giat.geowave.core.index.IndexUtils;
import mil.nga.giat.geowave.core.index.Mergeable;
import mil.nga.giat.geowave.core.index.MultiDimensionalCoordinateRangesArray;
import mil.nga.giat.geowave.core.index.PersistenceUtils;
import mil.nga.giat.geowave.core.store.entities.GeoWaveRow;
import mil.nga.giat.geowave.core.store.entities.GeoWaveRowImpl;
import mil.nga.giat.geowave.core.store.entities.GeoWaveValue;
import mil.nga.giat.geowave.core.store.entities.GeoWaveValueImpl;
import mil.nga.giat.geowave.core.store.filter.DistributableQueryFilter;
import mil.nga.giat.geowave.core.store.operations.BaseReaderParams;
import mil.nga.giat.geowave.core.store.operations.Reader;
import mil.nga.giat.geowave.core.store.operations.ReaderParams;
import mil.nga.giat.geowave.datastore.hbase.HBaseRow;
import mil.nga.giat.geowave.datastore.hbase.filters.FixedCardinalitySkippingFilter;
import mil.nga.giat.geowave.datastore.hbase.filters.HBaseDistributableFilter;
import mil.nga.giat.geowave.datastore.hbase.filters.HBaseNumericIndexStrategyFilter;
import mil.nga.giat.geowave.mapreduce.splits.GeoWaveRowRange;
import mil.nga.giat.geowave.mapreduce.splits.RecordReaderParams;

public class HBaseReader implements
		Reader
{
	private final static Logger LOGGER = LoggerFactory.getLogger(HBaseReader.class);

	private final ReaderParams readerParams;
	private final RecordReaderParams recordReaderParams;
	private final HBaseOperations operations;

	private ResultScanner scanner;
	private Iterator<Result> scanIt;

	private final boolean wholeRowEncoding;
	private final int partitionKeyLength;

	private Mergeable aggTotal = null;
	private boolean aggReady = false;

	public HBaseReader(
			final ReaderParams readerParams,
			final HBaseOperations operations ) {
		this.readerParams = readerParams;
		this.recordReaderParams = null;
		this.operations = operations;

		this.partitionKeyLength = readerParams.getIndex().getIndexStrategy().getPartitionKeyLength();
		this.wholeRowEncoding = readerParams.isMixedVisibility() && !readerParams.isServersideAggregation();

		if (readerParams.isServersideAggregation()) {
			this.scanner = null;
			this.scanIt = null;
			aggTotal = operations.aggregateServerSide(readerParams);
			aggReady = aggTotal != null;
		}
		else {
			initScanner();
		}
	}

	public HBaseReader(
			final RecordReaderParams recordReaderParams,
			final HBaseOperations operations ) {
		this.readerParams = null;
		this.recordReaderParams = recordReaderParams;
		this.operations = operations;

		this.partitionKeyLength = recordReaderParams.getIndex().getIndexStrategy().getPartitionKeyLength();
		this.wholeRowEncoding = recordReaderParams.isMixedVisibility() && !recordReaderParams.isServersideAggregation();

		initRecordScanner();
	}

	@Override
	public void close()
			throws Exception {
		if (scanner != null) {
			scanner.close();
		}
	}

	@Override
	public boolean hasNext() {
		if (scanner != null) { // not aggregation
			return scanIt.hasNext();
		}

		// This is a broken scanner situation
		if (!operations.isServerSideLibraryEnabled()) {
			return false;
		}

		// ready for agg result
		return aggReady;
	}

	@Override
	public GeoWaveRow next() {
		if (scanner != null) { // not aggregation
			final Result entry = scanIt.next();

			return new HBaseRow(
					entry,
					partitionKeyLength);
		}

		// Otherwise, server-side aggregation
		// Wrap the mergeable result in a GeoWaveRow and return it
		aggReady = false;

		return new GeoWaveRowImpl(
				null,
				new GeoWaveValue[] {
					new GeoWaveValueImpl(
							null,
							null,
							PersistenceUtils.toBinary(aggTotal))
				});
	}

	protected void initRecordScanner() {
		final FilterList filterList = new FilterList();
		final GeoWaveRowRange range = recordReaderParams.getRowRange();

		final Scan rscanner = createStandardScanner(recordReaderParams);
		rscanner.setStartRow(range.getStartSortKey());
		rscanner.setStopRow(range.getEndSortKey());

		if (operations.isServerSideLibraryEnabled()) {
			addSkipFilter(
					recordReaderParams,
					filterList);

			// Add distributable filters if requested, this has to be last
			// in the filter list for the dedupe filter to work correctly

			if (recordReaderParams.getFilter() != null) {
				addDistFilter(
						recordReaderParams,
						filterList);
			}
			else {
				addIndexFilter(
						recordReaderParams,
						filterList);
			}
		}

		if (!filterList.getFilters().isEmpty()) {
			rscanner.setFilter(filterList);
		}

		try {
			this.scanner = operations.getScannedResults(
					rscanner,
					recordReaderParams.getIndex().getId().getString(),
					recordReaderParams.getAdditionalAuthorizations());
		}
		catch (final IOException e) {
			LOGGER.error(
					"Could not get the results from scanner",
					e);
			this.scanner = null;
			return;
		}

		this.scanIt = scanner.iterator();
	}

	protected void initScanner() {
		final FilterList filterList = new FilterList();

		final Scan multiScanner = getMultiScanner(filterList);

		if (operations.isServerSideLibraryEnabled()) {
			addSkipFilter(
					readerParams,
					filterList);

			// Add distributable filters if requested, this has to be last
			// in the filter list for the dedupe filter to work correctly

			if (readerParams.getFilter() != null) {
				addDistFilter(
						readerParams,
						filterList);
			}
			else {
				addIndexFilter(
						readerParams,
						filterList);
			}
		}

		if (!filterList.getFilters().isEmpty()) {
			multiScanner.setFilter(filterList);
		}

		try {
			this.scanner = operations.getScannedResults(
					multiScanner,
					readerParams.getIndex().getId().getString(),
					readerParams.getAdditionalAuthorizations());
		}
		catch (final IOException e) {
			LOGGER.error(
					"Could not get the results from scanner",
					e);

			this.scanner = null;
			return;
		}

		this.scanIt = scanner.iterator();
	}

	private void addSkipFilter(
			BaseReaderParams params,
			FilterList filterList ) {
		// Add skipping filter if requested
		if (params.getMaxResolutionSubsamplingPerDimension() != null) {
			if (params.getMaxResolutionSubsamplingPerDimension().length != params
					.getIndex()
					.getIndexStrategy()
					.getOrderedDimensionDefinitions().length) {
				LOGGER.warn("Unable to subsample for table '" + params.getIndex().getId().getString()
						+ "'. Subsample dimensions = " + params.getMaxResolutionSubsamplingPerDimension().length
						+ " when indexed dimensions = "
						+ params.getIndex().getIndexStrategy().getOrderedDimensionDefinitions().length);
			}
			else {
				final int cardinalityToSubsample = IndexUtils.getBitPositionFromSubsamplingArray(
						params.getIndex().getIndexStrategy(),
						params.getMaxResolutionSubsamplingPerDimension());

				final FixedCardinalitySkippingFilter skippingFilter = new FixedCardinalitySkippingFilter(
						cardinalityToSubsample);
				filterList.addFilter(skippingFilter);
			}
		}
	}

	private void addDistFilter(
			BaseReaderParams params,
			FilterList filterList ) {
		final HBaseDistributableFilter hbdFilter = new HBaseDistributableFilter();
		if (wholeRowEncoding) {
			hbdFilter.setWholeRowFilter(true);
		}

		hbdFilter.setPartitionKeyLength(partitionKeyLength);

		final List<DistributableQueryFilter> distFilters = new ArrayList();
		distFilters.add(params.getFilter());
		hbdFilter.init(
				distFilters,
				params.getIndex().getIndexModel(),
				params.getAdditionalAuthorizations());

		filterList.addFilter(hbdFilter);
	}

	private void addIndexFilter(
			BaseReaderParams params,
			FilterList filterList ) {
		final List<MultiDimensionalCoordinateRangesArray> coords = params.getCoordinateRanges();
		if ((coords != null) && !coords.isEmpty()) {
			final HBaseNumericIndexStrategyFilter numericIndexFilter = new HBaseNumericIndexStrategyFilter(
					params.getIndex().getIndexStrategy(),
					coords.toArray(new MultiDimensionalCoordinateRangesArray[] {}));
			filterList.addFilter(numericIndexFilter);
		}
	}

	protected Scan getMultiScanner(
			final FilterList filterList ) {
		// Single scan w/ multiple ranges
		final Scan multiScanner = createStandardScanner(readerParams);

		final List<ByteArrayRange> ranges = readerParams.getQueryRanges().getCompositeQueryRanges();

		final MultiRowRangeFilter filter = operations.getMultiRowRangeFilter(ranges);
		if (filter != null) {
			filterList.addFilter(filter);

			final List<RowRange> rowRanges = filter.getRowRanges();
			multiScanner.setStartRow(rowRanges.get(
					0).getStartRow());

			final RowRange stopRowRange = rowRanges.get(rowRanges.size() - 1);
			byte[] stopRowExclusive;
			if (stopRowRange.isStopRowInclusive()) {
				// because the end is always exclusive, to make an inclusive
				// stop row into exlusive all we need to do is add a traling 0
				stopRowExclusive = new byte[stopRowRange.getStopRow().length + 1];

				System.arraycopy(
						stopRowRange.getStopRow(),
						0,
						stopRowExclusive,
						0,
						stopRowExclusive.length - 1);
			}
			else {
				stopRowExclusive = stopRowRange.getStopRow();
			}
			multiScanner.setStopRow(stopRowExclusive);
		}

		return multiScanner;
	}

	protected Scan createStandardScanner(
			BaseReaderParams readerParams ) {
		final Scan scanner = new Scan();

		// Performance tuning per store options
		scanner.setCaching(operations.getScanCacheSize());
		scanner.setCacheBlocks(operations.isEnableBlockCache());

		// Only return the most recent version
		scanner.setMaxVersions(1);

		if ((readerParams.getAdapterIds() != null) && !readerParams.getAdapterIds().isEmpty()) {
			for (final ByteArrayId adapterId : readerParams.getAdapterIds()) {
				scanner.addFamily(adapterId.getBytes());
			}
		}

		if ((readerParams.getLimit() != null) && (readerParams.getLimit() > 0)
				&& (readerParams.getLimit() < scanner.getBatch())) {
			scanner.setBatch(readerParams.getLimit());
		}

		return scanner;
	}
}
