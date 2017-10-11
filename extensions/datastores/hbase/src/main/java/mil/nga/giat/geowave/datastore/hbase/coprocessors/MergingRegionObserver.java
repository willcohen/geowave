package mil.nga.giat.geowave.datastore.hbase.coprocessors;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;

import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellScanner;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Durability;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Mutation;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.coprocessor.BaseRegionObserver;
import org.apache.hadoop.hbase.coprocessor.ObserverContext;
import org.apache.hadoop.hbase.coprocessor.RegionCoprocessorEnvironment;
import org.apache.hadoop.hbase.filter.ByteArrayComparable;
import org.apache.hadoop.hbase.filter.Filter;
import org.apache.hadoop.hbase.filter.FilterList;
import org.apache.hadoop.hbase.filter.CompareFilter.CompareOp;
import org.apache.hadoop.hbase.regionserver.InternalScanner;
import org.apache.hadoop.hbase.regionserver.MiniBatchOperationInProgress;
import org.apache.hadoop.hbase.regionserver.RegionScanner;
import org.apache.hadoop.hbase.regionserver.ScanType;
import org.apache.hadoop.hbase.regionserver.Store;
import org.apache.hadoop.hbase.regionserver.compactions.CompactionRequest;
import org.apache.hadoop.hbase.regionserver.wal.WALEdit;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;

import mil.nga.giat.geowave.core.index.StringUtils;
import mil.nga.giat.geowave.datastore.hbase.filters.HBaseMergingFilter;

public class MergingRegionObserver extends
		BaseRegionObserver
{
	private final static Logger LOGGER = Logger.getLogger(
			MergingRegionObserver.class);

	// TEST ONLY!
	static {
		LOGGER.setLevel(
				Level.DEBUG);
	}

	private HashMap<RegionScanner, HBaseMergingFilter> filterMap = new HashMap<RegionScanner, HBaseMergingFilter>();

	@Override
	public void preBatchMutate(
			final ObserverContext<RegionCoprocessorEnvironment> c,
			final MiniBatchOperationInProgress<Mutation> miniBatchOp )
			throws IOException {
		TableName tableName = c.getEnvironment().getRegionInfo().getTable();

		if (!tableName.isSystemTable()) {
			LOGGER.debug(
					">>> preBatchMutate for table: " + tableName.getNameAsString() +
					"; batch size = " +	miniBatchOp.size());
			for (int i = 0; i < miniBatchOp.size(); i++) {
				Mutation mutation = miniBatchOp.getOperation(i);
				if (mutation instanceof Put) {
					Put put = (Put)mutation;
					// TODO: Retrieve existing row if possible;
					// merge values and put.
				}
			}
		}

	}

	@Override
	public InternalScanner preFlush(
			ObserverContext<RegionCoprocessorEnvironment> e,
			Store store,
			InternalScanner scanner )
			throws IOException {
		TableName tableName = e.getEnvironment().getRegionInfo().getTable();

		if (!tableName.isSystemTable()) {
			LOGGER.debug(
					">>> preFlush for table: " + store.getTableName());
		}

		return super.preFlush(
				e,
				store,
				scanner);
	}

	@Override
	public InternalScanner preCompact(
			ObserverContext<RegionCoprocessorEnvironment> e,
			final Store store,
			final InternalScanner scanner,
			final ScanType scanType,
			CompactionRequest request )
			throws IOException {
		TableName tableName = e.getEnvironment().getRegionInfo().getTable();

		if (!tableName.isSystemTable()) {
			LOGGER.debug(
					">>> preCompact for table: " + store.getTableName());
		}

		return super.preCompact(
				e,
				store,
				scanner,
				scanType,
				request);
	}

	@Override
	public RegionScanner postScannerOpen(
			final ObserverContext<RegionCoprocessorEnvironment> e,
			final Scan scan,
			final RegionScanner s )
			throws IOException {
		TableName tableName = e.getEnvironment().getRegionInfo().getTable();

		if (!tableName.isSystemTable()) {
			if (scan != null) {
				Filter scanFilter = scan.getFilter();
				if (scanFilter != null) {
					HBaseMergingFilter mergingFilter = extractMergingFilter(
							scanFilter);

					if (mergingFilter != null) {
						filterMap.put(
								s,
								mergingFilter);
					}
				}
			}
		}

		return super.postScannerOpen(
				e,
				scan,
				s);
	}

	private HBaseMergingFilter extractMergingFilter(
			Filter checkFilter ) {
		if (checkFilter instanceof HBaseMergingFilter) {
			return (HBaseMergingFilter) checkFilter;
		}

		if (checkFilter instanceof FilterList) {
			for (Filter filter : ((FilterList) checkFilter).getFilters()) {
				HBaseMergingFilter mergingFilter = extractMergingFilter(
						filter);
				if (mergingFilter != null) {
					return mergingFilter;
				}
			}
		}

		return null;
	}

	@Override
	public boolean preScannerNext(
			final ObserverContext<RegionCoprocessorEnvironment> e,
			final InternalScanner s,
			final List<Result> results,
			final int limit,
			final boolean hasMore )
			throws IOException {
		TableName tableName = e.getEnvironment().getRegionInfo().getTable();

		if (!tableName.isSystemTable()) {
			HBaseMergingFilter mergingFilter = filterMap.get(
					s);

			if (mergingFilter != null) {
				// TODO: Any pre-scan work?
			}
		}

		return super.preScannerNext(
				e,
				s,
				results,
				limit,
				hasMore);
	}

	@Override
	public boolean postScannerNext(
			final ObserverContext<RegionCoprocessorEnvironment> e,
			final InternalScanner s,
			final List<Result> results,
			final int limit,
			final boolean hasMore )
			throws IOException {
		TableName tableName = e.getEnvironment().getRegionInfo().getTable();

		if (!tableName.isSystemTable()) {
			HBaseMergingFilter mergingFilter = filterMap.get(
					s);

			if (results.size() > 1) {
				LOGGER.debug(
						">> PostScannerNext has " + results.size() + " rows");

				HashMap<String, List<Result>> rowMap = new HashMap<String, List<Result>>();

				for (Result result : results) {
					byte[] row = result.getRow();

					if (row != null) {
						String rowKey = StringUtils.stringFromBinary(
								row);
						List<Result> resultList = rowMap.get(
								rowKey);

						if (resultList == null) {
							resultList = new ArrayList<Result>();
						}

						resultList.add(
								result);
						rowMap.put(
								rowKey,
								resultList);
					}
				}

				if (!rowMap.isEmpty()) {
					LOGGER.debug(
							">> PostScannerNext got " + rowMap.keySet().size() + " unique rows");
					for (String rowKey : rowMap.keySet()) {
						List<Result> resultList = rowMap.get(
								rowKey);
						LOGGER.debug(
								">> PostScannerNext got " + resultList.size() + " results for row " + rowKey);
					}
				}
			}
		}

		return super.postScannerNext(
				e,
				s,
				results,
				limit,
				hasMore);
	}
}
