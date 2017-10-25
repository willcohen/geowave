package mil.nga.giat.geowave.datastore.hbase.coprocessors;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;

import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.coprocessor.BaseRegionObserver;
import org.apache.hadoop.hbase.coprocessor.ObserverContext;
import org.apache.hadoop.hbase.coprocessor.RegionCoprocessorEnvironment;
import org.apache.hadoop.hbase.filter.Filter;
import org.apache.hadoop.hbase.filter.FilterList;
import org.apache.hadoop.hbase.regionserver.InternalScanner;
import org.apache.hadoop.hbase.regionserver.KeyValueScanner;
import org.apache.hadoop.hbase.regionserver.RegionScanner;
import org.apache.hadoop.hbase.regionserver.ScanType;
import org.apache.hadoop.hbase.regionserver.Store;
import org.apache.hadoop.hbase.regionserver.StoreFile;
import org.apache.hadoop.hbase.regionserver.compactions.CompactionRequest;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;

import mil.nga.giat.geowave.core.index.ByteArrayId;
import mil.nga.giat.geowave.core.store.adapter.RowMergingDataAdapter.RowTransform;

public class MergingRegionObserver extends
		BaseRegionObserver
{
	private final static Logger LOGGER = Logger.getLogger(MergingRegionObserver.class);

	public final static String COLUMN_FAMILIES_CONFIG_KEY = "hbase.coprocessor.merging.columnfamilies";

	// TEST ONLY!
	static {
		LOGGER.setLevel(Level.DEBUG);
	}

	private HashSet<String> mergingTables = new HashSet<>();
	private HashMap<ByteArrayId, RowTransform> mergingTransformMap = new HashMap<>();

	@Override
	public InternalScanner preFlushScannerOpen(
			final ObserverContext<RegionCoprocessorEnvironment> e,
			final Store store,
			final KeyValueScanner memstoreScanner,
			final InternalScanner s ) {
		TableName tableName = e.getEnvironment().getRegionInfo().getTable();

		if (!tableName.isSystemTable()) {
			String tableNameString = tableName.getNameAsString();

			if (mergingTables.contains(tableNameString)) {
				MergingInternalScanner mergingScanner = new MergingInternalScanner(
						memstoreScanner);

				mergingScanner.setTransformMap(mergingTransformMap);

				LOGGER.debug(">>> preFlushScannerOpen for merging table: " + tableNameString + " with scanner "
						+ mergingScanner.getId());

				return mergingScanner;
			}
		}

		return s;
	}

	@Override
	public InternalScanner preFlush(
			ObserverContext<RegionCoprocessorEnvironment> e,
			Store store,
			InternalScanner scanner )
			throws IOException {
		if (scanner instanceof MergingInternalScanner) {
			String id = ((MergingInternalScanner) scanner).getId();
			LOGGER.debug(">>> preFlush w/ scanner " + id);
		}

		return scanner;
	}

	@Override
	public InternalScanner preCompactScannerOpen(
			final ObserverContext<RegionCoprocessorEnvironment> e,
			final Store store,
			List<? extends KeyValueScanner> scanners,
			final ScanType scanType,
			final long earliestPutTs,
			final InternalScanner s,
			CompactionRequest request )
			throws IOException {
		TableName tableName = e.getEnvironment().getRegionInfo().getTable();

		if (!tableName.isSystemTable()) {
			String tableNameString = tableName.getNameAsString();

			if (mergingTables.contains(tableNameString)) {
				LOGGER.debug(">>> preCompact for merging table: " + tableNameString);

				MergingInternalScanner mergingScanner = new MergingInternalScanner(
						scanners);

				mergingScanner.setTransformMap(mergingTransformMap);

				return mergingScanner;
			}
		}

		return s;
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
			String tableNameString = tableName.getNameAsString();

			if (mergingTables.contains(tableNameString)) {
				LOGGER.debug(">>> preScannerNext for merging table: " + tableName.getNameAsString());

				if (s instanceof MergingRegionScanner) {
					String id = ((MergingRegionScanner) s).getId();
					LOGGER.debug(">>> preScannerNext using RegionScanner " + id);
				}
			}
		}

		return hasMore;
	}

	@Override
	public RegionScanner preScannerOpen(
			final ObserverContext<RegionCoprocessorEnvironment> e,
			final Scan scan,
			final RegionScanner s )
			throws IOException {
		TableName tableName = e.getEnvironment().getRegionInfo().getTable();

		if (!tableName.isSystemTable()) {
			// check for merge data messages
			if (scan != null) {
				Filter scanFilter = scan.getFilter();
				if (scanFilter != null) {
					MergeDataMessage mergeDataMessage = extractMergeData(scanFilter);

					if (mergeDataMessage != null) {
						updateMergingColumnFamilies(mergeDataMessage);

						e.bypass();
						e.complete();

						return null;
					}
				}
			}
		}

		return s;
	}

	@Override
	public RegionScanner postScannerOpen(
			final ObserverContext<RegionCoprocessorEnvironment> e,
			final Scan scan,
			final RegionScanner s )
			throws IOException {
		TableName tableName = e.getEnvironment().getRegionInfo().getTable();

		if (!tableName.isSystemTable()) {
			String tableNameString = tableName.getNameAsString();

			if (mergingTables.contains(tableNameString)) {
				LOGGER.debug(">>> postScannerOpen for merging table: " + tableNameString);

				// Use merging scanner here
				MergingRegionScanner mergingScanner = new MergingRegionScanner(
						s);
				mergingScanner.setTransformMap(mergingTransformMap);

				e.bypass();

				return mergingScanner;
			}
		}

		return s;
	}

	private MergeDataMessage extractMergeData(
			Filter checkFilter ) {
		if (checkFilter instanceof MergeDataMessage) {
			return (MergeDataMessage) checkFilter;
		}

		if (checkFilter instanceof FilterList) {
			for (Filter filter : ((FilterList) checkFilter).getFilters()) {
				MergeDataMessage mergingFilter = extractMergeData(filter);
				if (mergingFilter != null) {
					return mergingFilter;
				}
			}
		}

		return null;
	}

	private void updateMergingColumnFamilies(
			MergeDataMessage mergeDataMessage ) {
		LOGGER.debug("Updating CF from message: " + mergeDataMessage.getAdapterId().getString());

		String tableName = mergeDataMessage.getTableName().getString();
		if (!mergingTables.contains(tableName)) {
			mergingTables.add(tableName);
		}

		if (!mergingTransformMap.containsKey(mergeDataMessage.getAdapterId())) {
			RowTransform rowTransform = mergeDataMessage.getTransformData();

			try {
				rowTransform.initOptions(mergeDataMessage.getOptions());
			}
			catch (IOException e) {
				LOGGER.error(
						"Error initializing row transform",
						e);
			}

			mergingTransformMap.put(
					mergeDataMessage.getAdapterId(),
					rowTransform);
		}
	}
}
