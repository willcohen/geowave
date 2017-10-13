package mil.nga.giat.geowave.datastore.hbase.coprocessors;

import java.io.IOException;
import java.util.HashSet;
import java.util.List;
import java.util.Map.Entry;
import java.util.NavigableMap;
import java.util.Set;

import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Mutation;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.coprocessor.BaseRegionObserver;
import org.apache.hadoop.hbase.coprocessor.ObserverContext;
import org.apache.hadoop.hbase.coprocessor.RegionCoprocessorEnvironment;
import org.apache.hadoop.hbase.filter.Filter;
import org.apache.hadoop.hbase.filter.FilterList;
import org.apache.hadoop.hbase.regionserver.InternalScanner;
import org.apache.hadoop.hbase.regionserver.MiniBatchOperationInProgress;
import org.apache.hadoop.hbase.regionserver.RegionScanner;
import org.apache.hadoop.hbase.regionserver.ScanType;
import org.apache.hadoop.hbase.regionserver.Store;
import org.apache.hadoop.hbase.regionserver.compactions.CompactionRequest;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;

import mil.nga.giat.geowave.core.index.ByteArrayId;
import mil.nga.giat.geowave.core.index.Mergeable;
import mil.nga.giat.geowave.core.index.PersistenceUtils;
import mil.nga.giat.geowave.datastore.hbase.operations.HBaseOperations;

public class MergingRegionObserver extends
		BaseRegionObserver
{
	private final static Logger LOGGER = Logger.getLogger(
			MergingRegionObserver.class);

	public final static String COLUMN_FAMILIES_CONFIG_KEY = "hbase.coprocessor.merging.columnfamilies";

	// TEST ONLY!
	static {
		LOGGER.setLevel(
				Level.DEBUG);
	}

	private static HashSet<ByteArrayId> mergingColumnFamilies = null;

	private void updateMergingColumnFamilies(
			String mergeData ) {
		if (mergingColumnFamilies == null) {
			mergingColumnFamilies = new HashSet<>();
		}

		LOGGER.debug(
				"Updating CFs from message: " + mergeData);

		Set<ByteArrayId> adapterIdList = HBaseOperations.adapterIdsFromMergeData(
				mergeData);

		if (adapterIdList != null && !adapterIdList.isEmpty()) {
			for (ByteArrayId adapterId : adapterIdList) {
				if (!mergingColumnFamilies.contains(
						adapterId)) {
					mergingColumnFamilies.add(
							adapterId);

					LOGGER.debug(
							"Added CF: " + adapterId.getString() + " from filter message");
				}
			}
		}
	}

	public void preBatchMutateExperiment(
			final ObserverContext<RegionCoprocessorEnvironment> c,
			final MiniBatchOperationInProgress<Mutation> miniBatchOp )
			throws IOException {
		RegionCoprocessorEnvironment env = c.getEnvironment();
		TableName tableName = env.getRegionInfo().getTable();

		if (!tableName.isSystemTable()) {
			// TEST ONLY!
			if (!tableName.getNameAsString().equals(
					"mil_nga_giat_geowave_test_SPATIAL_IDX")) {
				return;
			}

			LOGGER.debug(
					">>> preBatchMutate for table: " + tableName.getNameAsString() + "; batch size = "
							+ miniBatchOp.size());

			for (int i = 0; i < miniBatchOp.size(); i++) {
				Mutation mutation = miniBatchOp.getOperation(
						i);
				if (mutation instanceof Put) {
					Put put = (Put) mutation;

					// Get column family(ies) from put and check against merge
					// list
					NavigableMap<byte[], List<Cell>> familyMap = put.getFamilyCellMap();
					for (Entry<byte[], List<Cell>> entry : familyMap.entrySet()) {
						Mergeable mergedValue = null;

						for (Cell cell : entry.getValue()) {
							ByteArrayId family = new ByteArrayId(
									CellUtil.cloneFamily(
											cell));
							LOGGER.debug(
									"Put has CF: " + family.getString());
							if (mergingColumnFamilies.contains(
									family)) {

								Mergeable value = (Mergeable) PersistenceUtils.fromBinary(
										CellUtil.cloneValue(
												cell),
										Mergeable.class);
								if (value != null) {
									if (mergedValue == null) {
										mergedValue = value;
									}
									else {
										mergedValue.merge(
												value);
									}
								}
								else {
									LOGGER.debug(
											"Cell value is not Mergeable!");
								}
							}
						}

						// Retrieve existing row if possible
						if (mergedValue != null) {
							LOGGER.debug(
									">>> Getting existing row for merge...");
							Table mergeTable = env.getTable(
									tableName);

							Get get = new Get(
									put.getRow());
							Result result = mergeTable.get(
									get);

							if (result != null && !result.isEmpty()) {
								// merge values
								LOGGER.debug(
										">>> MERGING! " + result.toString());

								for (Cell cell : result.listCells()) {
									Mergeable value = (Mergeable) PersistenceUtils.fromBinary(
											CellUtil.cloneValue(
													cell),
											Mergeable.class);

									mergedValue.merge(
											value);
								}

								// TODO: update the Put w/ merged value,
								// or do a new put and cancel this one?
								LOGGER.debug(
										">>> MERGED");
							}
						}
					}

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
		RegionCoprocessorEnvironment env = e.getEnvironment();
		TableName tableName = env.getRegionInfo().getTable();

		if (!tableName.isSystemTable()) {
			// TEST ONLY!
			if (!tableName.getNameAsString().equals(
					"mil_nga_giat_geowave_test_SPATIAL_IDX")) {
				return scanner;
			}

			LOGGER.debug(
					">>> preFlush for table: " + tableName.getNameAsString());

			return new MergingInternalScanner(
					scanner);
		}

		return scanner;
	}

	@Override
	public InternalScanner preCompact(
			ObserverContext<RegionCoprocessorEnvironment> e,
			final Store store,
			final InternalScanner scanner,
			final ScanType scanType,
			CompactionRequest request )
			throws IOException {
		RegionCoprocessorEnvironment env = e.getEnvironment();
		TableName tableName = env.getRegionInfo().getTable();

		if (!tableName.isSystemTable()) {
			// TEST ONLY!
			if (!tableName.getNameAsString().equals(
					"mil_nga_giat_geowave_test_SPATIAL_IDX")) {
				return scanner;
			}

			LOGGER.debug(
					">>> preCompact for table: " + tableName.getNameAsString());

			return new MergingInternalScanner(
					scanner);
		}

		return scanner;
	}

	@Override
	public RegionScanner preScannerOpen(
			final ObserverContext<RegionCoprocessorEnvironment> e,
			final Scan scan,
			final RegionScanner s )
			throws IOException {
		TableName tableName = e.getEnvironment().getRegionInfo().getTable();

		if (!tableName.isSystemTable()) {
			LOGGER.debug(
					">>> preScannerOpen for table: " + tableName.getNameAsString());

			if (scan != null) {
				Filter scanFilter = scan.getFilter();
				if (scanFilter != null) {
					MergeDataMessage mergeDataMessage = extractMergeData(
							scanFilter);

					if (mergeDataMessage != null) {
						updateMergingColumnFamilies(
								mergeDataMessage.getMergeData());

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
			LOGGER.debug(
					">>> postScannerOpen for table: " + tableName.getNameAsString());
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
				MergeDataMessage mergingFilter = extractMergeData(
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
			LOGGER.debug(
					">>> preScannerNext for table: " + tableName.getNameAsString());
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
			LOGGER.debug(
					">>> postScannerNext for table: " + tableName.getNameAsString());
		}

		return super.postScannerNext(
				e,
				s,
				results,
				limit,
				hasMore);
	}
}
