/*******************************************************************************
 * Copyright (c) 2013-2017 Contributors to the Eclipse Foundation
 * 
 * See the NOTICE file distributed with this work for additional
 * information regarding copyright ownership.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License,
 * Version 2.0 which accompanies this distribution and is available at
 * http://www.apache.org/licenses/LICENSE-2.0.txt
 ******************************************************************************/
package mil.nga.giat.geowave.datastore.hbase.operations;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.Coprocessor;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.HRegionLocation;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableExistsException;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.TableNotFoundException;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.BufferedMutator;
import org.apache.hadoop.hbase.client.BufferedMutatorParams;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.RegionLocator;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.client.coprocessor.Batch;
import org.apache.hadoop.hbase.filter.MultiRowRangeFilter;
import org.apache.hadoop.hbase.filter.MultiRowRangeFilter.RowRange;
import org.apache.hadoop.hbase.ipc.BlockingRpcCallback;
import org.apache.hadoop.hbase.security.visibility.Authorizations;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.Iterators;
import com.google.protobuf.ByteString;

import mil.nga.giat.geowave.core.index.ByteArrayId;
import mil.nga.giat.geowave.core.index.ByteArrayRange;
import mil.nga.giat.geowave.core.index.Mergeable;
import mil.nga.giat.geowave.core.index.MultiDimensionalCoordinateRangesArray;
import mil.nga.giat.geowave.core.index.PersistenceUtils;
import mil.nga.giat.geowave.core.index.StringUtils;
import mil.nga.giat.geowave.core.store.CloseableIterator;
import mil.nga.giat.geowave.core.store.CloseableIteratorWrapper;
import mil.nga.giat.geowave.core.store.adapter.AdapterIndexMappingStore;
import mil.nga.giat.geowave.core.store.adapter.AdapterStore;
import mil.nga.giat.geowave.core.store.adapter.DataAdapter;
import mil.nga.giat.geowave.core.store.adapter.RowMergingDataAdapter;
import mil.nga.giat.geowave.core.store.entities.GeoWaveRow;
import mil.nga.giat.geowave.core.store.filter.DistributableQueryFilter;
import mil.nga.giat.geowave.core.store.index.PrimaryIndex;
import mil.nga.giat.geowave.core.store.metadata.AbstractGeoWavePersistence;
import mil.nga.giat.geowave.core.store.operations.Deleter;
import mil.nga.giat.geowave.core.store.operations.MetadataDeleter;
import mil.nga.giat.geowave.core.store.operations.MetadataReader;
import mil.nga.giat.geowave.core.store.operations.MetadataType;
import mil.nga.giat.geowave.core.store.operations.MetadataWriter;
import mil.nga.giat.geowave.core.store.operations.Reader;
import mil.nga.giat.geowave.core.store.operations.ReaderParams;
import mil.nga.giat.geowave.core.store.operations.Writer;
import mil.nga.giat.geowave.core.store.query.aggregate.Aggregation;
import mil.nga.giat.geowave.core.store.query.aggregate.CommonIndexAggregation;
import mil.nga.giat.geowave.datastore.hbase.HBaseRow;
import mil.nga.giat.geowave.datastore.hbase.cli.config.HBaseOptions;
import mil.nga.giat.geowave.datastore.hbase.cli.config.HBaseRequiredOptions;
import mil.nga.giat.geowave.datastore.hbase.query.AggregationEndpoint;
import mil.nga.giat.geowave.datastore.hbase.query.HBaseDistributableFilter;
import mil.nga.giat.geowave.datastore.hbase.query.HBaseNumericIndexStrategyFilter;
import mil.nga.giat.geowave.datastore.hbase.query.protobuf.AggregationProtos;
import mil.nga.giat.geowave.datastore.hbase.util.ConnectionPool;
import mil.nga.giat.geowave.datastore.hbase.util.HBaseUtils;
import mil.nga.giat.geowave.datastore.hbase.util.HBaseUtils.ScannerClosableWrapper;
import mil.nga.giat.geowave.datastore.hbase.util.RewritingMergingEntryIterator;
import mil.nga.giat.geowave.mapreduce.MapReduceDataStoreOperations;
import mil.nga.giat.geowave.mapreduce.splits.RecordReaderParams;

public class HBaseOperations implements
		MapReduceDataStoreOperations
{
	private final static Logger LOGGER = LoggerFactory.getLogger(
			HBaseOperations.class);
	protected static final String DEFAULT_TABLE_NAMESPACE = "";
	public static final Object ADMIN_MUTEX = new Object();
	private static final long SLEEP_INTERVAL = 10000L;

	private final Connection conn;
	private final String tableNamespace;
	private final boolean schemaUpdateEnabled;
	private final HashMap<String, List<String>> coprocessorCache = new HashMap<>();
	private final Map<String, Set<ByteArrayId>> partitionCache = new HashMap<>();

	private final HBaseOptions options;

	public static final String[] METADATA_CFS = new String[] {
		MetadataType.AIM.name(),
		MetadataType.ADAPTER.name(),
		MetadataType.STATS.name(),
		MetadataType.INDEX.name()
	};

	private static final boolean ASYNC_WAIT = false;

	public HBaseOperations(
			final Connection connection,
			final String geowaveNamespace,
			final HBaseOptions options )
			throws IOException {
		conn = connection;
		tableNamespace = geowaveNamespace;

		schemaUpdateEnabled = conn.getConfiguration().getBoolean(
				"hbase.online.schema.update.enable",
				false);

		this.options = options;
	}

	public HBaseOperations(
			final String zookeeperInstances,
			final String geowaveNamespace,
			final HBaseOptions options )
			throws IOException {
		conn = ConnectionPool.getInstance().getConnection(
				zookeeperInstances);
		tableNamespace = geowaveNamespace;

		schemaUpdateEnabled = conn.getConfiguration().getBoolean(
				"hbase.online.schema.update.enable",
				false);

		this.options = options;
	}

	public static HBaseOperations createOperations(
			final HBaseRequiredOptions options )
			throws IOException {
		return new HBaseOperations(
				options.getZookeeper(),
				options.getGeowaveNamespace(),
				(HBaseOptions) options.getStoreOptions());
	}

	public Configuration getConfig() {
		return conn.getConfiguration();
	}

	public boolean isSchemaUpdateEnabled() {
		return schemaUpdateEnabled;
	}

	public boolean isServerSideDisabled() {
		return (options != null && !options.isServerSideDisabled());
	}

	public int getScanCacheSize() {
		if (options != null) {
			if (options.getScanCacheSize() != HConstants.DEFAULT_HBASE_CLIENT_SCANNER_CACHING) {
				return options.getScanCacheSize();
			}
		}

		// Need to get default from config.
		return 10000;
	}

	public boolean isEnableBlockCache() {
		if (options != null) {
			return options.isEnableBlockCache();
		}

		return true;
	}

	public TableName getTableName(
			final String tableName ) {
		return TableName.valueOf(
				getQualifiedTableName(
						tableName));
	}

	public HBaseWriter createWriter(
			final String sTableName,
			final String[] columnFamilies,
			final boolean createTable )
			throws IOException {
		return createWriter(
				sTableName,
				columnFamilies,
				createTable,
				null);
	}

	public HBaseWriter createWriter(
			final String sTableName,
			final String[] columnFamilies,
			final boolean createTable,
			final Set<ByteArrayId> splits )
			throws IOException {
		final TableName tableName = getTableName(
				sTableName);

		if (createTable) {
			createTable(
					columnFamilies,
					tableName);
		}

		return new HBaseWriter(
				getBufferedMutator(
						tableName),
				this,
				sTableName);
	}

	public void createTable(
			final String[] columnFamilies,
			final TableName name )
			throws IOException {
		synchronized (ADMIN_MUTEX) {
			if (!conn.getAdmin().isTableAvailable(
					name)) {
				final HTableDescriptor desc = new HTableDescriptor(
						name);
				for (final String columnFamily : columnFamilies) {
					desc.addFamily(
							new HColumnDescriptor(
									columnFamily));
				}

				try {
					conn.getAdmin().createTable(
							desc);
				}
				catch (final Exception e) {
					// We can ignore TableExists on create
					if (!(e instanceof TableExistsException)) {
						throw (e);
					}
				}
			}
			else { // add any missing column families
				addColumnFamilies(
						columnFamilies,
						name.getNameAsString());
			}
		}
	}

	protected void addColumnFamilies(
			final String[] columnFamilies,
			final String tableName )
			throws IOException {
		final TableName table = getTableName(
				tableName);
		final List<String> existingColumnFamilies = new ArrayList<>();
		final List<String> newColumnFamilies = new ArrayList<>();
		synchronized (ADMIN_MUTEX) {
			if (conn.getAdmin().isTableAvailable(
					table)) {
				final HTableDescriptor existingTableDescriptor = conn.getAdmin().getTableDescriptor(
						table);
				final HColumnDescriptor[] existingColumnDescriptors = existingTableDescriptor.getColumnFamilies();
				for (final HColumnDescriptor hColumnDescriptor : existingColumnDescriptors) {
					existingColumnFamilies.add(
							hColumnDescriptor.getNameAsString());
				}
				for (final String columnFamily : columnFamilies) {
					if (!existingColumnFamilies.contains(
							columnFamily)) {
						newColumnFamilies.add(
								columnFamily);
					}
				}
				for (final String newColumnFamily : newColumnFamilies) {
					existingTableDescriptor.addFamily(
							new HColumnDescriptor(
									newColumnFamily));
				}
				conn.getAdmin().modifyTable(
						table,
						existingTableDescriptor);
			}
		}
	}

	public String getQualifiedTableName(
			final String unqualifiedTableName ) {
		return HBaseUtils.getQualifiedTableName(
				tableNamespace,
				unqualifiedTableName);
	}

	@Override
	public void deleteAll()
			throws IOException {
		final TableName[] tableNamesArr = conn.getAdmin().listTableNames();
		for (final TableName tableName : tableNamesArr) {
			if ((tableNamespace == null) || tableName.getNameAsString().startsWith(
					tableNamespace)) {
				synchronized (ADMIN_MUTEX) {
					if (conn.getAdmin().isTableAvailable(
							tableName)) {
						conn.getAdmin().disableTable(
								tableName);
						conn.getAdmin().deleteTable(
								tableName);
					}
				}
			}
		}
	}

	@Override
	public boolean deleteAll(
			final ByteArrayId indexId,
			final ByteArrayId adapterId,
			final String... additionalAuthorizations ) {
		// final String qName = getQualifiedTableName(
		// indexId.getString());
		// try {
		// conn.getAdmin().deleteTable(
		// getTableName(
		// qName));
		// return true;
		// }
		// catch (final IOException ex) {
		// LOGGER.warn(
		// "Unable to delete table '" + qName + "'",
		// ex);
		// }
		// return false;
		// final TableName[] tableNamesArr = conn.getAdmin().listTableNames();
		// for (final TableName tableName : tableNamesArr) {
		// if ((tableNamespace == null) ||
		// tableName.getNameAsString().startsWith(
		// tableNamespace)) {
		// synchronized (ADMIN_MUTEX) {
		// if (conn.getAdmin().isTableAvailable(
		// tableName)) {
		// conn.getAdmin().disableTable(
		// tableName);
		// conn.getAdmin().deleteTable(
		// tableName);
		// }
		// }
		// }
		// }
		return false;
	}

	public boolean columnFamilyExists(
			final String tableName,
			final String columnFamily )
			throws IOException {
		synchronized (ADMIN_MUTEX) {
			final HTableDescriptor descriptor = conn.getAdmin().getTableDescriptor(
					getTableName(
							tableName));

			if (descriptor != null) {
				for (final HColumnDescriptor hColumnDescriptor : descriptor.getColumnFamilies()) {
					if (hColumnDescriptor.getNameAsString().equalsIgnoreCase(
							columnFamily)) {
						return true;
					}
				}
			}
		}
		return false;
	}

	public ResultScanner getScannedResults(
			final Scan scanner,
			final String tableName,
			final String... authorizations )
			throws IOException {
		if ((authorizations != null) && (authorizations.length > 0)) {
			scanner.setAuthorizations(
					new Authorizations(
							authorizations));
		}

		final Table table = conn.getTable(
				getTableName(
						tableName));

		final ResultScanner results = table.getScanner(
				scanner);

		table.close();

		return results;
	}

	public RegionLocator getRegionLocator(
			final String tableName )
			throws IOException {
		return conn.getRegionLocator(
				getTableName(
						tableName));
	}

	public Table getTable(
			final String tableName )
			throws IOException {
		return conn.getTable(
				getTableName(
						tableName));
	}

	public boolean verifyCoprocessor(
			final String tableNameStr,
			final String coprocessorName,
			final String coprocessorJar ) {
		try {
			// Check the cache first
			final List<String> checkList = coprocessorCache.get(
					tableNameStr);
			if (checkList != null) {
				if (checkList.contains(
						coprocessorName)) {
					return true;
				}
			}
			else {
				coprocessorCache.put(
						tableNameStr,
						new ArrayList<String>());
			}

			final Admin admin = conn.getAdmin();
			final TableName tableName = getTableName(
					tableNameStr);
			final HTableDescriptor td = admin.getTableDescriptor(
					tableName);

			if (!td.hasCoprocessor(
					coprocessorName)) {
				LOGGER.debug(
						tableNameStr + " does not have coprocessor. Adding " + coprocessorName);

				// if (!schemaUpdateEnabled &&
				// !admin.isTableDisabled(tableName)) {
				LOGGER.debug(
						"- disable table...");
				admin.disableTable(
						tableName);
				// }

				LOGGER.debug(
						"- add coprocessor...");

				// Retrieve coprocessor jar path from config
				if (coprocessorJar == null) {
					td.addCoprocessor(
							coprocessorName);
				}
				else {
					final Path hdfsJarPath = new Path(
							coprocessorJar);
					LOGGER.debug(
							"Coprocessor jar path: " + hdfsJarPath.toString());
					td.addCoprocessor(
							coprocessorName,
							hdfsJarPath,
							Coprocessor.PRIORITY_USER,
							null);
				}

				LOGGER.debug(
						"- modify table...");
				admin.modifyTable(
						tableName,
						td);

				// if (!schemaUpdateEnabled) {
				LOGGER.debug(
						"- enable table...");
				admin.enableTable(
						tableName);
			}
			// }

			// if (schemaUpdateEnabled) {
			int regionsLeft;

			do {
				regionsLeft = admin.getAlterStatus(
						tableName).getFirst();
				LOGGER.debug(
						regionsLeft + " regions remaining in table modify");

				try {
					Thread.sleep(
							SLEEP_INTERVAL);
				}
				catch (final InterruptedException e) {
					LOGGER.warn(
							"Sleeping while coprocessor add interrupted",
							e);
				}
			}
			while (regionsLeft > 0);
			// }

			LOGGER.debug(
					"Successfully added coprocessor");

			coprocessorCache.get(
					tableNameStr).add(
							coprocessorName);

			coprocessorCache.get(
					tableNameStr).add(
							coprocessorName);
		}
		catch (final IOException e) {
			LOGGER.error(
					"Error verifying/adding coprocessor.",
					e);

			return false;
		}

		return true;
	}

	@Override
	public boolean indexExists(
			final ByteArrayId indexId )
			throws IOException {
		synchronized (ADMIN_MUTEX) {
			return conn.getAdmin().isTableAvailable(
					getTableName(
							indexId.getString()));
		}
	}

	@Override
	public boolean mergeData(
			final PrimaryIndex index,
			final AdapterStore adapterStore,
			final AdapterIndexMappingStore adapterIndexMappingStore ) {
		// loop through all adapters and find row merging adapters
		final Map<ByteArrayId, RowMergingDataAdapter> map = new HashMap<>();
		final List<String> columnFamilies = new ArrayList<>();
		try (CloseableIterator<DataAdapter<?>> it = adapterStore.getAdapters()) {
			while (it.hasNext()) {
				final DataAdapter a = it.next();
				if (a instanceof RowMergingDataAdapter) {
					if (adapterIndexMappingStore.getIndicesForAdapter(
							a.getAdapterId()).contains(
									index.getId())) {
						map.put(
								a.getAdapterId(),
								(RowMergingDataAdapter) a);
						columnFamilies.add(
								a.getAdapterId().getString());
					}
				}
			}
		}
		catch (final IOException e) {
			LOGGER.error(
					"Cannot lookup data adapters",
					e);
			return false;
		}
		if (columnFamilies.isEmpty()) {
			LOGGER.warn(
					"There is no mergeable data found in datastore");
			return false;
		}
		final String table = index.getId().getString();
		try (HBaseWriter writer = createWriter(
				index.getId().getString(),
				columnFamilies.toArray(
						new String[] {}),
				false)) {
			final Scan scanner = new Scan();
			for (final String cf : columnFamilies) {
				scanner.addFamily(
						new ByteArrayId(
								cf).getBytes());
			}
			final ResultScanner rs = getScannedResults(
					scanner,
					table);

			// Get a GeoWaveRow iterator from ResultScanner
			final Iterator<GeoWaveRow> it = new CloseableIteratorWrapper<>(
					new ScannerClosableWrapper(
							rs),
					Iterators.transform(
							rs.iterator(),
							new com.google.common.base.Function<Result, GeoWaveRow>() {
								@Override
								public GeoWaveRow apply(
										Result result ) {
									return new HBaseRow(
											result,
											index.getIndexStrategy().getPartitionKeyLength());
								}

							}));

			final RewritingMergingEntryIterator iterator = new RewritingMergingEntryIterator<>(
					adapterStore,
					index,
					it,
					map,
					writer);
			while (iterator.hasNext()) {
				iterator.next();
			}
			return true;
		}
		catch (final IOException e) {
			LOGGER.error(
					"Cannot create writer for table '" + index.getId().getString() + "'",
					e);
		}
		return false;
	}

	public void insurePartition(
			final ByteArrayId partition,
			final String tableNameStr ) {
		TableName tableName = getTableName(
				tableNameStr);
		Set<ByteArrayId> existingPartitions = partitionCache.get(
				tableNameStr);

		try {
			synchronized (partitionCache) {
				if (existingPartitions == null) {
					RegionLocator regionLocator = conn.getRegionLocator(
							tableName);
					existingPartitions = new HashSet<>();

					for (byte[] startKey : regionLocator.getStartKeys()) {
						if (startKey.length > 0) {
							existingPartitions.add(
									new ByteArrayId(
											startKey));
						}
					}

					partitionCache.put(
							tableNameStr,
							existingPartitions);
				}

				if (!existingPartitions.contains(
						partition)) {
					Admin admin = conn.getAdmin();

					admin.split(
							tableName,
							partition.getBytes());

					existingPartitions.add(
							partition);

					// Split is async - do we need to wait?
					if (ASYNC_WAIT) {
						int regionsLeft;

						do {
							regionsLeft = admin.getAlterStatus(
									tableName).getFirst();
							LOGGER.debug(
									regionsLeft + " regions remaining in table modify");

							try {
								Thread.sleep(
										SLEEP_INTERVAL);
							}
							catch (final InterruptedException e) {
								LOGGER.warn(
										"Sleeping while coprocessor add interrupted",
										e);
							}
						}
						while (regionsLeft > 0);
					}

					admin.close();
				}
			}
		}
		catch (IOException e) {
			LOGGER.error(
					"Error accessing region info: " + e.getMessage());
		}
	}

	@Override
	public boolean insureAuthorizations(
			final String clientUser,
			final String... authorizations ) {
		return true;
	}

	@Override
	public Writer createWriter(
			final ByteArrayId indexId,
			final ByteArrayId adapterId ) {
		final TableName tableName = getTableName(
				indexId.getString());
		try {
			if (options.isCreateTable()) {
				String[] columnFamilies = new String[1];
				columnFamilies[0] = adapterId.getString();

				createTable(
						columnFamilies,
						tableName);
			}

			return new HBaseWriter(
					getBufferedMutator(
							tableName),
					this,
					indexId.getString());
		}
		catch (final TableNotFoundException e) {
			LOGGER.error(
					"Table does not exist",
					e);
		}
		catch (IOException e) {
			LOGGER.error(
					"Error creating table: " + indexId.getString(),
					e);
		}

		return null;
	}

	@Override
	public MetadataWriter createMetadataWriter(
			final MetadataType metadataType ) {
		final TableName tableName = getTableName(
				AbstractGeoWavePersistence.METADATA_TABLE);
		try {
			if (options.isCreateTable()) {

				createTable(
						METADATA_CFS,
						tableName);
			}

			return new HBaseMetadataWriter(
					getBufferedMutator(
							tableName),
					metadataType);
		}
		catch (IOException e) {
			LOGGER.error(
					"Error creating metadata table: " + AbstractGeoWavePersistence.METADATA_TABLE,
					e);
		}

		return null;
	}

	@Override
	public MetadataReader createMetadataReader(
			final MetadataType metadataType ) {
		return new HBaseMetadataReader(
				this,
				options,
				metadataType);
	}

	@Override
	public MetadataDeleter createMetadataDeleter(
			final MetadataType metadataType ) {
		return new HBaseMetadataDeleter(
				this,
				metadataType);
	}

	@Override
	public Reader createReader(
			final ReaderParams readerParams ) {
		return new HBaseReader(
				readerParams,
				this);
	}

	@Override
	public Reader createReader(
			final RecordReaderParams recordReaderParams ) {
		return new HBaseReader(
				recordReaderParams,
				this);
	}

	@Override
	public Deleter createDeleter(
			final ByteArrayId indexId,
			final String... authorizations )
			throws Exception {
		final TableName tableName = getTableName(
				indexId.getString());

		return new HBaseDeleter(
				getBufferedMutator(
						tableName),
				false);
	}

	public BufferedMutator getBufferedMutator(
			TableName tableName )
			throws IOException {
		final BufferedMutatorParams params = new BufferedMutatorParams(
				tableName);

		return conn.getBufferedMutator(
				params);
	}

	public MultiRowRangeFilter getMultiRowRangeFilter(
			final List<ByteArrayRange> ranges ) {
		// create the multi-row filter
		final List<RowRange> rowRanges = new ArrayList<RowRange>();
		if ((ranges == null) || ranges.isEmpty()) {
			rowRanges.add(
					new RowRange(
							HConstants.EMPTY_BYTE_ARRAY,
							true,
							HConstants.EMPTY_BYTE_ARRAY,
							false));
		}
		else {
			for (final ByteArrayRange range : ranges) {
				if (range.getStart() != null) {
					final byte[] startRow = range.getStart().getBytes();
					byte[] stopRow;
					if (!range.isSingleValue()) {
						stopRow = range.getEndAsNextPrefix().getBytes();
					}
					else {
						stopRow = range.getStart().getNextPrefix();
					}

					final RowRange rowRange = new RowRange(
							startRow,
							true,
							stopRow,
							false);

					rowRanges.add(
							rowRange);
				}
			}
		}

		// Create the multi-range filter
		try {
			return new MultiRowRangeFilter(
					rowRanges);
		}
		catch (final IOException e) {
			LOGGER.error(
					"Error creating range filter.",
					e);
		}
		return null;
	}

	public Mergeable aggregateServerSide(
			final ReaderParams readerParams ) {
		final String tableName = StringUtils.stringFromBinary(
				readerParams.getIndex().getId().getBytes());

		try {
			// Use the row count coprocessor
			if (options.isVerifyCoprocessors()) {
				verifyCoprocessor(
						tableName,
						AggregationEndpoint.class.getName(),
						options.getCoprocessorJar());
			}

			final Aggregation aggregation = readerParams.getAggregation().getRight();

			final AggregationProtos.AggregationType.Builder aggregationBuilder = AggregationProtos.AggregationType
					.newBuilder();
			aggregationBuilder.setName(
					aggregation.getClass().getName());

			if (aggregation.getParameters() != null) {
				final byte[] paramBytes = PersistenceUtils.toBinary(
						aggregation.getParameters());
				aggregationBuilder.setParams(
						ByteString.copyFrom(
								paramBytes));
			}

			final AggregationProtos.AggregationRequest.Builder requestBuilder = AggregationProtos.AggregationRequest
					.newBuilder();
			requestBuilder.setAggregation(
					aggregationBuilder.build());
			if (readerParams.getFilter() != null) {
				final List<DistributableQueryFilter> distFilters = new ArrayList();
				distFilters.add(
						readerParams.getFilter());

				final byte[] filterBytes = PersistenceUtils.toBinary(distFilters);
				final ByteString filterByteString = ByteString.copyFrom(filterBytes);
				requestBuilder.setFilter(
						filterByteString);
			}
			else {
				final List<MultiDimensionalCoordinateRangesArray> coords = readerParams.getCoordinateRanges();
				if (!coords.isEmpty()) {
					final byte[] filterBytes = new HBaseNumericIndexStrategyFilter(
							readerParams.getIndex().getIndexStrategy(),
							coords.toArray(
									new MultiDimensionalCoordinateRangesArray[] {})).toByteArray();
					final ByteString filterByteString = ByteString.copyFrom(
							new byte[] {
								0
							}).concat(
									ByteString.copyFrom(
											filterBytes));

					requestBuilder.setNumericIndexStrategyFilter(
							filterByteString);
				}
			}
			requestBuilder.setModel(
					ByteString.copyFrom(
							PersistenceUtils.toBinary(
									readerParams.getIndex().getIndexModel())));

			final MultiRowRangeFilter multiFilter = getMultiRowRangeFilter(
					readerParams.getQueryRanges().getCompositeQueryRanges());
			if (multiFilter != null) {
				requestBuilder.setRangeFilter(
						ByteString.copyFrom(
								multiFilter.toByteArray()));
			}
			if (readerParams.getAggregation().getLeft() != null) {
				final ByteArrayId adapterId = readerParams.getAggregation().getLeft().getAdapterId();
				if (readerParams.getAggregation().getRight() instanceof CommonIndexAggregation) {
					requestBuilder.setAdapterId(
							ByteString.copyFrom(
									adapterId.getBytes()));
				}
				// else {
				// final DataAdapter dataAdapter =
				// adapterStore.getAdapter(adapterId);
				// requestBuilder.setAdapter(ByteString.copyFrom(PersistenceUtils.toBinary(dataAdapter)));
				// }
			}

			if (readerParams.getAdditionalAuthorizations() != null
					&& readerParams.getAdditionalAuthorizations().length > 0) {
				requestBuilder.setVisLabels(
						ByteString.copyFrom(
								StringUtils.stringsToBinary(
										readerParams.getAdditionalAuthorizations())));
			}

			// if (wholeRowIterator) {
			// requestBuilder.setWholeRowFilter(true);
			// }

			requestBuilder.setPartitionKeyLength(
					readerParams.getIndex().getIndexStrategy().getPartitionKeyLength());

			final AggregationProtos.AggregationRequest request = requestBuilder.build();

			final Table table = getTable(
					tableName);

			byte[] startRow = null;
			byte[] endRow = null;

			final List<ByteArrayRange> ranges = readerParams.getQueryRanges().getCompositeQueryRanges();
			if ((ranges != null) && !ranges.isEmpty()) {
				final ByteArrayRange aggRange = ranges.get(
						0);
				startRow = aggRange.getStart().getBytes();
				endRow = aggRange.getEnd().getBytes();
			}

			final Map<byte[], ByteString> results = table.coprocessorService(
					AggregationProtos.AggregationService.class,
					startRow,
					endRow,
					new Batch.Call<AggregationProtos.AggregationService, ByteString>() {
						@Override
						public ByteString call(
								final AggregationProtos.AggregationService counter )
								throws IOException {
							final BlockingRpcCallback<AggregationProtos.AggregationResponse> rpcCallback = new BlockingRpcCallback<AggregationProtos.AggregationResponse>();
							counter.aggregate(
									null,
									request,
									rpcCallback);
							final AggregationProtos.AggregationResponse response = rpcCallback.get();
							return response.hasValue() ? response.getValue() : null;
						}
					});

			Mergeable total = null;

			int regionCount = 0;
			for (final Map.Entry<byte[], ByteString> entry : results.entrySet()) {
				regionCount++;

				final ByteString value = entry.getValue();
				if ((value != null) && !value.isEmpty()) {
					final byte[] bvalue = value.toByteArray();
					final Mergeable mvalue = PersistenceUtils.fromBinary(
							bvalue,
							Mergeable.class);

					LOGGER.debug(
							"Value from region " + regionCount + " is " + mvalue);

					if (total == null) {
						total = mvalue;
					}
					else {
						total.merge(
								mvalue);
					}
				}
				else {
					LOGGER.debug(
							"Empty response for region " + regionCount);
				}
			}

			return total;
		}
		catch (final Exception e) {
			LOGGER.error(
					"Error during aggregation.",
					e);
		}
		catch (final Throwable e) {
			LOGGER.error(
					"Error during aggregation.",
					e);
		}

		return null;
	}

	public List<ByteArrayId> getTableRegions(
			String tableNameStr ) {
		ArrayList<ByteArrayId> regionIdList = new ArrayList();
		TableName tableName = getTableName(
				tableNameStr);

		try {
			RegionLocator locator = conn.getRegionLocator(
					tableName);
			for (HRegionLocation regionLocation : locator.getAllRegionLocations()) {
				regionIdList.add(
						new ByteArrayId(
								regionLocation.getRegionInfo().getRegionName()));
			}
		}
		catch (IOException e) {
			LOGGER.error(
					"Error accessing region locator for " + tableNameStr,
					e);
		}

		return regionIdList;
	}
}
