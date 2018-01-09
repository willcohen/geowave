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
package mil.nga.giat.geowave.datastore.accumulo;

import java.io.IOException;
import java.util.List;

import org.apache.accumulo.core.client.AccumuloException;
import org.apache.accumulo.core.client.AccumuloSecurityException;
import org.apache.accumulo.core.client.TableNotFoundException;
import org.apache.hadoop.mapreduce.InputSplit;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import mil.nga.giat.geowave.core.store.adapter.AdapterIndexMappingStore;
import mil.nga.giat.geowave.core.store.adapter.AdapterStore;
import mil.nga.giat.geowave.core.store.adapter.DataAdapter;
import mil.nga.giat.geowave.core.store.adapter.RowMergingDataAdapter;
import mil.nga.giat.geowave.core.store.adapter.statistics.DataStatisticsStore;
import mil.nga.giat.geowave.core.store.index.IndexStore;
import mil.nga.giat.geowave.core.store.index.PrimaryIndex;
import mil.nga.giat.geowave.core.store.metadata.AdapterIndexMappingStoreImpl;
import mil.nga.giat.geowave.core.store.metadata.AdapterStoreImpl;
import mil.nga.giat.geowave.core.store.metadata.DataStatisticsStoreImpl;
import mil.nga.giat.geowave.core.store.metadata.IndexStoreImpl;
import mil.nga.giat.geowave.core.store.query.DistributableQuery;
import mil.nga.giat.geowave.core.store.query.QueryOptions;
import mil.nga.giat.geowave.core.store.server.RowMergingAdapterOptionProvider;
import mil.nga.giat.geowave.core.store.server.ServerOpHelper;
import mil.nga.giat.geowave.core.store.server.ServerSideOperations;
import mil.nga.giat.geowave.core.store.util.DataAdapterAndIndexCache;
import mil.nga.giat.geowave.datastore.accumulo.cli.config.AccumuloOptions;
import mil.nga.giat.geowave.datastore.accumulo.index.secondary.AccumuloSecondaryIndexDataStore;
import mil.nga.giat.geowave.datastore.accumulo.mapreduce.AccumuloSplitsProvider;
import mil.nga.giat.geowave.datastore.accumulo.operations.AccumuloOperations;
import mil.nga.giat.geowave.mapreduce.BaseMapReduceDataStore;

/**
 * This is the Accumulo implementation of the data store. It requires an
 * AccumuloOperations instance that describes how to connect (read/write data)
 * to Apache Accumulo. It can create default implementations of the IndexStore
 * and AdapterStore based on the operations which will persist configuration
 * information to Accumulo tables, or an implementation of each of these stores
 * can be passed in A DataStore can both ingest and query data based on
 * persisted indices and data adapters. When the data is ingested it is
 * explicitly given an index and a data adapter which is then persisted to be
 * used in subsequent queries.
 */
public class AccumuloDataStore extends
		BaseMapReduceDataStore
{
	private final static Logger LOGGER = LoggerFactory.getLogger(AccumuloDataStore.class);

	private final AccumuloSplitsProvider splitsProvider = new AccumuloSplitsProvider();

	public AccumuloDataStore(
			final AccumuloOperations accumuloOperations,
			final AccumuloOptions accumuloOptions ) {
		this(
				new IndexStoreImpl(
						accumuloOperations,
						accumuloOptions),
				new AdapterStoreImpl(
						accumuloOperations,
						accumuloOptions),
				new DataStatisticsStoreImpl(
						accumuloOperations,
						accumuloOptions),
				new AccumuloSecondaryIndexDataStore(
						accumuloOperations,
						accumuloOptions),
				new AdapterIndexMappingStoreImpl(
						accumuloOperations,
						accumuloOptions),
				accumuloOperations,
				accumuloOptions);
	}

	public AccumuloDataStore(
			final IndexStore indexStore,
			final AdapterStore adapterStore,
			final DataStatisticsStore statisticsStore,
			final AccumuloSecondaryIndexDataStore secondaryIndexDataStore,
			final AdapterIndexMappingStore indexMappingStore,
			final AccumuloOperations accumuloOperations ) {
		this(
				indexStore,
				adapterStore,
				statisticsStore,
				secondaryIndexDataStore,
				indexMappingStore,
				accumuloOperations,
				new AccumuloOptions());
	}

	public AccumuloDataStore(
			final IndexStore indexStore,
			final AdapterStore adapterStore,
			final DataStatisticsStore statisticsStore,
			final AccumuloSecondaryIndexDataStore secondaryIndexDataStore,
			final AdapterIndexMappingStore indexMappingStore,
			final AccumuloOperations accumuloOperations,
			final AccumuloOptions accumuloOptions ) {
		super(
				indexStore,
				adapterStore,
				statisticsStore,
				indexMappingStore,
				secondaryIndexDataStore,
				accumuloOperations,
				accumuloOptions);

		secondaryIndexDataStore.setDataStore(this);
	}

	@Override
	protected void initOnIndexWriterCreate(
			final DataAdapter adapter,
			final PrimaryIndex index ) {

		final String indexName = index.getId().getString();

		try {
			if (baseOptions.isServerSideLibraryEnabled() && adapter instanceof RowMergingDataAdapter) {
				if (!DataAdapterAndIndexCache.getInstance(
						RowMergingAdapterOptionProvider.ROW_MERGING_ADAPTER_CACHE_ID).add(
						adapter.getAdapterId(),
						indexName)) {
					if (baseOptions.isCreateTable()) {
						((AccumuloOperations) baseOperations).createTable(
								indexName,
								true,
								baseOptions.isEnableBlockCache());
					}
					ServerOpHelper.addServerSideRowMerging(
							((RowMergingDataAdapter<?, ?>) adapter),
							(ServerSideOperations) baseOperations,
							RowMergingCombiner.class.getName(),
							RowMergingVisibilityCombiner.class.getName(),
							indexName);
				}
			}

			final byte[] adapterId = adapter.getAdapterId().getBytes();
			if (((AccumuloOptions) baseOptions).isUseLocalityGroups()
					&& !((AccumuloOperations) baseOperations).localityGroupExists(
							indexName,
							adapterId)) {
				((AccumuloOperations) baseOperations).addLocalityGroup(
						indexName,
						adapterId);
			}
		}
		catch (AccumuloException | TableNotFoundException | AccumuloSecurityException e) {
			LOGGER.error(
					"Unable to determine existence of locality group [" + adapter.getAdapterId().getString() + "]",
					e);
		}

	}

	@Override
	public List<InputSplit> getSplits(
			final DistributableQuery query,
			final QueryOptions queryOptions,
			final AdapterStore adapterStore,
			final AdapterIndexMappingStore aimStore,
			final DataStatisticsStore statsStore,
			final IndexStore indexStore,
			final Integer minSplits,
			final Integer maxSplits )
			throws IOException,
			InterruptedException {
		return splitsProvider.getSplits(
				baseOperations,
				query,
				queryOptions,
				adapterStore,
				statsStore,
				indexStore,
				aimStore,
				minSplits,
				maxSplits);
	}
}
