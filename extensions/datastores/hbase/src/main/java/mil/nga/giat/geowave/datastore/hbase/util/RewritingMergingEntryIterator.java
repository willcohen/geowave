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
package mil.nga.giat.geowave.datastore.hbase.util;

import java.util.Iterator;
import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import mil.nga.giat.geowave.core.index.ByteArrayId;
import mil.nga.giat.geowave.core.store.adapter.AdapterStore;
import mil.nga.giat.geowave.core.store.adapter.RowMergingDataAdapter;
import mil.nga.giat.geowave.core.store.entities.GeoWaveRow;
import mil.nga.giat.geowave.core.store.index.PrimaryIndex;
import mil.nga.giat.geowave.core.store.util.MergingEntryIterator;
import mil.nga.giat.geowave.datastore.hbase.operations.HBaseWriter;

public class RewritingMergingEntryIterator<T> extends
		MergingEntryIterator<T>
{
	private final static Logger LOGGER = LoggerFactory.getLogger(RewritingMergingEntryIterator.class);

	private final HBaseWriter writer;

	public RewritingMergingEntryIterator(
			final AdapterStore adapterStore,
			final PrimaryIndex index,
			final Iterator<GeoWaveRow> scannerIt,
			final Map<ByteArrayId, RowMergingDataAdapter> mergingAdapters,
			final HBaseWriter writer ) {
		super(
				adapterStore,
				index,
				scannerIt,
				null,
				null,
				mergingAdapters);
		this.writer = writer;
	}
}
