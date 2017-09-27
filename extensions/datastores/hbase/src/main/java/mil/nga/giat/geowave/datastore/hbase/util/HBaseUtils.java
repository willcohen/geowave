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

import java.io.Closeable;
import java.io.IOException;
import java.util.List;

import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.RowMutations;
import org.apache.hadoop.hbase.filter.MultiRowRangeFilter.RowRange;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import mil.nga.giat.geowave.core.index.ByteArrayId;
import mil.nga.giat.geowave.core.index.ByteArrayRange;
import mil.nga.giat.geowave.core.index.NumericIndexStrategy;
import mil.nga.giat.geowave.core.index.PersistenceUtils;
import mil.nga.giat.geowave.core.index.QueryRanges;
import mil.nga.giat.geowave.core.index.sfc.data.MultiDimensionalNumericData;
import mil.nga.giat.geowave.core.store.adapter.statistics.DataStatistics;

@SuppressWarnings("rawtypes")
public class HBaseUtils
{
	private final static Logger LOGGER = LoggerFactory.getLogger(HBaseUtils.class);

	public static String getQualifiedTableName(
			final String tableNamespace,
			final String unqualifiedTableName ) {
		if ((tableNamespace == null) || tableNamespace.isEmpty()) {
			return unqualifiedTableName;
		}

		if (unqualifiedTableName.contains(tableNamespace)) {
			return unqualifiedTableName;
		}

		return tableNamespace + "_" + unqualifiedTableName;
	}

	public static QueryRanges constraintsToByteArrayRanges(
			final MultiDimensionalNumericData constraints,
			final NumericIndexStrategy indexStrategy,
			final int maxRanges ) {
		if ((constraints == null) || constraints.isEmpty()) {
			return null; // implies in negative and
			// positive infinity
		}
		else {
			return indexStrategy.getQueryRanges(
					constraints,
					maxRanges);
		}
	}

	public static RowMutations getDeleteMutations(
			final byte[] rowId,
			final byte[] columnFamily,
			final byte[] columnQualifier,
			final String[] authorizations )
			throws IOException {
		final RowMutations m = new RowMutations(
				rowId);
		final Delete d = new Delete(
				rowId);
		d.addColumns(
				columnFamily,
				columnQualifier);
		m.add(d);
		return m;
	}

	public static class ScannerClosableWrapper implements
			Closeable
	{
		private final ResultScanner results;

		public ScannerClosableWrapper(
				final ResultScanner results ) {
			this.results = results;
		}

		@Override
		public void close() {
			results.close();
		}

	}

	public static class MultiScannerClosableWrapper implements
			Closeable
	{
		private final List<ResultScanner> results;

		public MultiScannerClosableWrapper(
				final List<ResultScanner> results ) {
			this.results = results;
		}

		@Override
		public void close() {
			for (final ResultScanner scanner : results) {
				scanner.close();
			}
		}
	}

	public static boolean rangesIntersect(
			RowRange range1,
			RowRange range2 ) {
		ByteArrayRange thisRange = new ByteArrayRange(
				new ByteArrayId(
						range1.getStartRow()),
				new ByteArrayId(
						range1.getStopRow()));

		ByteArrayRange otherRange = new ByteArrayRange(
				new ByteArrayId(
						range2.getStartRow()),
				new ByteArrayId(
						range2.getStopRow()));

		return thisRange.intersects(otherRange);
	}

	public static DataStatistics getMergedStats(
			List<Cell> rowCells ) {
		DataStatistics mergedStats = null;
		for (Cell cell : rowCells) {
			byte[] byteValue = CellUtil.cloneValue(cell);
			DataStatistics stats = (DataStatistics) PersistenceUtils.fromBinary(
					byteValue,
					DataStatistics.class);

			if (mergedStats != null) {
				mergedStats.merge(stats);
			}
			else {
				mergedStats = stats;
			}
		}

		return mergedStats;
	}

}
