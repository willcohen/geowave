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
package mil.nga.giat.geowave.datastore.hbase.mapreduce;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.ListIterator;
import java.util.Map;
import java.util.Map.Entry;
import java.util.TreeSet;

import org.apache.commons.lang3.ArrayUtils;
import org.apache.hadoop.hbase.HRegionInfo;
import org.apache.hadoop.hbase.HRegionLocation;
import org.apache.hadoop.hbase.client.RegionLocator;
import org.apache.hadoop.hbase.filter.MultiRowRangeFilter.RowRange;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import mil.nga.giat.geowave.core.index.ByteArrayId;
import mil.nga.giat.geowave.core.index.ByteArrayRange;
import mil.nga.giat.geowave.core.index.NumericIndexStrategy;
import mil.nga.giat.geowave.core.index.sfc.data.MultiDimensionalNumericData;
import mil.nga.giat.geowave.core.store.adapter.AdapterStore;
import mil.nga.giat.geowave.core.store.adapter.DataAdapter;
import mil.nga.giat.geowave.core.store.adapter.statistics.DataStatisticsStore;
import mil.nga.giat.geowave.core.store.adapter.statistics.RowRangeHistogramStatistics;
import mil.nga.giat.geowave.core.store.index.PrimaryIndex;
import mil.nga.giat.geowave.core.store.operations.DataStoreOperations;
import mil.nga.giat.geowave.core.store.query.DistributableQuery;
import mil.nga.giat.geowave.core.store.util.DataStoreUtils;
import mil.nga.giat.geowave.datastore.hbase.operations.HBaseOperations;
import mil.nga.giat.geowave.datastore.hbase.util.HBaseUtils;
import mil.nga.giat.geowave.mapreduce.splits.GeoWaveRowRange;
import mil.nga.giat.geowave.mapreduce.splits.IntermediateSplitInfo;
import mil.nga.giat.geowave.mapreduce.splits.RangeLocationPair;
import mil.nga.giat.geowave.mapreduce.splits.SplitInfo;
import mil.nga.giat.geowave.mapreduce.splits.SplitsProvider;

public class HBaseSplitsProvider extends
		SplitsProvider
{
	private final static Logger LOGGER = LoggerFactory.getLogger(HBaseSplitsProvider.class);

	@Override
	protected TreeSet<IntermediateSplitInfo> populateIntermediateSplits(
			final TreeSet<IntermediateSplitInfo> splits,
			final DataStoreOperations operations,
			final PrimaryIndex index,
			final List<DataAdapter<Object>> adapters,
			final Map<PrimaryIndex, RowRangeHistogramStatistics<?>> statsCache,
			final AdapterStore adapterStore,
			final DataStatisticsStore statsStore,
			final Integer maxSplits,
			final DistributableQuery query,
			final String[] authorizations )
			throws IOException {

		HBaseOperations hbaseOperations = null;
		if (operations instanceof HBaseOperations) {
			hbaseOperations = (HBaseOperations) operations;
		}
		else {
			LOGGER.error("HBaseSplitsProvider requires BasicHBaseOperations object.");
			return splits;
		}

		if ((query != null) && !query.isSupported(index)) {
			return splits;
		}

		final NumericIndexStrategy indexStrategy = index.getIndexStrategy();
		final int partitionKeyLength = indexStrategy.getPartitionKeyLength();

		final RowRange fullrange = toHBaseRange(
				new GeoWaveRowRange(
						null,
						null,
						null,
						true,
						true),
				partitionKeyLength);

		final String tableName = index.getId().getString();

		// Build list of row ranges from query
		List<RowRange> ranges = new ArrayList<RowRange>();
		final List<ByteArrayRange> constraintRanges;
		if (query != null) {
			final List<MultiDimensionalNumericData> indexConstraints = query.getIndexConstraints(indexStrategy);
			if ((maxSplits != null) && (maxSplits > 0)) {
				constraintRanges = DataStoreUtils.constraintsToQueryRanges(
						indexConstraints,
						indexStrategy,
						maxSplits).getCompositeQueryRanges();
			}
			else {
				constraintRanges = DataStoreUtils.constraintsToQueryRanges(
						indexConstraints,
						indexStrategy,
						-1).getCompositeQueryRanges();
			}
			for (final ByteArrayRange constraintRange : constraintRanges) {
				ranges.add(new RowRange(
						constraintRange.getStart().getBytes(),
						true,
						constraintRange.getEnd().getBytes(),
						false));
			}
		}
		else {
			ranges.add(fullrange);
			if (LOGGER.isTraceEnabled()) {
				LOGGER.trace("Protected range: " + fullrange);
			}
		}

		final Map<HRegionLocation, Map<HRegionInfo, List<RowRange>>> binnedRanges = new HashMap<HRegionLocation, Map<HRegionInfo, List<RowRange>>>();
		final RegionLocator regionLocator = hbaseOperations.getRegionLocator(tableName);

		if (regionLocator == null) {
			LOGGER.error("Unable to retrieve RegionLocator for " + tableName);
			return splits;
		}

		while (!ranges.isEmpty()) {
			ranges = binRanges(
					ranges,
					binnedRanges,
					regionLocator);
		}

		for (final Entry<HRegionLocation, Map<HRegionInfo, List<RowRange>>> locationEntry : binnedRanges.entrySet()) {
			final String hostname = locationEntry.getKey().getHostname();

			for (final Entry<HRegionInfo, List<RowRange>> regionEntry : locationEntry.getValue().entrySet()) {
				final Map<ByteArrayId, SplitInfo> splitInfo = new HashMap<ByteArrayId, SplitInfo>();
				final List<RangeLocationPair> rangeList = new ArrayList<RangeLocationPair>();

				for (final RowRange range : regionEntry.getValue()) {
					GeoWaveRowRange rowRange = fromHBaseRange(
							range,
							partitionKeyLength);

					final double cardinality = getCardinality(
							getHistStats(
									index,
									adapters,
									adapterStore,
									statsStore,
									statsCache,
									authorizations),
							rowRange,
							index.getIndexStrategy().getPartitionKeyLength());

					if (HBaseUtils.rangesIntersect(
							range,
							fullrange)) {
						rangeList.add(new RangeLocationPair(
								rowRange,
								hostname,
								cardinality < 1 ? 1.0 : cardinality));
					}
					else {
						LOGGER.info("Query split outside of range");
					}
					if (LOGGER.isTraceEnabled()) {
						LOGGER.warn("Clipped range: " + rangeList.get(
								rangeList.size() - 1).getRange());
					}
				}

				if (!rangeList.isEmpty()) {
					splitInfo.put(
							index.getId(),
							new SplitInfo(
									index,
									rangeList));
					splits.add(new IntermediateSplitInfo(
							splitInfo,
							this));
				}
			}
		}

		return splits;
	}

	private static List<RowRange> binRanges(
			final List<RowRange> inputRanges,
			final Map<HRegionLocation, Map<HRegionInfo, List<RowRange>>> binnedRanges,
			final RegionLocator regionLocator )
			throws IOException {

		// Loop through ranges, getting RegionLocation and RegionInfo for
		// startKey, clipping range by that regionInfo's extent, and leaving
		// remainder in the List to be region'd
		final ListIterator<RowRange> i = inputRanges.listIterator();
		while (i.hasNext()) {
			final RowRange range = i.next();

			final HRegionLocation location = regionLocator.getRegionLocation(range.getStartRow());

			Map<HRegionInfo, List<RowRange>> regionInfoMap = binnedRanges.get(location);
			if (regionInfoMap == null) {
				regionInfoMap = new HashMap<HRegionInfo, List<RowRange>>();
				binnedRanges.put(
						location,
						regionInfoMap);
			}

			final HRegionInfo regionInfo = location.getRegionInfo();
			List<RowRange> rangeList = regionInfoMap.get(regionInfo);
			if (rangeList == null) {
				rangeList = new ArrayList<RowRange>();
				regionInfoMap.put(
						regionInfo,
						rangeList);
			}

			if (regionInfo.containsRange(
					range.getStartRow(),
					range.getStopRow())) {
				rangeList.add(range);
				i.remove();
			}
			else {
				ByteArrayRange thisRange = new ByteArrayRange(
						new ByteArrayId(
								range.getStartRow()),
						new ByteArrayId(
								range.getStopRow()));
				ByteArrayRange regionRange = new ByteArrayRange(
						new ByteArrayId(
								regionInfo.getStartKey()),
						new ByteArrayId(
								regionInfo.getEndKey()));

				final ByteArrayRange overlappingRange = thisRange.intersection(regionRange);

				rangeList.add(new RowRange(
						overlappingRange.getStart().getBytes(),
						true,
						overlappingRange.getEnd().getBytes(),
						false));
				i.remove();

				i.add(new RowRange(
						regionInfo.getEndKey(),
						true,
						range.getStopRow(),
						false));
			}
		}

		return inputRanges;
	}

	protected static RowRange rangeIntersection(
			final RowRange thisRange,
			final RowRange otherRange ) {
		ByteArrayRange thisByteArrayRange = new ByteArrayRange(
				new ByteArrayId(
						thisRange.getStartRow()),
				new ByteArrayId(
						thisRange.getStopRow()));
		ByteArrayRange otherByteArrayRange = new ByteArrayRange(
				new ByteArrayId(
						otherRange.getStartRow()),
				new ByteArrayId(
						otherRange.getStopRow()));

		final ByteArrayRange overlappingRange = thisByteArrayRange.intersection(otherByteArrayRange);

		return new RowRange(
				overlappingRange.getStart().getBytes(),
				true,
				overlappingRange.getEnd().getBytes(),
				false);
	}

	public static RowRange toHBaseRange(
			final GeoWaveRowRange range,
			final int partitionKeyLength ) {
		if ((range.getPartitionKey() == null) || (range.getPartitionKey().length == 0)) {
			return new RowRange(
					(range.getStartSortKey() == null) ? null : range.getStartSortKey(),
					range.isStartSortKeyInclusive(),
					(range.getEndSortKey() == null) ? null : range.getEndSortKey(),
					range.isEndSortKeyInclusive());
		}
		else {
			return new RowRange(
					(range.getStartSortKey() == null) ? null : ArrayUtils.addAll(
							range.getPartitionKey(),
							range.getStartSortKey()),
					range.isStartSortKeyInclusive(),
					(range.getEndSortKey() == null) ? new ByteArrayId(
							range.getPartitionKey()).getNextPrefix() : ArrayUtils.addAll(
							range.getPartitionKey(),
							range.getEndSortKey()),
					(range.getEndSortKey() != null) && range.isEndSortKeyInclusive());
		}
	}

	public static GeoWaveRowRange fromHBaseRange(
			final RowRange range,
			final int partitionKeyLength ) {
		if (partitionKeyLength <= 0) {
			return new GeoWaveRowRange(
					null,
					range.getStartRow() == null ? null : range.getStartRow(),
					range.getStopRow() == null ? null : range.getStopRow(),
					range.isStartRowInclusive(),
					range.isStopRowInclusive());
		}
		else {
			byte[] partitionKey;
			boolean partitionKeyDiffers = false;
			if ((range.getStartRow() == null) && (range.getStopRow() == null)) {
				return null;
			}
			else if (range.getStartRow() != null) {
				partitionKey = ArrayUtils.subarray(
						range.getStartRow(),
						0,
						partitionKeyLength);
				if (range.getStopRow() != null) {
					partitionKeyDiffers = !Arrays.equals(
							partitionKey,
							ArrayUtils.subarray(
									range.getStopRow(),
									0,
									partitionKeyLength));
				}
			}
			else {
				partitionKey = ArrayUtils.subarray(
						range.getStopRow(),
						0,
						partitionKeyLength);
			}
			return new GeoWaveRowRange(
					partitionKey,
					range.getStartRow() == null ? null : ArrayUtils.subarray(
							range.getStartRow(),
							partitionKeyLength,
							range.getStartRow().length),
					partitionKeyDiffers ? null : range.getStopRow() == null ? null : ArrayUtils.subarray(
							range.getStopRow(),
							partitionKeyLength,
							range.getStopRow().length),
					range.isStartRowInclusive(),
					partitionKeyDiffers ? true : range.isStopRowInclusive());

		}
	}

}
