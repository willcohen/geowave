package mil.nga.giat.geowave.core.store.adapter.statistics;

import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;

import org.apache.commons.lang3.ArrayUtils;

import mil.nga.giat.geowave.core.index.ByteArrayId;
import mil.nga.giat.geowave.core.index.Mergeable;
import mil.nga.giat.geowave.core.store.adapter.statistics.histogram.ByteUtils;
import mil.nga.giat.geowave.core.store.adapter.statistics.histogram.MinimalBinDistanceHistogram.MinimalBinDistanceHistogramFactory;
import mil.nga.giat.geowave.core.store.adapter.statistics.histogram.NumericHistogram;
import mil.nga.giat.geowave.core.store.adapter.statistics.histogram.NumericHistogramFactory;
import mil.nga.giat.geowave.core.store.entities.GeoWaveRow;

/**
 * Dynamic histogram provide very high accuracy for CDF and quantiles over the a
 * numeric attribute.
 *
 */
public class RowRangeHistogramStatistics<T> extends
		AbstractDataStatistics<T>
{
	public static final ByteArrayId STATS_ID = new ByteArrayId(
			"RANGE_HISTOGRAM_");
	private static final NumericHistogramFactory HistFactory = new MinimalBinDistanceHistogramFactory();
	private Map<ByteArrayId, NumericHistogram> histogramPerPartition = new HashMap<ByteArrayId, NumericHistogram>();

	protected RowRangeHistogramStatistics() {
		super();
	}

	public RowRangeHistogramStatistics(
			final ByteArrayId adapterId,
			final ByteArrayId indexId ) {
		super(
				adapterId,
				composeId(indexId));
	}

	private static NumericHistogram createHistogram() {
		return HistFactory.create(1024);
	}

	public static ByteArrayId composeId(
			final ByteArrayId indexId ) {
		return new ByteArrayId(
				ArrayUtils.addAll(
						STATS_ID.getBytes(),
						indexId.getBytes()));
	}

	@Override
	public DataStatistics<T> duplicate() {
		return new RowRangeHistogramStatistics<T>(
				dataAdapterId,
				decomposeFromId(statisticsId));// indexId
	}

	public static ByteArrayId decomposeFromId(
			final ByteArrayId id ) {
		final int idLength = id.getBytes().length - STATS_ID.getBytes().length;
		final byte[] idBytes = new byte[idLength];
		System.arraycopy(
				id.getBytes(),
				STATS_ID.getBytes().length,
				idBytes,
				0,
				idLength);
		return new ByteArrayId(
				idBytes);
	}

	public boolean isSet() {
		return false;
	}

	private synchronized NumericHistogram getHistogram(
			final ByteArrayId partition ) {
		NumericHistogram histogram = histogramPerPartition.get(partition);
		if (histogram == null) {
			histogram = createHistogram();
			histogramPerPartition.put(
					partition,
					histogram);
		}
		return histogram;
	}

	public double cardinality(
			final byte[] partition,
			final byte[] start,
			final byte[] end ) {
		final NumericHistogram histogram = getHistogram(getPartitionKey(partition));
		return ((end == null ? histogram.getTotalCount() : histogram.sum(
				ByteUtils.toDouble(end),
				true)) - (start == null ? 0 : histogram.sum(
				ByteUtils.toDouble(start),
				false)));
	}

	public double[] quantile(
			final byte[] partition,
			final int bins ) {
		final double[] result = new double[bins];
		final double binSize = 1.0 / bins;
		for (int bin = 0; bin < bins; bin++) {
			result[bin] = quantile(
					partition,
					binSize * (bin + 1));
		}
		return result;
	}

	public long[] count(
			final ByteArrayId partition,
			final int bins ) {
		return getHistogram(
				partition).count(
				bins);
	}

	public double cdf(
			final byte[] partition,
			final byte[] id ) {
		return getHistogram(
				getPartitionKey(partition)).cdf(
				ByteUtils.toDouble(id));
	}

	public double quantile(
			final byte[] partition,
			final double percentage ) {
		return getHistogram(
				getPartitionKey(partition)).quantile(
				(percentage));
	}

	public double percentPopulationOverRange(
			final byte[] partition,
			final byte[] start,
			final byte[] stop ) {
		return cdf(
				partition,
				stop) - cdf(
				partition,
				start);
	}

	public long getLeftMostCount(
			final ByteArrayId partition ) {
		final NumericHistogram histogram = getHistogram(partition);
		return (long) Math.ceil(histogram.sum(
				histogram.getMinValue(),
				true));
	}

	public long totalSampleSize() {
		long retVal = 0;
		for (final NumericHistogram histogram : histogramPerPartition.values()) {
			retVal += histogram.getTotalCount();
		}
		return retVal;
	}

	public long totalSampleSize(
			final ByteArrayId partition ) {
		return getHistogram(
				partition).getTotalCount();
	}

	@Override
	public void merge(
			final Mergeable mergeable ) {
		if (mergeable instanceof RowRangeHistogramStatistics) {
			for (final Entry<ByteArrayId, NumericHistogram> otherHistogram : ((RowRangeHistogramStatistics<?>) mergeable).histogramPerPartition
					.entrySet()) {
				final NumericHistogram histogram = histogramPerPartition.get(otherHistogram.getKey());
				if (histogram == null) {
					histogramPerPartition.put(
							otherHistogram.getKey(),
							otherHistogram.getValue());
				}
				else {
					histogram.merge(otherHistogram.getValue());
				}
			}
		}
	}

	@Override
	public byte[] toBinary() {
		int bufferSize = 4;
		for (final Entry<ByteArrayId, NumericHistogram> e : histogramPerPartition.entrySet()) {
			bufferSize += 8;
			if (e.getKey() != null) {
				bufferSize += e.getKey().getBytes().length;
			}
			bufferSize += e.getValue().bufferSize();
		}
		final ByteBuffer buffer = super.binaryBuffer(bufferSize);
		buffer.putInt(histogramPerPartition.size());
		for (final Entry<ByteArrayId, NumericHistogram> e : histogramPerPartition.entrySet()) {
			if (e.getKey() == null) {
				buffer.putInt(0);
			}
			else {
				buffer.putInt(e.getKey().getBytes().length);
				buffer.put(e.getKey().getBytes());
			}
			e.getValue().toBinary(
					buffer);
		}
		return buffer.array();
	}

	@Override
	public void fromBinary(
			final byte[] bytes ) {
		final ByteBuffer buffer = super.binaryBuffer(bytes);
		final int numPartitions = buffer.getInt();
		final Map<ByteArrayId, NumericHistogram> internalHistogramPerPartition = new HashMap<ByteArrayId, NumericHistogram>();
		for (int i = 0; i < numPartitions; i++) {
			final int partitionKeyLength = buffer.getInt();
			ByteArrayId partitionKey;
			if (partitionKeyLength <= 0) {
				partitionKey = null;
			}
			else {
				final byte[] partitionKeyBytes = new byte[partitionKeyLength];
				buffer.get(partitionKeyBytes);
				partitionKey = new ByteArrayId(
						partitionKeyBytes);
			}
			final NumericHistogram histogram = createHistogram();
			histogram.fromBinary(buffer);
			internalHistogramPerPartition.put(
					partitionKey,
					histogram);
		}
		this.histogramPerPartition = internalHistogramPerPartition;
	}

	@Override
	public void entryIngested(
			final T entry,
			final GeoWaveRow... kvs ) {
		for (final GeoWaveRow kv : kvs) {
			final byte[] idBytes = kv.getSortKey();
			add(
					getPartitionKey(kv.getPartitionKey()),
					ByteUtils.toDouble(idBytes));

		}
	}

	protected ByteArrayId getPartitionKey(
			final byte[] partitionBytes ) {
		return ((partitionBytes == null) || (partitionBytes.length == 0)) ? null : new ByteArrayId(
				partitionBytes);
	}

	protected void add(
			final ByteArrayId partition,
			final double num ) {
		getHistogram(
				partition).add(
				1,
				num);
	}

	@Override
	public String toString() {
		final StringBuffer buffer = new StringBuffer();
		buffer.append(
				"histogram[index=").append(
				super.statisticsId.getString());
		buffer.append(", partitions={");
		for (final Entry<ByteArrayId, NumericHistogram> entry : histogramPerPartition.entrySet()) {
			buffer.append(
					"partition[id=").append(
					entry.getKey() == null ? "null" : entry.getKey().getString());
			buffer.append(", bins={");
			for (final double v : this.quantile(
					entry.getKey() == null ? null : entry.getKey().getBytes(),
					10)) {
				buffer.append(v);
				buffer.append(' ');
			}
			buffer.deleteCharAt(buffer.length() - 1);
			buffer.append("}, counts={");
			for (final long v : this.count(
					entry.getKey(),
					10)) {
				buffer.append(
						v).append(
						' ');
			}

			buffer.deleteCharAt(buffer.length() - 1);
			buffer.append("}]");
		}
		buffer.append("}]");
		return buffer.toString();
	}

}
