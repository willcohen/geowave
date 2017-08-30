package mil.nga.giat.geowave.datastore.hbase.operations;

import java.util.Iterator;

import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.Iterators;

import mil.nga.giat.geowave.core.index.StringUtils;
import mil.nga.giat.geowave.core.store.CloseableIterator;
import mil.nga.giat.geowave.core.store.CloseableIteratorWrapper;
import mil.nga.giat.geowave.core.store.DataStoreOptions;
import mil.nga.giat.geowave.core.store.entities.GeoWaveMetadata;
import mil.nga.giat.geowave.core.store.metadata.AbstractGeoWavePersistence;
import mil.nga.giat.geowave.core.store.operations.MetadataQuery;
import mil.nga.giat.geowave.core.store.operations.MetadataReader;
import mil.nga.giat.geowave.core.store.operations.MetadataType;
import mil.nga.giat.geowave.datastore.hbase.util.HBaseUtils.ScannerClosableWrapper;

public class HBaseMetadataReader implements
		MetadataReader
{
	private final static Logger LOGGER = LoggerFactory.getLogger(
			HBaseMetadataReader.class);
	private final HBaseOperations operations;
	private final DataStoreOptions options;
	private final MetadataType metadataType;

	public HBaseMetadataReader(
			HBaseOperations operations,
			DataStoreOptions options,
			MetadataType metadataType ) {
		this.operations = operations;
		this.options = options;
		this.metadataType = metadataType;
	}

	@Override
	public CloseableIterator<GeoWaveMetadata> query(
			final MetadataQuery query ) {
		final Scan scanner = new Scan();

		try {
			final byte[] columnFamily = StringUtils.stringToBinary(
					metadataType.name());
			final byte[] columnQualifier = query.getSecondaryId();

			if (columnFamily != null) {
				if (columnQualifier != null) {
					scanner.addColumn(
							columnFamily,
							columnQualifier);
				}
				else {
					scanner.addFamily(
							columnFamily);
				}
			}

			if (query.getPrimaryId() != null) {
				scanner.setStartRow(
						query.getPrimaryId());
				scanner.setStopRow(
						query.getPrimaryId());
			}

			final ResultScanner rS = operations.getScannedResults(
					scanner,
					AbstractGeoWavePersistence.METADATA_TABLE,
					query.getAuthorizations());
			final Iterator<Result> it = rS.iterator();

			return new CloseableIteratorWrapper<>(
					new ScannerClosableWrapper(
							rS),
					Iterators.transform(
							it,
							new com.google.common.base.Function<Result, GeoWaveMetadata>() {
								@Override
								public GeoWaveMetadata apply(
										Result result ) {
									return new GeoWaveMetadata(
											result.getRow(),
											columnQualifier,
											null,
											result.value());
								}

							}));

		}
		catch (final Exception e) {
			LOGGER.warn(
					"GeoWave metadata table not found",
					e);
		}
		return new CloseableIterator.Wrapper<>(
				Iterators.emptyIterator());
	}
}
