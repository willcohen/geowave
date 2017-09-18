package mil.nga.giat.geowave.datastore.hbase.operations;

import java.io.IOException;

import org.apache.hadoop.hbase.client.BufferedMutator;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.security.visibility.CellVisibility;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import mil.nga.giat.geowave.core.index.Mergeable;
import mil.nga.giat.geowave.core.index.PersistenceUtils;
import mil.nga.giat.geowave.core.index.StringUtils;
import mil.nga.giat.geowave.core.store.entities.GeoWaveMetadata;
import mil.nga.giat.geowave.core.store.operations.MetadataType;
import mil.nga.giat.geowave.core.store.operations.MetadataWriter;

public class HBaseMetadataWriter implements
		MetadataWriter
{
	private static final Logger LOGGER = LoggerFactory.getLogger(
			HBaseMetadataWriter.class);

	private final HBaseOperations operations;
	private final BufferedMutator writer;
	private final MetadataType metadataType;
	private final byte[] metadataTypeBytes;

	public HBaseMetadataWriter(
			final HBaseOperations operations,
			final BufferedMutator writer,
			final MetadataType metadataType ) {
		this.operations = operations;
		this.writer = writer;
		this.metadataType = metadataType;
		this.metadataTypeBytes = StringUtils.stringToBinary(
				metadataType.name());
	}

	@Override
	public void close()
			throws Exception {
		try {
			writer.close();
		}
		catch (final IOException e) {
			LOGGER.warn(
					"Unable to close metadata writer",
					e);
		}
	}

	@Override
	public void write(
			final GeoWaveMetadata metadata ) {
		byte[] byteValue;

		// Check for stats
		if (metadataType == MetadataType.STATS) {
			// Combine stats clients-side
			Mergeable mergedValue = operations.mergeStatValue(
					metadata);
			byteValue = PersistenceUtils.toBinary(
					mergedValue);
		}
		else {
			byteValue = metadata.getValue();
		}

		final Put put = new Put(
				metadata.getPrimaryId());

		byte[] secondaryBytes = metadata.getSecondaryId() != null ? metadata.getSecondaryId() : new byte[0];

		put.addColumn(
				metadataTypeBytes,
				secondaryBytes,
				byteValue);

		if (metadata.getVisibility() != null) {
			put.setCellVisibility(
					new CellVisibility(
							StringUtils.stringFromBinary(
									metadata.getVisibility())));
		}

		try {
			writer.mutate(
					put);
		}
		catch (final IOException e) {
			LOGGER.error(
					"Unable to write metadata",
					e);
		}

	}

	@Override
	public void flush() {
		try {
			writer.flush();
		}
		catch (final IOException e) {
			LOGGER.warn(
					"Unable to flush metadata writer",
					e);
		}
	}
}
