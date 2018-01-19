package mil.nga.giat.geowave.datastore.hbase.operations;

import java.io.IOException;

import org.apache.hadoop.hbase.client.BufferedMutator;
import org.apache.hadoop.hbase.client.Delete;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import mil.nga.giat.geowave.core.index.ByteArrayId;
import mil.nga.giat.geowave.core.store.adapter.DataAdapter;
import mil.nga.giat.geowave.core.store.entities.GeoWaveKey;
import mil.nga.giat.geowave.core.store.entities.GeoWaveRow;
import mil.nga.giat.geowave.core.store.operations.Deleter;

public class HBaseDeleter implements
		Deleter
{
	private static Logger LOGGER = LoggerFactory.getLogger(HBaseDeleter.class);
	private final BufferedMutator deleter;
	private final boolean isAltIndex;

	public HBaseDeleter(
			final BufferedMutator deleter,
			final boolean isAltIndex ) {
		this.deleter = deleter;
		this.isAltIndex = isAltIndex;
	}

	@Override
	public void close() {
		try {
			if (deleter != null) {
				deleter.close();
			}
		}
		catch (final IOException e) {
			LOGGER.warn(
					"Unable to close BufferedMutator",
					e);
		}
	}

	@Override
	public void delete(
			final GeoWaveRow row,
			final DataAdapter<?> adapter ) {
		final Delete delete = new Delete(
				GeoWaveKey.getCompositeId(row),
				System.nanoTime());

		try {
			deleter.mutate(delete);
		}
		catch (IOException e) {
			LOGGER.warn(
					"Unable to delete row",
					e);
		}
	}
}
