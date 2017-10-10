package mil.nga.giat.geowave.datastore.cassandra.operations;

import java.nio.ByteBuffer;

import com.datastax.driver.core.DataType;
import com.datastax.driver.core.querybuilder.Insert;
import com.datastax.driver.core.querybuilder.QueryBuilder;
import com.datastax.driver.core.schemabuilder.Create;

import mil.nga.giat.geowave.core.index.PersistenceUtils;
import mil.nga.giat.geowave.core.store.entities.GeoWaveMetadata;
import mil.nga.giat.geowave.core.store.operations.MetadataWriter;

public class CassandraMetadataWriter implements
		MetadataWriter
{
	protected static final String PRIMARY_ID_KEY = "I";
	protected static final String SECONDARY_ID_KEY = "S";
	// serves as unique ID for instances where primary+secondary are repeated
	protected static final String TIMESTAMP_ID_KEY = "T";
	protected static final String VALUE_KEY = "V";

	private static final Object CREATE_TABLE_MUTEX = new Object();
	private CassandraOperations operations;

	public CassandraMetadataWriter(
			CassandraOperations operations ) {
		this.operations = operations;
	}

	@Override
	public void close()
			throws Exception {

	}

	@Override
	public void write(
			final GeoWaveMetadata metadata ) {
		
		final Insert insert = operations.getInsert(
				getTablename());
		insert.value(
				PRIMARY_ID_KEY,
				ByteBuffer.wrap(
						id.getBytes()));
		if (secondaryId != null) {
			insert.value(
					SECONDARY_ID_KEY,
					ByteBuffer.wrap(
							secondaryId.getBytes()));
			insert.value(
					TIMESTAMP_ID_KEY,
					QueryBuilder.now());
		}
		insert.value(
				VALUE_KEY,
				ByteBuffer.wrap(
						PersistenceUtils.toBinary(
								object)));
		operations.getSession().execute(
				insert);
	}

	@Override
	public void flush() {

	}

}
