package mil.nga.giat.geowave.datastore.cassandra.operations;

import java.nio.ByteBuffer;

import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Row;
import com.datastax.driver.core.querybuilder.QueryBuilder;
import com.datastax.driver.core.querybuilder.Select;
import com.datastax.driver.core.querybuilder.Select.Where;
import com.google.common.collect.Iterators;

import mil.nga.giat.geowave.core.store.CloseableIterator;
import mil.nga.giat.geowave.core.store.DataStoreOptions;
import mil.nga.giat.geowave.core.store.entities.GeoWaveMetadata;
import mil.nga.giat.geowave.core.store.operations.MetadataQuery;
import mil.nga.giat.geowave.core.store.operations.MetadataReader;
import mil.nga.giat.geowave.core.store.operations.MetadataType;

public class CassandraMetadataReader implements
		MetadataReader
{
	private final CassandraOperations operations;
	private final DataStoreOptions options;
	private final MetadataType metadataType;

	public CassandraMetadataReader(
			final CassandraOperations operations,
			final DataStoreOptions options,
			final MetadataType metadataType ) {
		this.operations = operations;
		this.options = options;
		this.metadataType = metadataType;
	}

	@Override
	public CloseableIterator<GeoWaveMetadata> query(
			final MetadataQuery query ) {
		final String tableName = operations.getMetadataTableName(
				metadataType);
		//TODO need to merge stats
		if (query.hasPrimaryId()) {
			final Select select = operations.getSelect(
					tableName,
					CassandraMetadataWriter.VALUE_KEY);
			final Where where = select.where(
					QueryBuilder.eq(
							CassandraMetadataWriter.PRIMARY_ID_KEY,
							ByteBuffer.wrap(
									query.getPrimaryId())));
			if (query.hasSecondaryId()) {
				where.and(
						QueryBuilder.eq(
								CassandraMetadataWriter.SECONDARY_ID_KEY,
								ByteBuffer.wrap(
										query.getSecondaryId())));
			}
			final ResultSet rs = operations.getSession().execute(
					select);
			return new CloseableIterator.Wrapper<>(
					Iterators.transform(
							rs.iterator(),
							new com.google.common.base.Function<Row, GeoWaveMetadata>() {
								@Override
								public GeoWaveMetadata apply(
										final Row result ) {

									return new GeoWaveMetadata(
											query.getPrimaryId(),
											query.getSecondaryId(),
											null,
											result.get(
													CassandraMetadataWriter.VALUE_KEY,
													ByteBuffer.class).array());
								}
							}));
		}
		//should be a full scan
		return null;
	}
}
