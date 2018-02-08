package mil.nga.giat.geowave.datastore.cassandra;

import java.io.Closeable;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.log4j.Logger;

import com.google.common.base.Function;
import com.google.common.collect.Lists;

import mil.nga.giat.geowave.core.index.ByteArrayId;
import mil.nga.giat.geowave.core.store.CloseableIterator;
import mil.nga.giat.geowave.core.store.CloseableIteratorWrapper;
import mil.nga.giat.geowave.core.store.DataStoreOptions;
import mil.nga.giat.geowave.core.store.IndexWriter;
import mil.nga.giat.geowave.core.store.adapter.AdapterStore;
import mil.nga.giat.geowave.core.store.adapter.DataAdapter;
import mil.nga.giat.geowave.core.store.adapter.statistics.DataStatisticsStore;
import mil.nga.giat.geowave.core.store.adapter.statistics.DuplicateEntryCount;
import mil.nga.giat.geowave.core.store.base.BaseDataStore;
import mil.nga.giat.geowave.core.store.callback.IngestCallback;
import mil.nga.giat.geowave.core.store.callback.ScanCallback;
import mil.nga.giat.geowave.core.store.data.visibility.DifferingFieldVisibilityEntryCount;
import mil.nga.giat.geowave.core.store.entities.GeoWaveRow;
import mil.nga.giat.geowave.core.store.entities.GeoWaveKeyImpl;
import mil.nga.giat.geowave.core.store.filter.DedupeFilter;
import mil.nga.giat.geowave.core.store.index.IndexMetaDataSet;
import mil.nga.giat.geowave.core.store.index.IndexStore;
import mil.nga.giat.geowave.core.store.index.PrimaryIndex;
import mil.nga.giat.geowave.core.store.operations.DataStoreOperations;
import mil.nga.giat.geowave.core.store.operations.Deleter;
import mil.nga.giat.geowave.core.store.operations.Writer;
import mil.nga.giat.geowave.core.store.query.DistributableQuery;
import mil.nga.giat.geowave.core.store.query.Query;
import mil.nga.giat.geowave.core.store.query.QueryOptions;
import mil.nga.giat.geowave.core.store.util.DataStoreUtils;
import mil.nga.giat.geowave.core.store.util.NativeEntryIteratorWrapper;
import mil.nga.giat.geowave.datastore.cassandra.index.secondary.CassandraSecondaryIndexDataStore;
import mil.nga.giat.geowave.datastore.cassandra.mapreduce.GeoWaveCassandraRecordReader;
import mil.nga.giat.geowave.datastore.cassandra.metadata.CassandraAdapterIndexMappingStore;
import mil.nga.giat.geowave.datastore.cassandra.metadata.CassandraAdapterStore;
import mil.nga.giat.geowave.datastore.cassandra.metadata.CassandraDataStatisticsStore;
import mil.nga.giat.geowave.datastore.cassandra.metadata.CassandraIndexStore;
import mil.nga.giat.geowave.datastore.cassandra.operations.CassandraOperations;
import mil.nga.giat.geowave.datastore.cassandra.query.CassandraConstraintsQuery;
import mil.nga.giat.geowave.datastore.cassandra.query.CassandraRowIdsQuery;
import mil.nga.giat.geowave.datastore.cassandra.query.CassandraRowPrefixQuery;
import mil.nga.giat.geowave.datastore.cassandra.split.CassandraSplitsProvider;
import mil.nga.giat.geowave.mapreduce.MapReduceDataStore;
import mil.nga.giat.geowave.mapreduce.input.GeoWaveInputKey;

public class CassandraDataStore extends
		BaseDataStore implements
		MapReduceDataStore
{
	private final static Logger LOGGER = Logger.getLogger(CassandraDataStore.class);
	public static final Integer PARTITIONS = 4;

	private final CassandraOperations operations;
	private final CassandraSplitsProvider splitsProvider = new CassandraSplitsProvider();
	private static int counter = 0;

	public CassandraDataStore(
			final CassandraOperations operations ) {
		super(
				new CassandraIndexStore(
						operations),
				new CassandraAdapterStore(
						operations),
				new CassandraDataStatisticsStore(
						operations),
				new CassandraAdapterIndexMappingStore(
						operations),
				new CassandraSecondaryIndexDataStore(
						operations),
				operations,
				operations.getOptions());
		this.operations = operations;
	}

	@Override
	protected <T> void addAltIndexCallback(
			final List<IngestCallback<T>> callbacks,
			final String indexName,
			final DataAdapter<T> adapter,
			final ByteArrayId primaryIndexId ) {
		// TODO Auto-generated method stub

	}


	@Override
	protected void initOnIndexWriterCreate(
			final DataAdapter adapter,
			final PrimaryIndex index ) {
		// TODO Auto-generated method stub

	}

	@Override
	protected Iterable<GeoWaveRow> getRowsFromIngest(
			byte[] adapterId,
			DataStoreEntryInfo ingestInfo,
			List<FieldInfo<?>> fieldInfoList,
			boolean ensureUniqueId ) {
		final List<GeoWaveRow> rows = new ArrayList<GeoWaveRow>();

		// The single FieldInfo contains the fieldMask in the ID, and the
		// flattened fields in the written value
		byte[] fieldMask = fieldInfoList.get(
				0).getDataValue().getId().getBytes();
		byte[] value = fieldInfoList.get(
				0).getWrittenValue();

		Iterator<ByteArrayId> rowIdIterator = ingestInfo.getRowIds().iterator();

		for (final ByteArrayId insertionId : ingestInfo.getInsertionIds()) {
			byte[] uniqueDataId;
			if (ensureUniqueId) {
				uniqueDataId = DataStoreUtils.ensureUniqueId(
						ingestInfo.getDataId(),
						false).getBytes();
			}
			else {
				uniqueDataId = ingestInfo.getDataId();
			}

			// for each insertion(index) id, there's a matching rowId
			// that contains the duplicate count
			GeoWaveRow tempRow = new GeoWaveKeyImpl(
					rowIdIterator.next().getBytes());
			int numDuplicates = tempRow.getNumberOfDuplicates();

			rows.add(new CassandraRow(
					nextPartitionId(),
					uniqueDataId,
					adapterId,
					insertionId.getBytes(),
					fieldMask,
					value,
					numDuplicates));
		}

		return rows;
	}

	private byte[] nextPartitionId() {
		counter = (counter + 1) % PARTITIONS;

		return new byte[] {
			(byte) counter
		};
	}

	@Override
	public void write(
			Writer writer,
			Iterable<GeoWaveRow> rows,
			final String columnFamily ) {
		for (GeoWaveRow geowaveRow : rows) {
			CassandraRow cassRow = (CassandraRow) geowaveRow;
			((CassandraWriter) writer).write(cassRow);
		}
	}
}
