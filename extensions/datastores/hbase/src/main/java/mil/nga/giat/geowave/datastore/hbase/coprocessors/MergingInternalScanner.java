package mil.nga.giat.geowave.datastore.hbase.coprocessors;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.regionserver.InternalScanner;
import org.apache.hadoop.hbase.regionserver.ScannerContext;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;

import mil.nga.giat.geowave.core.index.ByteArrayId;
import mil.nga.giat.geowave.core.index.Mergeable;
import mil.nga.giat.geowave.core.index.PersistenceUtils;
import mil.nga.giat.geowave.core.store.adapter.RowMergingDataAdapter.RowTransform;

public class MergingInternalScanner implements
		InternalScanner
{
	private final static Logger LOGGER = Logger.getLogger(
			MergingInternalScanner.class);

	private final InternalScanner delegate;
	private HashMap<ByteArrayId, RowTransform> mergingTransformMap;
	private Cell peekedCell = null;

	// TEST ONLY!
	static {
		LOGGER.setLevel(
				Level.DEBUG);
	}

	public MergingInternalScanner(
			final InternalScanner delegate ) {
		this.delegate = delegate;
	}

	@Override
	public boolean next(
			List<Cell> results )
			throws IOException {
		LOGGER.debug(
				"MERGING SCANNER > next(1)");

		boolean done = delegate.next(
				results);

		return done;
	}

	@Override
	public boolean next(
			List<Cell> result,
			ScannerContext scannerContext )
			throws IOException {
		Mergeable mergedValue = null;
		Cell mergedCell = null;
		boolean hasMore = true;

		if (peekedCell != null) {
			mergedCell = peekedCell;
		}
		else {
			hasMore = getNextCell(
					scannerContext,
					mergedCell);
		}

		// Peek ahead to see if it needs to be merged with the next result
		while (hasMore) {
			hasMore = getNextCell(
					scannerContext,
					peekedCell);

			if (CellUtil.matchingRow(
					mergedCell,
					peekedCell)) {
				mergeCells(
						mergedCell,
						peekedCell);
			}
			else {
				break;
			}
		}

		return hasMore;
	}

	private boolean mergeCells(
			Cell mergedCell,
			Cell peekedCell ) {
		ByteArrayId family = new ByteArrayId(
				CellUtil.cloneFamily(
						mergedCell));
		boolean merged = false;

		if (mergingTransformMap.containsKey(
				family)) {
			RowTransform transform = mergingTransformMap.get(
					family);

			final Mergeable mergeable = transform.getRowAsMergeableObject(
					family,
					new ByteArrayId(
							CellUtil.cloneQualifier(
									mergedCell)),
					CellUtil.cloneValue(
							mergedCell));

			if (mergeable != null) {
				final Mergeable otherMergeable = transform.getRowAsMergeableObject(
						family,
						new ByteArrayId(
								CellUtil.cloneQualifier(
										mergedCell)),
						CellUtil.cloneValue(
								peekedCell));

				if (otherMergeable != null) {
					mergeable.merge(
							otherMergeable);
					merged = true;
				}
				else {
					LOGGER.error(
							"Cell value is not Mergeable!");
				}

				mergedCell = CellUtil.createCell(
						CellUtil.cloneRow(
								mergedCell),
						CellUtil.cloneFamily(
								mergedCell),
						CellUtil.cloneQualifier(
								mergedCell),
						mergedCell.getTimestamp(),
						KeyValue.Type.Put.getCode(),
						PersistenceUtils.toBinary(
								mergeable));
			}
			else {
				LOGGER.error(
						"Cell value is not Mergeable!");
			}
		}
		else {
			LOGGER.error(
					"No merging transform for adapter: " + family.getString());
		}

		return merged;
	}

	private boolean getNextCell(
			ScannerContext scannerContext,
			Cell nextCell )
			throws IOException {
		List<Cell> cellList = new ArrayList<>();

		boolean hasMore = delegate.next(
				cellList,
				scannerContext);

		// Should generally be one cell for rasters
		LOGGER.debug(
				"MERGING SCANNER > next(2): got " + cellList.size() + " cells.");

		Mergeable mergedValue = null;
		Cell mergedCell = null;

		for (Cell cell : cellList) {
			if (mergedCell == null) {
				mergedCell = cell;
			}
			else {
				mergeCells(mergedCell, cell);
			}
		}
		
		nextCell = mergedCell;

		return hasMore;
	}

	@Override
	public void close()
			throws IOException {
		delegate.close();
	}

	public void setTransformMap(
			HashMap<ByteArrayId, RowTransform> mergingTransformMap ) {
		this.mergingTransformMap = mergingTransformMap;
	}
}
