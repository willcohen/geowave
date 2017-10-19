package mil.nga.giat.geowave.datastore.hbase.coprocessors;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.regionserver.InternalScanner;
import org.apache.hadoop.hbase.regionserver.KeyValueScanner;
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

	private KeyValueScanner delegate = null;
	private List<? extends KeyValueScanner> delegateList = null;
	private InternalScanner internalDelegate = null;
	private HashMap<ByteArrayId, RowTransform> mergingTransformMap;
	private Cell peekedCell = null;

	// TEST ONLY!
	static {
		LOGGER.setLevel(
				Level.DEBUG);
	}

	public MergingInternalScanner(
			final KeyValueScanner delegate ) {
		this.delegate = delegate;
	}

	public MergingInternalScanner(
			final List<? extends KeyValueScanner> delegateList ) {
		this.delegateList = delegateList;
	}

	public MergingInternalScanner(
			final InternalScanner internalDelegate ) {
		this.internalDelegate = internalDelegate;
	}

	@Override
	public boolean next(
			List<Cell> results,
			ScannerContext scannerContext )
			throws IOException {
		return nextInternal(
				results,
				scannerContext);
	}

	@Override
	public boolean next(
			List<Cell> results )
			throws IOException {
		return nextInternal(
				results,
				null);
	}

	private boolean nextInternal(
			List<Cell> results,
			ScannerContext scannerContext )
			throws IOException {
		Cell mergedCell = null;
		boolean hasMore = true;

		if (peekedCell != null) {
			mergedCell = copyCell(
					peekedCell,
					null);

			peekedCell = null;
		}
		else {
			hasMore = getNextCell(
					mergedCell);
		}

		// Peek ahead to see if it needs to be merged with the next result
		while (hasMore) {
			hasMore = getNextCell(
					peekedCell);
			if (peekedCell != null && CellUtil.matchingRow(
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

		results.clear();
		results.add(
				mergedCell);

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

				mergedCell = copyCell(
						mergedCell,
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
			Cell nextCell )
			throws IOException {
		boolean hasMore;

		if (delegate != null) {
			nextCell = delegate.next();
			hasMore = delegate.seek(
					nextCell);
		}
		else if (delegateList != null) {
			hasMore = getNextCellFromList(
					nextCell);
		}
		else { // if (internalDelegate != null) {
			hasMore = getNextCellInternal(
					nextCell,
					null);
		}

		return hasMore;
	}

	private boolean getNextCellFromList(
			Cell nextCell )
			throws IOException {
		Cell mergedCell = null;
		boolean hasMore = false;

		for (KeyValueScanner scanner : delegateList) {
			Cell cell = scanner.next();
			if (mergedCell == null) {
				mergedCell = cell;
			}
			else {
				mergeCells(
						mergedCell,
						cell);
			}

			hasMore = scanner.peek() != null;
		}

		if (mergedCell != null) {
			nextCell = copyCell(
					mergedCell,
					null);
		}

		return hasMore;
	}

	private boolean getNextCellInternal(
			Cell nextCell,
			ScannerContext scannerContext )
			throws IOException {
		List<Cell> cellList = new ArrayList<>();

		boolean hasMore;
		if (scannerContext != null) {
			hasMore = internalDelegate.next(
					cellList,
					scannerContext);
		}
		else {
			hasMore = internalDelegate.next(
					cellList);
		}

		// Should generally be one cell for rasters
		LOGGER.debug(
				"MERGING SCANNER > next: got " + cellList.size() + " cells.");

		Cell mergedCell = null;

		for (Cell cell : cellList) {
			if (mergedCell == null) {
				mergedCell = cell;
			}
			else {
				mergeCells(
						mergedCell,
						cell);
			}
		}

		if (mergedCell != null) {
			nextCell = copyCell(
					mergedCell,
					null);
		}

		return hasMore;
	}

	private Cell copyCell(
			Cell sourceCell,
			byte[] newValue ) {
		if (newValue == null) {
			newValue = CellUtil.cloneValue(
					sourceCell);
		}

		Cell newCell = CellUtil.createCell(
				CellUtil.cloneRow(
						sourceCell),
				CellUtil.cloneFamily(
						sourceCell),
				CellUtil.cloneQualifier(
						sourceCell),
				sourceCell.getTimestamp(),
				KeyValue.Type.Put.getCode(),
				newValue);

		return newCell;
	}

	@Override
	public void close()
			throws IOException {
		if (delegate != null) {
			delegate.close();
		}
		else if (delegateList != null) {
			for (KeyValueScanner scanner : delegateList) {
				scanner.close();
			}
		}
		else {
			internalDelegate.close();
		}
	}

	public void setTransformMap(
			HashMap<ByteArrayId, RowTransform> mergingTransformMap ) {
		this.mergingTransformMap = mergingTransformMap;
	}
}
