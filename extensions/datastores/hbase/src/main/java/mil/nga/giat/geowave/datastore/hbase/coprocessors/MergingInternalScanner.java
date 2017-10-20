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
	private final static Logger LOGGER = Logger.getLogger(MergingInternalScanner.class);

	private KeyValueScanner delegate = null;
	private List<? extends KeyValueScanner> delegateList = null;
	private InternalScanner internalDelegate = null;
	private HashMap<ByteArrayId, RowTransform> mergingTransformMap;
	private NextCell peekedCell = null;

	static class NextCell
	{
		boolean hasMore;
		Cell cell;
	}

	// TEST ONLY!
	static {
		LOGGER.setLevel(Level.DEBUG);
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
		NextCell mergedCell = new NextCell();

		if (peekedCell != null) {
			mergedCell.cell = copyCell(
					peekedCell.cell,
					null);
			mergedCell.hasMore = peekedCell.hasMore;

			peekedCell = null;
		}
		else {
			mergedCell = getNextCell();
		}

		// Peek ahead to see if it needs to be merged with the next result
		while (mergedCell.hasMore) {
			peekedCell = getNextCell();
			if (peekedCell.cell != null && CellUtil.matchingRow(
					mergedCell.cell,
					peekedCell.cell)) {
				mergedCell.cell = mergeCells(
						mergedCell.cell,
						peekedCell.cell);
				mergedCell.hasMore = peekedCell.hasMore;
			}
			else {
				break;
			}
		}

		results.clear();
		results.add(mergedCell.cell);

		return mergedCell.hasMore;
	}

	/**
	 * Assumes cells share family and qualifier
	 * 
	 * @param cell1
	 * @param cell2
	 * @return
	 */
	private Cell mergeCells(
			Cell cell1,
			Cell cell2 ) {
		Cell mergedCell = null;

		ByteArrayId family = new ByteArrayId(
				CellUtil.cloneFamily(cell1));

		if (mergingTransformMap.containsKey(family)) {
			RowTransform transform = mergingTransformMap.get(family);

			final Mergeable mergeable = transform.getRowAsMergeableObject(
					family,
					new ByteArrayId(
							CellUtil.cloneQualifier(cell1)),
					CellUtil.cloneValue(cell1));

			if (mergeable != null) {
				final Mergeable otherMergeable = transform.getRowAsMergeableObject(
						family,
						new ByteArrayId(
								CellUtil.cloneQualifier(cell1)),
						CellUtil.cloneValue(cell2));

				if (otherMergeable != null) {
					mergeable.merge(otherMergeable);
				}
				else {
					LOGGER.error("Cell value is not Mergeable!");
				}

				mergedCell = copyCell(
						cell1,
						PersistenceUtils.toBinary(mergeable));
			}
			else {
				LOGGER.error("Cell value is not Mergeable!");
			}
		}
		else {
			LOGGER.error("No merging transform for adapter: " + family.getString());
		}

		return mergedCell;
	}

	private NextCell getNextCell()
			throws IOException {
		NextCell nextCell;

		if (delegate != null) {
			nextCell = new NextCell();
			nextCell.cell = delegate.next();
			nextCell.hasMore = delegate.peek() != null;
		}
		else if (delegateList != null) {
			nextCell = getNextCellFromList();
		}
		else { // if (internalDelegate != null) {
			nextCell = getNextCellInternal();
		}

		return nextCell;
	}

	private NextCell getNextCellFromList()
			throws IOException {
		NextCell nextCell = new NextCell();

		Cell mergedCell = null;
		boolean hasMore = false;

		for (KeyValueScanner scanner : delegateList) {
			Cell cell = scanner.next();
			if (mergedCell == null) {
				mergedCell = cell;
			}
			else {
				mergedCell = mergeCells(
						mergedCell,
						cell);
			}

			hasMore = scanner.peek() != null;
		}

		if (mergedCell != null) {
			nextCell.cell = copyCell(
					mergedCell,
					null);
			nextCell.hasMore = hasMore;
		}

		return nextCell;
	}

	private NextCell getNextCellInternal()
			throws IOException {
		List<Cell> cellList = new ArrayList<>();
		NextCell nextCell = new NextCell();

		boolean hasMore = internalDelegate.next(cellList);

		// Should generally be one cell for rasters
		LOGGER.debug("MERGING SCANNER > next: got " + cellList.size() + " cells.");

		Cell mergedCell = null;

		for (Cell cell : cellList) {
			if (mergedCell == null) {
				mergedCell = cell;
			}
			else {
				mergedCell = mergeCells(
						mergedCell,
						cell);
			}
		}

		if (mergedCell != null) {
			nextCell.cell = copyCell(
					mergedCell,
					null);
			nextCell.hasMore = hasMore;
		}

		return nextCell;
	}

	private Cell copyCell(
			Cell sourceCell,
			byte[] newValue ) {
		if (newValue == null) {
			newValue = CellUtil.cloneValue(sourceCell);
		}

		Cell newCell = CellUtil.createCell(
				CellUtil.cloneRow(sourceCell),
				CellUtil.cloneFamily(sourceCell),
				CellUtil.cloneQualifier(sourceCell),
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
