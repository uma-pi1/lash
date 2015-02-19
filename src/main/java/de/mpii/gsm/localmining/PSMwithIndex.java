package de.mpii.gsm.localmining;

import java.io.BufferedReader;
import java.io.DataInputStream;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.ArrayList;

import de.mpii.gsm.taxonomy.Taxonomy;
import de.mpii.gsm.writer.GsmWriter;
import de.mpii.gsm.utils.PrimitiveUtils;

import it.unimi.dsi.fastutil.ints.IntArrayList;
import it.unimi.dsi.fastutil.bytes.ByteArrayList;
import it.unimi.dsi.fastutil.ints.Int2IntOpenHashMap;
import it.unimi.dsi.fastutil.ints.Int2ObjectOpenHashMap;
import it.unimi.dsi.fastutil.ints.IntIterator;
import it.unimi.dsi.fastutil.ints.IntOpenHashSet;

public class PSMwithIndex {

	protected int sigma;

	protected int gamma;

	protected int lambda;

	protected ArrayList<int[]> inputTransactions = new ArrayList<int[]>();

	protected IntArrayList transactionSupports = new IntArrayList();

	protected Taxonomy taxonomy;

	protected int beginItem = 0;

	protected int endItem = Integer.MAX_VALUE;

	Int2IntOpenHashMap globallyFrequentItems = new Int2IntOpenHashMap();

	IntOpenHashSet tempSet = new IntOpenHashSet();

	GsmWriter writer;

	int pivotItemId = -1;

	Item pivotItem = new Item();

	private int _noOfFrequentPatterns = 0;

	private int[] transaction = null;

	int rightLevel = 0;

	int leftLevel = 0;

	IntOpenHashSet[][] scanIndex;

	IntOpenHashSet[][] rightIndex;

	int totalSequences = 0;

	public PSMwithIndex() {

	}

	public PSMwithIndex(int sigma, int gamma, int lambda, Taxonomy taxonomy) {
		this.sigma = sigma;
		this.gamma = gamma;
		this.lambda = lambda;
		this.taxonomy = taxonomy;
	}

	public void clear() {
		inputTransactions.clear();
		transactionSupports.clear();
		globallyFrequentItems.clear();
		pivotItem.clear();

		rightLevel = 0;
		leftLevel = 0;

	}

	public void setParameters(int sigma, int gamma, int lambda, Taxonomy taxonomy) {
		this.sigma = sigma;
		this.gamma = gamma;
		this.lambda = lambda;
		this.taxonomy = taxonomy;

		scanIndex = new IntOpenHashSet[lambda - 1][lambda - 1];
		rightIndex = new IntOpenHashSet[lambda - 1][lambda - 1];
		for (int i = 0; i < lambda - 1; ++i) {
			for (int j = 0; j < lambda - 1; ++j) {
				scanIndex[i][j] = new IntOpenHashSet();
				rightIndex[i][j] = new IntOpenHashSet();
			}
		}
		clear();
	}

	public void initialize() {
		initialize(0, Integer.MAX_VALUE);
	}

	public void initialize(int b, int e) {
		clear();
		this.beginItem = b;
		this.endItem = e;
		assert (b == e);
		this.pivotItemId = b;
	}

	public void scanDatabase(String dbFile) throws Exception {

		FileInputStream fstream;
		fstream = new FileInputStream(dbFile);
		DataInputStream in = new DataInputStream(fstream);
		BufferedReader br = new BufferedReader(new InputStreamReader(in));
		String strLine;

		while ((strLine = br.readLine()) != null) {
			String[] sequence = strLine.split("\\s* \\s*"); // TODO: take item
			// separator as
			// parameter
			int[] sequenceAsInts = new int[sequence.length];
			int i = 0;
			for (String term : sequence) {
				sequenceAsInts[i] = Integer.parseInt(term);
				i++;
			}
			addTransaction(sequenceAsInts, 0, sequenceAsInts.length, 1);
		}
		br.close();
	}

	public void addTransaction(int[] transaction, int fromIndex, int toIndex, int support) {
		int transactionId = transactionSupports.size();
		transactionSupports.add(support);

		int length = toIndex - fromIndex;
		int[] inputTransaction = new int[length];
		System.arraycopy(transaction, fromIndex, inputTransaction, 0, length);
		inputTransactions.add(inputTransaction);

		tempSet.clear();

		for (int i = fromIndex; i < toIndex; ++i) {
			assert transaction[i] <= endItem;
			if (transaction[i] < 0) {
				continue;
			}
			int itemId = transaction[i];
			if (pivotItemId == transaction[i]) { // pivot item(s)

				pivotItem.addTransaction(transactionId, support, i, i);

			}
			tempSet.add(itemId);
			while (taxonomy.hasParent(itemId)) {
				itemId = taxonomy.getParent(itemId);
				tempSet.add(itemId);
			}
		}
		for (int itemId : tempSet) {
			globallyFrequentItems.addTo(itemId, support);
		}
	}

	public void mine(GsmWriter writer) throws IOException, InterruptedException {
		this.writer = writer;

		_noOfFrequentPatterns = 0;
		totalSequences = 0;

		int[] prefix = new int[] { pivotItemId };

		rightExpansion(prefix, pivotItem.transactionIds);

		leftExpansion(prefix, pivotItem.transactionIds);

		for (int j = 0; j < lambda - 1; ++j) {
			scanIndex[0][j].clear();
			rightIndex[0][j].clear();
		}
		
		clear();
	}

	private void rightExpansion(int[] prefix, ByteArrayList transactionIds) throws IOException, InterruptedException {

		if (prefix.length == lambda)
			return;

		rightLevel++;
		boolean hasExtension = false;

		/** Projected database as list of transaction identifiers */
		PostingList.Decompressor transactions = new PostingList.Decompressor(transactionIds);

		Items localRightItems = new Items();

		do {
			int transactionId = transactions.nextValue();
			transaction = inputTransactions.get(transactionId);

			// for all positions
			while (transactions.hasNextValue()) {
				int leftPosition = transactions.nextValue();
				int rightPosition = transactions.nextValue();

				/** Count items in the right gamma+1 neighborhood */
				int gap = 0;
				for (int j = 0; gap <= gamma && (rightPosition + j + 1 < transaction.length); ++j) {
					int itemId = transaction[rightPosition + j + 1];
					if (itemId < 0) {
						gap -= itemId;
						continue;
					}
					gap++;

					if ((itemId < pivotItemId) && globallyFrequentItems.get(itemId) >= sigma)
						localRightItems.addItem(itemId, transactionId, transactionSupports.get(transactionId), leftPosition,
								(rightPosition + j + 1));
					/** Count ancestors */
					while (taxonomy.hasParent(itemId)) {
						itemId = taxonomy.getParent(itemId);
						if ((itemId < pivotItemId) && globallyFrequentItems.get(itemId) >= sigma)
							localRightItems.addItem(itemId, transactionId, transactionSupports.get(transactionId), leftPosition,
									(rightPosition + j + 1));
					}
				}
			}

		} while (transactions.nextPosting());

		totalSequences += localRightItems.itemIndex.size();

		int[] newPrefix = new int[prefix.length + 1];
		System.arraycopy(prefix, 0, newPrefix, 0, prefix.length);

		// for (Map.Entry<Integer, Item> entry :
		// localRightItems.itemIndex.entrySet()) {

		final IntIterator it = localRightItems.itemIndex.keySet().iterator();
		while (it.hasNext()) {

			int itemId = it.nextInt();
			Item item = localRightItems.itemIndex.get(itemId);

			if (item.support >= sigma) {
				// int itemId = entry.getKey();
				newPrefix[prefix.length] = itemId;

				_noOfFrequentPatterns++;
				hasExtension = true;

				if (writer != null)
					writer.write(newPrefix, item.support);

				rightIndex[leftLevel][rightLevel - 1].add(itemId);

				rightExpansion(newPrefix, item.transactionIds);

				// localRightItems.itemIndex.remove(itemId);
				it.remove();
			} else {
				// localRightItems.itemIndex.remove(itemId);
				it.remove();
			}
		}
		localRightItems.clear(); // TODO clear already processed items (increases runtime)
		rightLevel--;

		if (hasExtension) {
			scanIndex[leftLevel][rightLevel].add(prefix[prefix.length - 1]);
		}
	}

	private void leftExpansion(int[] prefix, ByteArrayList transactionIds) throws IOException, InterruptedException {
		if (prefix.length == lambda)
			return;

		leftLevel++;

		PostingList.Decompressor transactions = new PostingList.Decompressor(transactionIds);

		Items localLeftItems = new Items();

		do {
			int transactionId = transactions.nextValue();
			transaction = inputTransactions.get(transactionId);
			// for all positions
			while (transactions.hasNextValue()) {
				int leftPosition = transactions.nextValue();
				int rightPosition = transactions.nextValue();
				int gap = 0;

				for (int j = 0; gap <= gamma && (leftPosition - j - 1 >= 0); ++j) {
					int itemId = transaction[leftPosition - j - 1];
					if (itemId < 0) {
						gap -= itemId;
						continue;
					}
					gap++;
					if ((itemId <= pivotItemId) && globallyFrequentItems.get(itemId) >= sigma)
						localLeftItems.addItem(itemId, transactionId, transactionSupports.get(transactionId),
								(leftPosition - j - 1), rightPosition);
					// add parents
					while (taxonomy.hasParent(itemId)) {
						itemId = taxonomy.getParent(itemId);
						if ((itemId <= pivotItemId) && globallyFrequentItems.get(itemId) >= sigma)
							localLeftItems.addItem(itemId, transactionId, transactionSupports.get(transactionId),
									(leftPosition - j - 1), rightPosition);
					}
				}

			}
		} while (transactions.nextPosting());

		totalSequences += localLeftItems.itemIndex.size();

		int[] newPrefix = new int[prefix.length + 1];
		System.arraycopy(prefix, 0, newPrefix, 1, prefix.length);

		final IntIterator it = localLeftItems.itemIndex.keySet().iterator();
		while (it.hasNext()) {

			int itemId = it.nextInt();

			Item item = localLeftItems.itemIndex.get(itemId);

			if (item.support >= sigma) {

				newPrefix[0] = itemId;

				_noOfFrequentPatterns++;
				if (writer != null)
					writer.write(newPrefix, item.support);

				rightOfLeftExpansion(newPrefix, item.transactionIds);

				leftExpansion(newPrefix, item.transactionIds);

				// localLeftItems.itemIndex.remove(itemId);
				it.remove();

			} else {
				// localLeftItems.itemIndex.remove(itemId);
				it.remove();
			}
		}
		localLeftItems.clear(); // TODO: clear already processing postings
														// (increases runtime)

		leftLevel--;

		for (int j = 0; j < lambda - 1; ++j) {
			scanIndex[leftLevel][j].clear();
			rightIndex[leftLevel][j].clear();
		}
	}

	private void rightOfLeftExpansion(int[] prefix, ByteArrayList transactionIds) throws IOException,
			InterruptedException {

		// Early pruning
		if (prefix.length == lambda || !hasRightExtension((prefix.length - leftLevel), (prefix[prefix.length - 1])))
			return;

		rightLevel++;
		boolean hasExtension = false;

		// Projected database as list of transaction identifiers
		PostingList.Decompressor transactions = new PostingList.Decompressor(transactionIds);

		Items localRightItems = new Items();

		do {
			int transactionId = transactions.nextValue();
			transaction = inputTransactions.get(transactionId);
			// for all positions
			while (transactions.hasNextValue()) {
				int leftPosition = transactions.nextValue();
				int rightPosition = transactions.nextValue();

				/** Count items in the right gamma+1 neighborhood */
				int gap = 0;
				for (int j = 0; gap <= gamma && (rightPosition + j + 1 < transaction.length); ++j) {
					int itemId = transaction[rightPosition + j + 1];
					if (itemId < 0) {
						gap -= itemId;
						continue;
					}
					gap++;

					if ((itemId < pivotItemId) && isRightFrequent((prefix.length - leftLevel), itemId))
						localRightItems.addItem(itemId, transactionId, transactionSupports.get(transactionId), leftPosition,
								(rightPosition + j + 1));
					// add parents
					while (taxonomy.hasParent(itemId)) {
						itemId = taxonomy.getParent(itemId);
						if ((itemId < pivotItemId) && isRightFrequent((prefix.length - leftLevel), itemId))
							localRightItems.addItem(itemId, transactionId, transactionSupports.get(transactionId), leftPosition,
									(rightPosition + j + 1));
					}
				}
			}

		} while (transactions.nextPosting());

		totalSequences += localRightItems.itemIndex.size();

		int[] newPrefix = new int[prefix.length + 1];
		System.arraycopy(prefix, 0, newPrefix, 0, prefix.length);

		final IntIterator it = localRightItems.itemIndex.keySet().iterator();
		while (it.hasNext()) {

			int itemId = it.nextInt();
			Item item = localRightItems.itemIndex.get(itemId);

			if (item.support >= sigma) {

				newPrefix[prefix.length] = itemId;

				_noOfFrequentPatterns++;
				hasExtension = true;

				if (writer != null)
					writer.write(newPrefix, item.support);

				rightIndex[leftLevel][rightLevel - 1].add(itemId);

				rightOfLeftExpansion(newPrefix, item.transactionIds);

				// localRightItems.itemIndex.remove(itemId);
				it.remove();

			} else {
				// localRightItems.itemIndex.remove(itemId);
				it.remove();
			}
		}
		localRightItems.clear(); // TODO clear already processed items (increases
															// runtime)
		rightLevel--;

		if (hasExtension) {
			scanIndex[leftLevel][rightLevel].add(prefix[prefix.length - 1]);
		}

	}

	/*
	 * private void addToRightIndex(int scanItem, int countItem) {
	 * 
	 * //countSet[leftLevel].add(PrimitiveUtils.combine(rightLevel, countItem)); }
	 */

	private boolean isRightFrequent(int level, int item) {

		// return countSet[leftLevel-1].contains(PrimitiveUtils.combine(level,
		// item));
		return rightIndex[leftLevel - 1][level - 1].contains(item);

	}

	private boolean hasRightExtension(int level, int r) {

		// return (scanSet[leftLevel - 1].contains(PrimitiveUtils.combine(l, r)));
		return scanIndex[leftLevel - 1][level - 1].contains(r);
	}

	public int noOfFrequentPatterns() {
		return _noOfFrequentPatterns;
	}

	public int noOfTotalSequences() {
		return totalSequences;
	}

	public static void main(String[] args) {
		// TODO Auto-generated method stub

	}

	// -- HELPER CLASSES --

	private static final class Item {

		int support;

		int lastTransactionId;

		long lastPosition;

		ByteArrayList transactionIds;

		Item() {
			support = 0;
			lastTransactionId = -1;
			lastPosition = -1;
			transactionIds = new ByteArrayList();
		}

		public void addTransaction(int transactionId, int support, int l, int r) {
			this.support = support;

			long lr = PrimitiveUtils.combine(l, r);

			if (lastTransactionId != transactionId) {

				/** Add transaction separator */
				if (transactionIds.size() > 0) {
					PostingList.addCompressed(0, transactionIds);
				}

				lastTransactionId = transactionId;
				lastPosition = lr;
				support += support;
				PostingList.addCompressed(transactionId + 1, transactionIds);
				PostingList.addCompressed(l + 1, transactionIds);
				PostingList.addCompressed(r + 1, transactionIds);

			} else if (lastPosition != lr) {
				PostingList.addCompressed(l + 1, transactionIds);
				PostingList.addCompressed(r + 1, transactionIds);
				lastPosition = lr;
			}

		}

		public void clear() {
			support = 0;
			lastTransactionId = -1;
			lastPosition = -1;
			transactionIds.clear();
		}

	}

	private static final class Items {

		Int2ObjectOpenHashMap<Item> itemIndex = new Int2ObjectOpenHashMap<Item>();

		public void addItem(int itemId, int transactionId, int support, int l, int r) {
			Item item = itemIndex.get(itemId);
			if (item == null) {
				item = new Item();
				itemIndex.put(itemId, item);
				// baseItems.add(itemId);
			}
			long lr = PrimitiveUtils.combine(l, r);
			if (item.lastTransactionId != transactionId) {

				/** Add transaction separator */
				if (item.transactionIds.size() > 0) {
					PostingList.addCompressed(0, item.transactionIds);
				}

				item.lastTransactionId = transactionId;
				item.lastPosition = lr;
				item.support += support;
				PostingList.addCompressed(transactionId + 1, item.transactionIds);
				PostingList.addCompressed(l + 1, item.transactionIds);
				PostingList.addCompressed(r + 1, item.transactionIds);

			} else if (item.lastPosition != lr) {
				PostingList.addCompressed(l + 1, item.transactionIds);
				PostingList.addCompressed(r + 1, item.transactionIds);
				item.lastPosition = lr;
			}
		}

		public void clear() {
			itemIndex.clear();
		}
	}

}
