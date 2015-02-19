package de.mpii.gsm.localmining;

import it.unimi.dsi.fastutil.ints.IntArrayList;
import it.unimi.dsi.fastutil.bytes.ByteArrayList;
import org.apache.mahout.math.map.OpenIntIntHashMap;
import it.unimi.dsi.fastutil.ints.Int2ObjectOpenHashMap;
import it.unimi.dsi.fastutil.ints.IntOpenHashSet;

import java.io.BufferedReader;
import java.io.DataInputStream;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.Map;

import de.mpii.gsm.taxonomy.Taxonomy;
import de.mpii.gsm.writer.GsmWriter;
import de.mpii.gsm.utils.PrimitiveUtils;

public class PSM {
	
	protected int sigma;

	protected int gamma;

	protected int lambda;

	protected ArrayList<int[]> inputTransactions = new ArrayList<int[]>();

	protected IntArrayList transactionSupports = new IntArrayList();

	protected Taxonomy taxonomy;

	protected Items pivotItems = new Items();

	private int _noOfFrequentPatterns = 0;

	protected int beginItem = 0;

	protected int endItem = Integer.MAX_VALUE;

	IntOpenHashSet tempSet = new IntOpenHashSet();

	OpenIntIntHashMap globallyFrequentItems = new OpenIntIntHashMap();

	private int[] transaction = null;
	
	GsmWriter writer;

	public PSM() {
	}

	public PSM(int sigma, int gamma, int lambda, Taxonomy taxonomy) {
		this.sigma = sigma;
		this.gamma = gamma;
		this.lambda = lambda;
		this.taxonomy = taxonomy;
	}

	public void clear() {
		inputTransactions.clear();
		transactionSupports.clear();
		pivotItems.clear();
		globallyFrequentItems.clear();
	}

	public void setParameters(int sigma, int gamma, int lambda, Taxonomy taxonomy) {
		this.sigma = sigma;
		this.gamma = gamma;
		this.lambda = lambda;
		this.taxonomy = taxonomy;
		clear();
	}

	public void initialize() {
		initialize(0, Integer.MAX_VALUE);
	}

	public void initialize(int b, int e) {
		clear();
		this.beginItem = b;
		this.endItem = e;
	}

	public void scanDatabase(String dbFile) throws Exception {

		FileInputStream fstream;
		fstream = new FileInputStream(dbFile);
		DataInputStream in = new DataInputStream(fstream);
		BufferedReader br = new BufferedReader(new InputStreamReader(in));
		String strLine;

		while ((strLine = br.readLine()) != null) {
			String[] sequence = strLine.split("\\s* \\s*"); // TODO: take item separator as parameter
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
			if (beginItem <= transaction[i]) { // pivot item(s)

				pivotItems.addItem(itemId, transactionId, support, i, i);

				
			}
			itemId = transaction[i];
			tempSet.add(itemId);
			while (taxonomy.hasParent(itemId)) {
				itemId = taxonomy.getParent(itemId);
				tempSet.add(itemId);
			}
		}
		for (int itemId : tempSet) {
			globallyFrequentItems.adjustOrPutValue(itemId, +support, +support);
		}
	}

	public void mine(GsmWriter writer) throws IOException, InterruptedException {
		this.writer = writer;
		_noOfFrequentPatterns = 0;

		int[] prefix = new int[1];

		System.out.println("pivots = " + pivotItems.itemIndex.size());

		for (Map.Entry<Integer, Item> entry : pivotItems.itemIndex.entrySet()) {
			Item item = entry.getValue();
			if (item.support >= sigma) {
				prefix[0] = entry.getKey();
				growRight(prefix, item.transactionIds, prefix[0], false);
				growLeft(prefix, item.transactionIds, prefix[0]);
			}
		}
		clear();
	}

	private void growRight(int[] prefix, ByteArrayList transactionIds, int pivotItem, boolean fromLeft)
			throws IOException, InterruptedException {
		if (prefix.length == lambda)
			return;
		PostingList.Decompressor transactions = new PostingList.Decompressor(transactionIds);

		Items localRightItems = new Items();

		do {
			int transactionId = transactions.nextValue();
			transaction = inputTransactions.get(transactionId);
			// for all positions
			while (transactions.hasNextValue()) {
				int leftPosition = transactions.nextValue();
				int rightPosition = transactions.nextValue();

				/** Add items in the right gamma+1 neighborhood */
				int gap = 0;
				for (int j = 0; gap <= gamma && (rightPosition + j + 1 < transaction.length); ++j) {
					int itemId = transaction[rightPosition + j + 1];
					if (itemId < 0) {
						gap -= itemId;
						continue;
					}
					gap++;
					if ((itemId < pivotItem) && globallyFrequentItems.get(itemId) >= sigma)
						localRightItems.addItem(itemId, transactionId, transactionSupports.get(transactionId), leftPosition,
								(rightPosition + j + 1));
					// add parents
					while (taxonomy.hasParent(itemId)) {
						itemId = taxonomy.getParent(itemId);
						if ((itemId < pivotItem) && globallyFrequentItems.get(itemId) >= sigma)
							localRightItems.addItem(itemId, transactionId, transactionSupports.get(transactionId), leftPosition,
									(rightPosition + j + 1));
					}
				}
			}

		} while (transactions.nextPosting());

		int[] newPrefix = new int[prefix.length + 1];

		for (Map.Entry<Integer, Item> entry : localRightItems.itemIndex.entrySet()) {
			Item item = entry.getValue();
			if (item.support >= sigma) {
				System.arraycopy(prefix, 0, newPrefix, 0, prefix.length);
				newPrefix[prefix.length] = entry.getKey();

				_noOfFrequentPatterns++;
				if (writer != null)
					writer.write(newPrefix, item.support);

				growRight(newPrefix, item.transactionIds, pivotItem, false);
			}
		}
		localRightItems.clear();
	}

	// Left
	private void growLeft(int[] prefix, ByteArrayList transactionIds, int pivotItem) throws IOException,
			InterruptedException {

		if (prefix.length == lambda)
			return;
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
					if ((itemId <= pivotItem) && globallyFrequentItems.get(itemId) >= sigma)
						localLeftItems.addItem(itemId, transactionId, transactionSupports.get(transactionId),
								(leftPosition - j - 1), rightPosition);
					// add parents
					while (taxonomy.hasParent(itemId)) {
						itemId = taxonomy.getParent(itemId);
						if ((itemId <= pivotItem) && globallyFrequentItems.get(itemId) >= sigma)
							localLeftItems.addItem(itemId, transactionId, transactionSupports.get(transactionId),
									(leftPosition - j - 1), rightPosition);
					}
				}

			}
		} while (transactions.nextPosting());

		int[] newPrefix = new int[prefix.length + 1];

		for (Map.Entry<Integer, Item> entry : localLeftItems.itemIndex.entrySet()) {
			Item item = entry.getValue();
			if (item.support >= sigma) {
				System.arraycopy(prefix, 0, newPrefix, 1, prefix.length);
				newPrefix[0] = entry.getKey();

				_noOfFrequentPatterns++;
				if (writer != null)
					writer.write(newPrefix, item.support);

				growRight(newPrefix, item.transactionIds, pivotItem, true);
				growLeft(newPrefix, item.transactionIds, pivotItem);
			}
		}
		localLeftItems.clear();

	}

	public int noOfFrequentPatterns() {
		return _noOfFrequentPatterns;
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
