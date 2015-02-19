package de.mpii.gsm.localmining;

import de.mpii.gsm.taxonomy.*;
import de.mpii.gsm.writer.GsmWriter;
import de.mpii.gsm.utils.IntArrayStrategy;



import it.unimi.dsi.fastutil.ints.IntArrayList;
import it.unimi.dsi.fastutil.bytes.ByteArrayList;
import it.unimi.dsi.fastutil.objects.Object2ObjectOpenCustomHashMap;
import it.unimi.dsi.fastutil.objects.Object2ObjectOpenHashMap;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Map;

public class Bfs {

	// -- parameters
	// --------------------------------------------------------------------------------

	/** Minimum support */
	protected int sigma;

	/** Maximum gap */
	protected int gamma;

	/** Maximum length */
	protected int lambda;

	/**
	 * Start of pivot range (see class description). Set to 0 to mine all frequent
	 * sequences.
	 */
	protected int beginItem = 0;

	/**
	 * End of pivot range (see class description). Set to {@link Integer.MAXVALUE}
	 * to mine all frequent sequences.
	 */
	protected int endItem = Integer.MAX_VALUE;

	// -- internal variables
	// ------------------------------------------------------------------------

	// At any point of time, we store an inverted index that maps subsequences of
	// length k
	// to their posting lists.
	//
	// During data input, we have k=2. Every input transaction is added to the
	// index (by generating
	// all its (2,gamma)-subsequences and then discarded.
	//
	// During frequent sequence mining, we compute repeatedly compute length-(k+1)
	// subsequences
	// from the length-k subsequences.

	/** Length of subsequences currently being mined */
	protected int k;

	/**
	 * A list of sequences of length k; no sequence occurs more than once. Each
	 * sequence is stored in either uncompressed or compressed format.
	 * 
	 * In uncompressed format, each sequence is encoded as an array of exactly k
	 * item identifiers. When k=2, all sequences are stored in uncompressed
	 * format.
	 * 
	 * In compressed format, each sequence is encoded as a length-2 array (p, w).
	 * To reconstruct the uncompressed sequence, take the first k-1 items from the
	 * sequence at position p in kSequences (p is a "prefix pointer") and set the
	 * k-th item to w (suffix item). When k>2, an entry is compressed when it has
	 * two elements and uncompressed when it has k elements.
	 */
	protected ArrayList<int[]> kSequences = new ArrayList<int[]>();

	/**
	 * Maps 2-sequence to their position in kSequences (lowest 32 bits) and their
	 * largest transaction id (highest 32 bits). Only used during data input, k=2.
	 */
	Map<int[], KSequenceIndexEntry> kSequenceIndex = new Object2ObjectOpenCustomHashMap<int[], KSequenceIndexEntry>(
			new IntArrayStrategy());

	/** Holds information about a posting list. Used only during data input, k=32. */
	protected class KSequenceIndexEntry {
		int index;
		int lastTransactionId;
		int lastPosition;
	}

	/** Used as a temporary buffer during data input. */
	// final int[] twoSequence = new int[2];
	protected int[] twoSequence = new int[2];

	/**
	 * Posting list for each sequence in kSequences. A posting list is a set of
	 * postings, one for each transaction in which the sequence occurs. Every
	 * posting consists of a transaction identifier and a list of starting
	 * positions (at which a match of the sequence occurs in the respective
	 * transactions). Transactions and starting positions within transactions are
	 * sorted in ascending order. Each posting is encoded using variable-length
	 * integer encoding; postings are separated by a 0 byte. To avoid collisions,
	 * we store transactionId+1 and position+1. (Note that not every 0 byte
	 * separates posting; the byte before the 0 byte must have its
	 * highest-significant bit set to 0).
	 */
	protected ArrayList<ByteArrayList> kPostingLists = new ArrayList<ByteArrayList>();

	/**
	 * Support of each transaction (indexed by transaction id). If an input
	 * transaction has support larger than one, is it treated as if it had occured
	 * in the data as many times as given by its support value.
	 */
	protected IntArrayList transactionSupports = new IntArrayList();

	/**
	 * Total support for each sequence in kSequences. Identical to the sum of the
	 * supports of each transaction that occurs in the posting list.
	 */
	protected IntArrayList kTotalSupports = new IntArrayList();

	protected Taxonomy taxonomy;

	protected int noOfPatterns_ = 0;

	public Bfs() {

	}

	/**
	 * @param sigma minimum support
	 * @param gamma maximum gap
	 * @param lambda maximum length
	 * @param taxonomy taxonomy
	 */
	public Bfs(int sigma, int gamma, int lambda, Taxonomy taxonomy) {
		this.sigma = sigma;
		this.gamma = gamma;
		this.lambda = lambda;
		this.taxonomy = taxonomy;
	}

	/** Flushes all internal data structures. */
	public void clear() {
		k = 2;
		kSequenceIndex.clear();
		kSequences.clear();
		kPostingLists.clear();
		kTotalSupports.clear();
		transactionSupports.clear();
	}

	/**
	 * Updates the parameters used for sequence mining and flushes internal data
	 * structures.
	 *
	 * @param sigma
	 *          minimum support
	 * @param gamma
	 *          maximum gap
	 * @param lambda
	 *          maximum length
	 */
	public void setParametersAndClear(int sigma, int gamma, int lambda, Taxonomy taxonomy) {
		this.sigma = sigma;
		this.gamma = gamma;
		this.lambda = lambda;
		this.taxonomy = taxonomy;
		clear();
	}

	/**
	 * Initialize frequent sequence miner (without pivots). Should be called
	 * before any data input.
	 */
	public void initialize() {
		initialize(0, Integer.MAX_VALUE);
	}

	/**
	 * Initialize frequent sequence miner (with pivots). Should be called before
	 * any data input.
	 * 
	 * @param beginItem
	 *          begin of pivot range (see class description)
	 * @param endItem
	 *          end of pivot range (see class description)
	 */
	public void initialize(int beginItem, int endItem) {
		clear();
		this.beginItem = beginItem;
		this.endItem = endItem;
	}

	// -- input phase
	// -------------------------------------------------------------------------------

	public void addTransaction(int[] transaction, int fromIndex, int toIndex, int support) {
		// only valid during data input phase
		assert k == 2;
		assert kSequences.size() == kSequenceIndex.size();

		// store the support of the input transaction
		int transactionId = transactionSupports.size();
		transactionSupports.add(support);

		// Add the transaction to the inverted index. Here we construct all
		// gapped 2-sequences
		// and update their corresponding posting lists
		int position = 0; // current position in expanded sequence (i.e.,
		// without compressed gaps)
		for (int i = fromIndex; i < toIndex; i++) {
			assert transaction[i] <= endItem; // contract of this class

			// skip gaps
			if (transaction[i] < 0) {
				position -= transaction[i];
				continue;
			}

			// create all 2-subsequences
			// i points to first item, j to second item
			for (int j = i + 1; j < toIndex && isWithinGap(transaction, i, j, gamma); j++) {
				// skip gaps
				if (transaction[j] < 0) {
					continue;
				}

				// we found a valid 2-sequence; create a posting for the two sequence
				// and its generalizations
				twoSequence[0] = transaction[i];

				while (true) {
					twoSequence[1] = transaction[j];
					addPosting(twoSequence, transactionId, support, position);

					while (taxonomy.hasParent(twoSequence[1])) {
						twoSequence[1] = taxonomy.getParent(twoSequence[1]);
						addPosting(twoSequence, transactionId, support, position);
					}
					if (taxonomy.hasParent(twoSequence[0])) {
						twoSequence[0] = taxonomy.getParent(twoSequence[0]);
					} else {
						break;
					}

				}
			}
			position++;
		}
	}

	/**
	 * Adds an occurrence of a 2-sequence to the inverted index. Only used for
	 * 2-sequences during the input phase. The provided kSequence is not stored,
	 * i.e., can be reused.
	 */
	protected void addPosting(int[] kSequence, int transactionId, int support, int position) {
		// get the posting list for the current sequence
		// if the sequence has not seen before, create a new posting list
		KSequenceIndexEntry entry = kSequenceIndex.get(kSequence);

		ByteArrayList postingList;

		if (entry == null) {
			// we never saw this 2-sequence before
			entry = new KSequenceIndexEntry();
			entry.index = kSequences.size();
			entry.lastTransactionId = -1;
			kSequence = new int[] { kSequence[0], kSequence[1] }; // copy necessary
																														// here
			kSequences.add(kSequence);
			kSequenceIndex.put(kSequence, entry);
			postingList = new ByteArrayList();
			kPostingLists.add(postingList);
			kTotalSupports.add(0);
		} else {
			// a new occurrence of a previously seen 2-sequence
			postingList = kPostingLists.get(entry.index);
		}

		// add the current occurrence to the posting list
		if (entry.lastTransactionId != transactionId) {
			if (postingList.size() > 0) {
				// add a separator
				PostingList.addCompressed(0, postingList);
			}
			// add transaction id
			PostingList.addCompressed(transactionId + 1, postingList);
			PostingList.addCompressed(position + 1, postingList);

			// update data structures
			entry.lastTransactionId = transactionId;
			entry.lastPosition = position;
			kTotalSupports.set(entry.index, kTotalSupports.get(entry.index) + support);
		} else if (entry.lastPosition != position) { // don't add any position more
																									// than once
			PostingList.addCompressed(position + 1, postingList);
			entry.lastPosition = position;
		}
	}

	/**
	 * Checks whether there are less than gamma items in between the items
	 * indicated by the start and end pointers. Correctly treats gap entries
	 * (e.g., -3 indicating 3 irrelevant items).
	 *
	 * @param transaction
	 *          input transaction
	 * @param index1
	 *          index of first item in transaction
	 * @param index2
	 *          index of second item in transaction
	 * @param gamma
	 *          maximum gap
	 * @return
	 */
	protected boolean isWithinGap(int[] transaction, int index1, int index2, int gamma) {
		if (index2 - index1 > gamma + 1)
			return false; // quick check, for efficiency
		int gap = 0;
		for (int i = index1 + 1; i < index2; i++) {
			if (transaction[i] < 0) {
				gap -= transaction[i];
			} else {
				gap++;
			}
			if (gap > gamma) {
				return false;
			}
		}
		return true;
	}

	/**
	 * Finalizes the input phase by computing the overall support of each
	 * 2-sequence and pruning all 2-sequences below minimum support.
	 */
	public void computeTwoSequences() {
		// returning the 2-sequences that have support equal or above minsup and
		// their posting lists
		kSequenceIndex.clear(); // not needed anymore
		transactionSupports.trim(); // will not be changed anymore

		// compute total support of each sequence and remove sequences with support
		// less than sigma
		for (int id = 0; id < kSequences.size();) {
			if (kTotalSupports.get(id) >= sigma) {
				// accept sequence
				// uncomment next line to save some space during 1st phase (but:
				// increased runtime)
				// postingList.trimToSize();
				id++; // next id
			} else {
				// delete the current sequence (by moving the last sequence to the
				// current position)
				int size1 = kPostingLists.size() - 1;
				if (id < size1) {
					kSequences.set(id, kSequences.remove(size1));
					kPostingLists.set(id, kPostingLists.remove(size1));
					kTotalSupports.set(id, kTotalSupports.get(size1));
          kTotalSupports.remove(size1);
				} else {
					kSequences.remove(size1);
					kPostingLists.remove(size1);
					kTotalSupports.remove(size1);
				}
				// same id again (now holding a different kSequence)
			}
		}
	}

	public int noOfPatterns() {
		return noOfPatterns_;
	}

	// -- mining phase
	// ------------------------------------------------------------------------------

	public void mineAndClear(GsmWriter writer) throws IOException, InterruptedException {

		noOfPatterns_ = 0;

		computeTwoSequences();

		noOfPatterns_ += kSequences.size();
		outputKSequences(writer);
		while ((k < lambda) && !kSequences.isEmpty()) {
			bfsTraversal();
			outputKSequences(writer);
			noOfPatterns_ += kSequences.size();
		}
		clear();
	}

	/**
	 * Outputs all k-sequences that contain a pivot.
	 * 
	 * @throws InterruptedException
	 * @throws IOException
	 */
	private void outputKSequences(GsmWriter writer) throws IOException, InterruptedException {
		int[] prefixSequence = null;
		int[] temp = new int[k];

		// walk over all sequences
		for (int i = 0; i < kSequences.size(); i++) {
			int[] sequence = kSequences.get(i);

			// uncompress sequence (if necessary)
			if (k == 2 || sequence.length == k) {
				// uncompressed sequence
				prefixSequence = sequence;
			} else {
				// compressed sequence (entries = (prefix pointer, suffix item)
				// reconstruct whole sequence by taking first k-1 symbols taken from
				// previous sequence
				// plus the given suffix
				System.arraycopy(prefixSequence, 0, temp, 0, prefixSequence.length - 1);
				temp[k - 1] = sequence[1]; // suffix item
				sequence = temp;
			}

			// check if the sequence contains a pivot
			boolean hasPivot = false;
			for (int word : sequence) {
				if (word >= beginItem) {
					assert word <= endItem; // contract of thisoffse class
					hasPivot = true;
					break;
				}
			}

			// if contains a pivot, output the sequence and its support
			if (hasPivot) {
				if (writer != null)
					writer.write(sequence, kTotalSupports.get(i));
				// output here...
				// System.out.println(Arrays.toString(sequence) + " " +
				// kTotalSupports.get(i));
				// System.out.println(Arrays.toString(sequence));
				// Global.writeToFile(sequence, kTotalSupports.get(i));
			}

		}
	}

	/**
	 * This method constructs all frequent (k+1)-sequences from the set of
	 * k-sequences (values of k, kSequences, kPostings, kTotalSupport are
	 * updated).
	 */
	protected void bfsTraversal() {
		// terminology
		// k : 5
		// k1 : 6
		// k- sequence : abcde
		// prefix : abcd (= right join key)
		// suffix : bcde (= left join key)

		// build prefix/suffix indexes (maps prefix/suffix to list of sequences with
		// this prefix/suffix)
		// values point to indexes in kSequences
		Map<IntArrayList, IntArrayList> sequencesWithSuffix = new Object2ObjectOpenHashMap<IntArrayList, IntArrayList>();
		Map<IntArrayList, IntArrayList> sequencesWithPrefix = new Object2ObjectOpenHashMap<IntArrayList, IntArrayList>();
		buildPrefixSuffixIndex(sequencesWithPrefix, sequencesWithSuffix);

		// variables for the (k+1)-sequences
		int k1 = k + 1;
		ArrayList<int[]> k1Sequences = new ArrayList<int[]>();
		ArrayList<ByteArrayList> k1PostingLists = new ArrayList<ByteArrayList>();
		IntArrayList k1TotalSupports = new IntArrayList();

		// temporary variables
		ByteArrayList postingList = new ByteArrayList(); // list of postings for a
																												// new (k+1) sequence
		PostingList.Decompressor leftPostingList = new PostingList.Decompressor(); // posting
																																								// list
																																								// of
																																								// the
																																								// left
																																								// k-sequence
		PostingList.Decompressor rightPostingList = new PostingList.Decompressor(); // posting
																																								// list
																																								// of
																																								// the
																																								// right
																																								// k-sequence

		// we now join sequences (e.g., abcde) that end with some suffix with
		// sequences
		// that start with the same prefix (e.g., bcdef)
		for (Map.Entry<IntArrayList, IntArrayList> entry : sequencesWithSuffix.entrySet()) {
			// if there is no right key to join, continue
			IntArrayList joinKey = entry.getKey();
			IntArrayList rightSequences = sequencesWithPrefix.get(joinKey); // indexes
																																				// of
																																				// right
																																				// sequences
			if (rightSequences == null) {
				continue;
			}

			// there are right keys for the join, so let's join
			IntArrayList leftSequences = entry.getValue(); // indexes of left
																											// sequences
			for (int i = 0; i < leftSequences.size(); i++) {
				// get the postings of that sequence for joining
				leftPostingList.postingList = kPostingLists.get(leftSequences.get(i));

				// compression
				// total number of successful joins for the current left sequence
				int noK1SequencesForLeftSequence = 0;
				int pointerToFirstK1Sequence = -1; // index of first join match

				// for every right key that matches the current left key, perform
				// a merge join of the posting lists (match if we find two postings
				// of the same transactions such that the starting position of the right
				// sequence is close enough to the starting position of the left
				// sequence (at most gamma items in between)
				for (int j = 0; j < rightSequences.size(); j++) {
					// initialize
					postingList.clear();
					int totalSupport = 0; // of the current posting list
					leftPostingList.offset = 0;
					rightPostingList.postingList = kPostingLists.get(rightSequences.get(j));
					rightPostingList.offset = 0;
					int leftTransactionId = leftPostingList.nextValue();
					int rightTransactionId = rightPostingList.nextValue();
					boolean foundMatchWithLeftTransactionId = false;

					while (leftPostingList.hasNextValue() && rightPostingList.hasNextValue()) {
						// invariant: leftPostingList and rightPostingList point to first
						// position after
						// a transaction id

						if (leftTransactionId == rightTransactionId) {
							// potential match; now check offsets
							int transactionId = leftTransactionId;
							int rightPosition = -1;
							while (leftPostingList.hasNextValue()) {
								int leftPosition = leftPostingList.nextValue();

								// fast forward right cursor (merge join; positions are sorted)
								while (rightPosition <= leftPosition && rightPostingList.hasNextValue()) {
									rightPosition = rightPostingList.nextValue();
								}
								if (rightPosition <= leftPosition)
									break;

								// check whether join condition is met
								if (rightPosition <= leftPosition + gamma + 1) {
									// yes, add a posting
									if (!foundMatchWithLeftTransactionId) {
										if (postingList.size() > 0) {
											PostingList.addCompressed(0, postingList); // add
																																	// separator
																																	// byte
										}
										PostingList.addCompressed(transactionId + 1, postingList); // add
																																								// transaction
																																								// id
										foundMatchWithLeftTransactionId = true;
										totalSupport += transactionSupports.get(transactionId);
									}
									PostingList.addCompressed(leftPosition + 1, postingList); // add
																																						// position
								}
							}

							// advance both join lists
							if (rightPostingList.nextPosting()) {
								rightTransactionId = rightPostingList.nextValue();
							}
							if (leftPostingList.nextPosting()) {
								leftTransactionId = leftPostingList.nextValue();
								foundMatchWithLeftTransactionId = false;
							}
							// end leftTransactionId == rightTransactionId
						} else if (leftTransactionId > rightTransactionId) {
							// advance right join list (merge join; lists sorted by
							// transaction id)
							if (rightPostingList.nextPosting()) {
								rightTransactionId = rightPostingList.nextValue();
							}
						} else {
							// advance left join (merge join; lists sorted by transaction id)
							if (leftPostingList.nextPosting()) {
								leftTransactionId = leftPostingList.nextValue();
								foundMatchWithLeftTransactionId = false;
							}
						}
					}

					// if the new (k+1)-sequence has support equal or above minimum
					// support,
					// add it to the result of this round
					if (totalSupport >= sigma) {
						noK1SequencesForLeftSequence++;
						int suffixItem = this.kSequences.get(rightSequences.get(j))[this.kSequences.get(rightSequences.get(j)).length - 1];
						int[] kSequence; // holds result

						if (noK1SequencesForLeftSequence == 1) {
							// uncompressed output
							pointerToFirstK1Sequence = k1Sequences.size();

							// construct whole (k+1)-sequence
							kSequence = new int[k1];
							int[] prefix = kSequences.get(leftSequences.get(i));
							if (prefix.length == k1 - 1 || k1 <= 3) { // prefix sequence is
																												// uncompressed
								System.arraycopy(prefix, 0, kSequence, 0, prefix.length);
							} else { // prefix sequence is compressed (only suffix item
												// stored)
								// need to retrieve prefix from initial sequence
								int prefixPos = prefix[0];
								int[] tempPrefix = kSequences.get(prefixPos);
								System.arraycopy(tempPrefix, 0, kSequence, 0, tempPrefix.length - 1);
								kSequence[k1 - 2] = prefix[1];
							}
							kSequence[k1 - 1] = suffixItem;
						} else {
							// include only the suffix item of (k+1)-sequence (first k items
							// same as
							// the ones at index pointerToPrefixSequence)
							kSequence = new int[2];
							kSequence[0] = pointerToFirstK1Sequence;
							kSequence[1] = suffixItem;
						}

						// store in results of current round
						k1Sequences.add(kSequence);
						ByteArrayList temp = new ByteArrayList(postingList.size()); // copying
																																					// necessary
																																					// here;
																																					// postingList
																																					// reused
						for (int k = 0; k < postingList.size(); k++) {
							temp.add(postingList.get(k)); // bad API here; newer Trove
																						// versions support this directly
						}
						k1PostingLists.add(temp);
						k1TotalSupports.add(totalSupport);
					}
				} // for all right sequences of the same key
			} // for all left sequences of each left key
		} // for all left keys

		// we are done; store output
		k = k1;
		this.kSequences = k1Sequences;
		this.kPostingLists = k1PostingLists;
		this.kTotalSupports = k1TotalSupports;
	}

	/** Builds a prefix/suffix index from the currently stored k-sequences */
	void buildPrefixSuffixIndex(Map<IntArrayList, IntArrayList> sequencesWithPrefix,
			Map<IntArrayList, IntArrayList> sequencesWithSuffix) {
		int k1 = k + 1;

		// scan over all k-sequences and build prefix/suffix index
		IntArrayList suffix = null;
		IntArrayList prefix = null;
		for (int index = 0; index < kSequences.size(); index++) {
			int[] sequence = kSequences.get(index);

			// construct prefix (last item of sequence omitted) and suffix (first item
			// omitted)
			if (sequence.length == 2 && k1 > 3) {
				// compressed sequence
				// only suffix in sequence, need to construct left key
				suffix = new IntArrayList(k - 1); // TODO: inefficient
				for (int j = 1; j < prefix.size(); j++) {
					suffix.add(prefix.get(j));
				}
				suffix.add(sequence[1]);
				// right key remains unchanged
			} else {
				// uncompressed sequence
				prefix = new IntArrayList(k - 1);
				for (int j = 0; j < k - 1; j++) {
					prefix.add(sequence[j]);
				}

				suffix = new IntArrayList(k - 1);
				for (int j = 1; j < k; j++) {
					suffix.add(sequence[j]);
				}
			}

			// update list of sequences starting with the prefix
			IntArrayList sequences = sequencesWithPrefix.get(prefix);
			if (sequences == null) {
				sequences = new IntArrayList();
				sequencesWithPrefix.put(prefix, sequences);
			}
			sequences.add(index);

			// update list of sequences ending with suffix
			sequences = sequencesWithSuffix.get(suffix);
			if (sequences == null) {
				sequences = new IntArrayList();
				sequencesWithSuffix.put(suffix, sequences);
			}
			sequences.add(index);
		}
	}

}
