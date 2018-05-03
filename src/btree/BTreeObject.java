package btree;

import java.nio.LongBuffer;

/**
 * key/counter object to store in the tree
 * 
 * @author amandadelu, pjcory, joshsanders
 *
 */
public class BTreeObject implements Comparable<BTreeObject> {

	private long key;
	private long Counter;
	public static int size = 16; // to indicate the size of data from BTreeObject in binary format, key value 8
									// bytes each
	/*
	 * constructor initialize key set counter to zero
	 */

	public BTreeObject(long key) {
		this.key = key;
		Counter = 0;
	}

	public void IncCounter() {
		Counter++; // increments counter by one
	}

	public long getKey() {
		return key; // getter for key stored there
	}

	public long getCounter() {
		return Counter; // read the counter
	}

	public String toString() {
		return String.format("%s %s", Long.toString(key), Long.toString(Counter));
	}

	// comparison function which compares to objects to see if keys are equal
	public boolean equals(Object o) {
		if (o instanceof BTreeObject)
			return key == ((BTreeObject) o).getKey();
		return false;
	}

	// saves object to the long buffer for binary file later
	public void toBuffer(LongBuffer l) {
		l.put(key);
		l.put(Counter);
	}

	// read from buffer
	public void fromBuffer(LongBuffer l) {
		key = l.get();
		Counter = l.get();
	}

	@Override
	public int compareTo(BTreeObject o) { // compares two long keys
		return Long.compare(key, o.getKey());
		// -1 if current obj key is less, 0 if keys are equal, 1 if current obj key is
		// larger
	}

}
