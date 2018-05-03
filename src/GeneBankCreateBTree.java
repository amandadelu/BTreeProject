import java.io.IOException;

import bterrors.BTreeBadMetadata;
import bterrors.BTreeFullNode;
import bterrors.BTreeNoInternalNodeChild;
import bterrors.BTreeNonExactNonLeaf;
import bterrors.BTreeNotFullNode;
import bterrors.BTreeWrongBlockID;
import bterrors.BTreeWrongKeyOrder;
import bterrors.DNASequenceNotFound;
import bterrors.DNAWrongSequenceLength;
import btree.BTree;
import btree.BTree.NearestSearchResult;
import btree.BTreeObject;
import btree.DebugPrint;
import ncbi.DNAInput;
/**
 * 
 * @author amandadelu, pjcory, joshsanders
 *
 */
public class GeneBankCreateBTree {
    public static void usage() {
    	System.out.println("java GeneBankCreateBTree <0/1(no/with Cache)> <degree> <gbk file>"
    			+ " <sequence length> <cache size> [<debug level>]");
    }
	public static void main(String[] args) throws IOException, BTreeBadMetadata, BTreeWrongBlockID, DNAWrongSequenceLength, InterruptedException, DNASequenceNotFound, BTreeNonExactNonLeaf, BTreeNotFullNode, BTreeWrongKeyOrder, BTreeNoInternalNodeChild, BTreeFullNode {
		boolean usecache;
		int degree;
		String fname;
		int seqlen;
		int cachesize;

		if (args.length< 5 || args.length > 6) {
			usage();
			return;
		}
		try {
			switch(Integer.parseInt(args[0])) {
			case 0: usecache=false; break;
			case 1: usecache=true; break;
			default: throw new IllegalArgumentException();
			}
			degree = Integer.parseInt(args[1]);
			//if (degree < 2) throw new IllegalArgumentException();
			fname = args[2];
			seqlen = Integer.parseInt(args[3]);
			if (seqlen < 1 || seqlen > 31) throw new IllegalArgumentException();
			cachesize = Integer.parseInt(args[4]);
			if (usecache) {
				  if(cachesize < 1)
					  throw new IllegalArgumentException();
			} else cachesize=0;
			
			if (args.length == 6) {
				DebugPrint.debuglevel = Integer.parseInt(args[5]);
				if (DebugPrint.debuglevel < -1 || DebugPrint.debuglevel > 1) {
					throw new IllegalArgumentException();
				}
			}
			
		} catch(Exception e) {
			usage();
			return;
		}
		
		BTree dnatree;
		String treefname = fname + ".btree.data."+seqlen;
		if (degree>=2) {
			dnatree = new BTree(treefname, degree, cachesize);
		} else {
			dnatree = new BTree(treefname, false, true, cachesize);
		}
		DNAInput dnaparser = new DNAInput(fname, seqlen);
		long start = System.currentTimeMillis();
		while (dnaparser.hasNext()) {
			long dnakey = dnaparser.Next();
			BTreeObject newkey = new BTreeObject(dnakey);
			NearestSearchResult check = dnatree.lookup(newkey);
			if (check.exact) {
				check.foundkey.IncCounter();
				check.saveNode(); 
				//need to save node explicitly incase we do not use cache
				if (DebugPrint.debuglevel>0) {
					DebugPrint.message(String.format("Incrementing key %d to %d count", check.foundkey.getKey(), check.foundkey.getCounter()));
				}
			} else {
				newkey.IncCounter();
				//newkey.IncCounter();
				dnatree.insertToFoundLoc(check);
				if (DebugPrint.debuglevel>0) {
					DebugPrint.message(String.format("Inserting key %d", newkey.getKey()));
				}
			}
			
		}
		if (DebugPrint.debuglevel>=0) {
			if (DebugPrint.debuglevel>0) {
				dnatree.dump(seqlen, "dump");
				
			}
			DebugPrint.message("Finished in " + (System.currentTimeMillis()-start) + " ms");
		}
		dnatree.shutdown();
		
		

	}

}
