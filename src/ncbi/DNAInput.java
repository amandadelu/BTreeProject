package ncbi;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.Scanner;
import bterrors.DNASequenceNotFound;
import bterrors.DNAWrongSequenceLength;

public class DNAInput {
private BufferedReader buff;
//private Scanner strScan;
//private IntStream strChars;
private char strQ[];
private int strQpos;
private DNASequence seqBuilder;
private boolean nextFound, hasData;

public DNAInput(String fname, int seqlen) throws IOException, DNAWrongSequenceLength, InterruptedException {
	
	seqBuilder = new DNASequence(seqlen);
	buff = new BufferedReader(new FileReader(fname));
	//strScan = new Scanner("");
	//strChars = "".chars();
	//strQ = new LinkedList<Character>();
	strQ = new char[0];
	strQpos = 0;
	hasData = runtoOrigin();
	nextFound = false;
	moveToNext();
}

public boolean hasNext() {
	return nextFound;
}

public long Next() throws DNAWrongSequenceLength, DNASequenceNotFound, IOException, InterruptedException {
	if (nextFound) {
	   long ret = seqBuilder.getDNAKey();
	   moveToNext();
	   return ret;
	} else throw new DNASequenceNotFound();
}

private boolean runtoOrigin() throws IOException {
	
	String tmp="";
	while (!tmp.equals("ORIGIN")) {
		tmp = buff.readLine();
		if (tmp!=null) {
			tmp = tmp.trim();
		} else {
			break;
		}
	}
	//if (tmp!=null) System.out.println(tmp);
	return tmp!=null;
}

private boolean moveToNext() throws IOException, InterruptedException, DNAWrongSequenceLength {
	if (hasData) {
		nextFound = false;
		do {
			if (strQpos >= strQ.length) {
				String tmp = buff.readLine();
				if (tmp==null) {
					hasData = false;
					return false;
				} else if (tmp.trim().equals("//")) {
					seqBuilder.reset();
					hasData = runtoOrigin();
					continue;
				} else {
					Scanner tmpScan = new Scanner(tmp.trim());
					System.err.println(tmpScan.next());
					//strChars = tmpScan.nextLine().trim().chars();
					strQpos = 0;
					strQ = tmpScan.nextLine().toCharArray();
				}
			} else {
				char c = strQ[strQpos++];
				System.out.println(c);
				switch (c) {
				case 'n':
				case 'N':
					seqBuilder.reset();
					continue;
				case 'A':
				case 'a':
				case 'T':
				case 't':
				case 'C':
				case 'c':
				case 'G':
				case 'g':
					seqBuilder.pushCodon(c);
					if (seqBuilder.isComplete()) {
						nextFound = true;
						return true;
					}
				default:
					continue;
				}
			}
		} while(hasData);
	} 
	return false;
	

}

}
