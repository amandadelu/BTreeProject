package ncbi;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.Scanner;
import bterrors.DNASequenceNotFound;
import bterrors.DNAWrongSequenceLength;
/**
 * Reading the ncbi file starting from ORIGIN going line by line and character by
 * character in each line. It will use the character DNASequence instance to accumulate
 * the DNA sequence
 * @author amandadelu, pjcory, joshsanders
 *
 */
public class DNAInput {
private BufferedReader buff; //class to read file
private char strQ[]; //array to hold characters from each line
private int strQpos; //current position in that line
private DNASequence seqBuilder; //sequence builder
private boolean nextFound, hasData; //next sequence is ready, remaining data at all
/*
 * constructor opens file under that filename
 */
public DNAInput(String fname, int seqlen) throws IOException, DNAWrongSequenceLength, InterruptedException {
	
	seqBuilder = new DNASequence(seqlen); //initialize sequence builder for desired length
	buff = new BufferedReader(new FileReader(fname)); //open the ncbi file
	strQ = new char[0]; //initialize the empty current string
	strQpos = 0; //position to zero
	hasData = runtoOrigin(); //scroll to read file till ORIGIN found
	nextFound = false; //flag, next sequence is not confirmed yet
	moveToNext(); //try to obtain next sequence
}
//reader for the flag, is next sequence ready
public boolean hasNext() {
	return nextFound;
}
//returns the next sequence and moves to the sequence after next if possible
public long Next() throws DNAWrongSequenceLength, DNASequenceNotFound, IOException, InterruptedException {
	if (nextFound) { 
	   long ret = seqBuilder.getDNAKey();
	   moveToNext(); //trys to obtain next sequence if possible
	   return ret; 
	} else throw new DNASequenceNotFound();
}
//keep reading the file until you encounter the line that contains the word ORIGIN
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
	return tmp!=null; //return true if ORIGIN is located
}

private boolean moveToNext() throws IOException, InterruptedException, DNAWrongSequenceLength {
	if (hasData) { //act only if you have data
		nextFound = false; //assume you have not found it 
		do { //try to build next sequence while file still has data
			if (strQpos >= strQ.length) { //current line empty 
				String tmp = buff.readLine();//try to read next line
				if (tmp==null) { //if not successful than flag and breakout
					hasData = false; 
					return false;
				} else if (tmp.trim().equals("//")) { //if line indicates end of block
					seqBuilder.reset();
					hasData = runtoOrigin(); //reset and try to find next ORIGIN
					continue;
				} else { //line read from file succesfully
					Scanner tmpScan = new Scanner(tmp.trim()); 
					int lineno = tmpScan.nextInt(); //reads line number from line
					strQpos = 0; //set current position to zero
					strQ = tmpScan.nextLine().toCharArray(); //split line into strQ array
				}
			} else { //still have data in current line represented by strQ
				char c = strQ[strQpos++]; //get next character from strQ and advance current position
				switch (c) {
				case 'n':
				case 'N':
					seqBuilder.reset(); //reset if encouter 'N' or 'n'
					continue;
				case 'A':
				case 'a':
				case 'T':
				case 't':
				case 'C':
				case 'c':
				case 'G':
				case 'g':
					seqBuilder.pushCodon(c); //if successful push to seqBuilder
					if (seqBuilder.isComplete()) { //check if complete
						nextFound = true; //mark as next found and return true
						return true;
					}
				default: //defualt characters ignore
					continue;
				}
			}
		} while(hasData); //try to build next sequence while file still has data
	} 
	return false; //by default return false
	

}

}
