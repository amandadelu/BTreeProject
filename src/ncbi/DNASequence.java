package ncbi;

import bterrors.DNABadCode;
import bterrors.DNAWrongSequenceLength;
/*
 * class that builds a long key letter by letter from a DNA
 * it accumulates the sequence and every new letter updates the sequence pushing
 * the oldest letter out
 * 
 */
public class DNASequence {
	/*
	 * takes a character and returns two bits corresponding to the letter
	 * regardless of case
	 */
	public static long Let2Bin(char l) throws DNABadCode {
		switch(l) {
		case 'A':
		case 'a': return 0b00;
		case 'T':
		case 't': return 0b11;
		case 'C':
		case 'c': return 0b01;
		case 'G':
		case 'g': return 0b10;
		default: throw new DNABadCode();
		}
	}
	//takes a long number but only looks at the two lowest bits
	//returns a letter corresponding to the two lowest bits

	public static char Bin2Let(long b) {
		switch((short)b & 0b11) {
		case 0b00: return 'a';
		case 0b11: return 't';
		case 0b01: return 'c';
		case 0b10: return 'g';
		}
		return 0;
	}
	
	private long acc, leftmost; //sequence accumulator, bit position for new letter
	private int seqlen, needlen; //length of sequence, remaining num letters still needed
	//getter for accumulator value of the sequence
	//enforces that sequence needs to be complete
	public long getDNAKey() throws DNAWrongSequenceLength {
		if (needlen > 0) throw new DNAWrongSequenceLength();
		return acc;
	}
	//builds the DNA string that represents the sequence of a given length
	//corresponding to given key
	public static String getDNAString(int seqlen, long dnakey) {
		char[] buf = new char[seqlen];
		for (int i=0; i<seqlen; i++) {
			buf[i] = Bin2Let(dnakey);
			dnakey >>= 2;
		}
		return new String(buf);
	}
	//function checking if DNA sequence is complete in length
	public boolean isComplete() {
		return needlen == 0;
	}
	//stores sequence length
	//if length is wrong then throw exception
	public DNASequence(int seqlen) throws DNAWrongSequenceLength {
		if (seqlen < 1 || seqlen > 31) throw new DNAWrongSequenceLength();
		this.seqlen = seqlen;
		//Alternative, (seqlen - 1) * 2, by how many bits to move 
		//to insert the new data
		leftmost = (seqlen - 1) << 1; 
		reset(); //reset accumulators
	}
	//function that resets accumulators
	public void reset() {
		acc = 0;
		needlen = seqlen;
	}
	
	public void pushCodon(char c) throws DNAWrongSequenceLength {
		try {
			
			//Move the accumulater 2 bits right
			//discard the rightmost 2 bits and set the leftmost two bits with new data
			//  // (acc / 4) + (Let2Bin(c) * 2^leftmost)
			acc = (acc >> 2) | (Let2Bin(c) << leftmost);
			switch(needlen) {
			case 0:
				break;
			case -1:
				throw new DNAWrongSequenceLength();
			default:
				needlen -= 1;
				break;
			}
		} catch(DNABadCode e) {
			reset(); //if something goes wrong then reset sequence and start from beginning
		}
	}
}
