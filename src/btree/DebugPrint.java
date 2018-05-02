package btree;
/*
 * this class is just global placeholder for debug level flag and method to print debug 
 * message from anywhere in the program 
 */
public class DebugPrint {
	
	static public int debuglevel = 0;
	static public void message(String msg) {
		System.err.println(msg);
	}

}
