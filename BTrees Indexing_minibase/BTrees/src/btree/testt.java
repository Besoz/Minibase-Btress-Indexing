package btree;

public class testt {

	/**
	 * @param args
	 */
	public static void main(String[] args) {
		// TODO Auto-generated method stub
		String cString = new String ("String c");

		String aString = new String("String a");
		String bString = aString;
		System.out.println(aString+" "+bString+" "+cString);
		bString=cString;
		System.out.println(aString+" "+bString+" "+cString);
	
	}

}
