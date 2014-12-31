package tests;

import java.io.IOException;

import diskmgr.Page;

import btree.BTLeafPage;
import btree.BTSortedPage;
import btree.ConstructPageException;

import global.Convert;
import global.PageId;

public class Main {
public static void main(String[] args) throws IOException, ConstructPageException {
	int n = 77234234;
	byte[] arr = new byte[4];
	Convert.setIntValue(n, 0, arr);
	System.out.println(Convert.getIntValue(0, arr));
	
	BTLeafPage btSortedPage = new BTLeafPage(4);
	btSortedPage.init(new PageId(5), new Page());
}
}
