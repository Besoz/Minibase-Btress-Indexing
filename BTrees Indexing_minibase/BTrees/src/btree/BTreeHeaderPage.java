package btree;

import java.io.IOException;

import bufmgr.BufMgrException;
import bufmgr.BufferPoolExceededException;
import bufmgr.HashEntryNotFoundException;
import bufmgr.HashOperationException;
import bufmgr.InvalidFrameNumberException;
import bufmgr.PageNotReadException;
import bufmgr.PagePinnedException;
import bufmgr.PageUnpinnedException;
import bufmgr.ReplacerException;
import global.Convert;
import global.GlobalConst;
import global.PageId;
import global.RID;
import global.SystemDefs;
import heap.HFPage;
import heap.InvalidSlotNumberException;
import heap.Tuple;

public class BTreeHeaderPage  extends HFPage {
	BTreeHeaderPage()
	{
		
	}
	BTreeHeaderPage(PageId pid) throws InvalidSlotNumberException, IOException, ReplacerException, HashOperationException, PageUnpinnedException, InvalidFrameNumberException, PageNotReadException, BufferPoolExceededException, PagePinnedException, BufMgrException, HashEntryNotFoundException
	{
		this.init(pid, this);
		SystemDefs.JavabaseBM.pinPage(pid, this, false);
		RID rid = this.firstRecord();
		keytype = (short) convertToInt(this.getRecord(rid).getTupleByteArray());
		rid = this.nextRecord(rid);
		keysize = convertToInt(this.getRecord(rid).getTupleByteArray());
		rid = this.nextRecord(rid);
		rootID = convertToInt(this.getRecord(rid).getTupleByteArray());
		rid = this.nextRecord(rid);
		leafPages=convertToInt(this.getRecord(rid).getTupleByteArray());
		rid = this.nextRecord(rid);
		indexPages=convertToInt(this.getRecord(rid).getTupleByteArray());
		SystemDefs.JavabaseBM.unpinPage(pid,false);

	}
	public short get_keyType() {
		return keytype;
	}

	public void setKeytype(short keytype) throws IOException, InvalidSlotNumberException {
		Tuple old = this.returnRecord(this.firstRecord());
//		Tuple t = new Tuple (convertToByteArr((int)keytype),old.getOffset(),old.getLength());
//		old.tupleInit(t.returnTupleByteArray(),t.getOffset(),t.getLength());
		byte arr[] = old.getTupleByteArray();
		arr = convertToByteArr(keytype);
		int n = convertToInt(arr);
		for(int i = 0 ; i < 5 ; i ++)
			this.deleteRecord(this.firstRecord());
		this.insertRecord(arr);
		this.insertRecord(convertToByteArr(keysize));
		this.insertRecord(convertToByteArr(rootID));
		this.insertRecord(convertToByteArr(leafPages));
		this.insertRecord(convertToByteArr(indexPages));
		this.keytype=keytype;
	}

	public int getKeysize() {
		return keysize;
	}
	
	public void setKeysize(int keysize) throws IOException, InvalidSlotNumberException {
		RID rid = new RID();
		rid = this.firstRecord();
		rid = this.nextRecord(rid);
		Tuple old = this.returnRecord(rid);
//		Tuple t = new Tuple (convertToByteArr((int)keytype),old.getOffset(),old.getLength());
//		old.tupleInit(t.returnTupleByteArray(),t.getOffset(),t.getLength());
		byte arr[] = old.getTupleByteArray();
		arr = convertToByteArr(keysize);
		int n = convertToInt(arr);
		for(int i = 0 ; i < 5 ; i ++)
			this.deleteRecord(this.firstRecord());
		this.insertRecord(convertToByteArr(keytype));
		this.insertRecord(arr);
		this.insertRecord(convertToByteArr(rootID));
		this.insertRecord(convertToByteArr(leafPages));
		this.insertRecord(convertToByteArr(indexPages));
		this.keysize=keysize;
	}

	public PageId get_rootId() {
		return new PageId(rootID);
	}

	public void setRootID(int rootID) throws IOException, InvalidSlotNumberException {
		RID rid = new RID();
		rid = this.firstRecord();
		rid = this.nextRecord(rid);
		rid = this.nextRecord(rid);
		Tuple old = this.returnRecord(rid);
//		Tuple t = new Tuple (convertToByteArr((int)keytype),old.getOffset(),old.getLength());
//		old.tupleInit(t.returnTupleByteArray(),t.getOffset(),t.getLength());
		byte arr[] = old.getTupleByteArray();
		arr = convertToByteArr(rootID);
		int n = convertToInt(arr);
		for(int i = 0 ; i < 5 ; i ++)
			this.deleteRecord(this.firstRecord());
		this.insertRecord(convertToByteArr(keytype));
		this.insertRecord(convertToByteArr(keysize));
		this.insertRecord(arr);
		this.insertRecord(convertToByteArr(leafPages));
		this.insertRecord(convertToByteArr(indexPages));
		this.rootID = rootID;
	}

	int keysize,rootID;
	short keytype;
	int leafPages;
	int indexPages;
	 void setLeafPages(int l) throws IOException, InvalidSlotNumberException{
		 	RID rid = new RID();
			rid = this.firstRecord();
			rid = this.nextRecord(rid);
			rid = this.nextRecord(rid);
			rid = this.nextRecord(rid);
			Tuple old = this.returnRecord(rid);
//			Tuple t = new Tuple (convertToByteArr((int)keytype),old.getOffset(),old.getLength());				
//			old.tupleInit(t.returnTupleByteArray(),t.getOffset(),t.getLength());
			byte arr[] = old.getTupleByteArray();
			arr = convertToByteArr(l);
			int n = convertToInt(arr);
			for(int i = 0 ; i < 5 ; i ++)
				this.deleteRecord(this.firstRecord());
			this.insertRecord(convertToByteArr(keytype));
			this.insertRecord(convertToByteArr(keysize));
			this.insertRecord(convertToByteArr(rootID));
			this.insertRecord(arr);
			this.insertRecord(convertToByteArr(indexPages));
			
			this.leafPages = l;
	}
	 void setIndesPages(int j) throws IOException, InvalidSlotNumberException
	 {
			RID rid = new RID();
			rid = this.firstRecord();
			rid = this.nextRecord(rid);
			rid = this.nextRecord(rid);
			rid = this.nextRecord(rid);
			rid = this.nextRecord(rid);
			Tuple old = this.returnRecord(rid);
//			Tuple t = new Tuple (convertToByteArr((int)keytype),old.getOffset(),old.getLength());
//			old.tupleInit(t.returnTupleByteArray(),t.getOffset(),t.getLength());
			byte arr[] = old.getTupleByteArray();
			arr = convertToByteArr(j);
			int n = convertToInt(arr);
			for(int i = 0 ; i < 5 ; i ++)
				this.deleteRecord(this.firstRecord());
			this.insertRecord(convertToByteArr(keytype));
			this.insertRecord(convertToByteArr(keysize));
			this.insertRecord(convertToByteArr(rootID));
			this.insertRecord(convertToByteArr(leafPages));
			this.insertRecord(arr);
			this.indexPages = j;
		 
	 }
	public byte[] convertToByteArr(int n) throws IOException
	{
		byte[] arr = new byte[4];
		Convert.setIntValue(n, 0, arr);
		return arr;
	}
	public int convertToInt(byte[] arr) throws IOException
	{
		return Convert.getIntValue(0, arr);
	}
	public void initiate() throws IOException
	{
		this.insertRecord(convertToByteArr(-1));
		this.insertRecord(convertToByteArr(-1));
		this.insertRecord(convertToByteArr(-1));
		this.insertRecord(convertToByteArr(-1));
		this.insertRecord(convertToByteArr(-1));
	}
}
