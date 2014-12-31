package btree;

import java.io.IOException;

import diskmgr.Page;
import global.PageId;
import global.RID;
import heap.InvalidSlotNumberException;
import heap.Tuple;

public class BTIndexPage extends BTSortedPage {
	// pointers

	// keys
	public BTIndexPage(int arg0) throws ConstructPageException, IOException {
		super(arg0);
		this.setType(NodeType.INDEX);
		// TODO Auto-generated constructor stub
	}

	public BTIndexPage(Page arg0, int arg1) throws IOException {
		super(arg0, arg1);
		this.setType(NodeType.INDEX);
		// TODO Auto-generated constructor stub
	}

	public BTIndexPage(PageId arg0, int arg1) throws ConstructPageException,
			IOException {
		super(arg0, arg1);
		this.setType(NodeType.INDEX);
		// TODO Auto-generated constructor stub
	}

	public PageId getLeftLink() throws IOException {
		return this.getPrevPage();
	}

	public void setLeftLink(PageId p) throws IOException {
		this.setPrevPage(p);
	}

	public PageId getPageNoByKey(KeyClass key) throws IOException,
			KeyNotMatchException, NodeNotMatchException, ConvertException,
			InvalidSlotNumberException {
		RID r = this.firstRecord();
		KeyDataEntry kdt = BT.getEntryFromBytes(this.getRecord(r)
				.getTupleByteArray(), this.getRecord(r).getOffset(), this
				.getRecord(r).getTupleByteArray().length, keyType,
				NodeType.INDEX);
		KeyClass k = kdt.key;
		boolean firstRec = false;
		while (BT.keyCompare(key, k) >= 0) {
			r = this.nextRecord(r);
			KeyDataEntry kdt2=null;
			if(r==null)
				return ((IndexData)kdt.data).getData();
			else {
				 kdt2 = BT.getEntryFromBytes(this.getRecord(r).getTupleByteArray(),
						this.getRecord(r).getOffset(), this.getRecord(r)
						.getTupleByteArray().length, keyType,
				NodeType.INDEX);
			}
			if(BT.keyCompare(kdt2.key, key) > 0)
				return ((IndexData)kdt.data).getData();

			kdt = BT.getEntryFromBytes(this.getRecord(r).getTupleByteArray(),
					this.getRecord(r).getOffset(), this.getRecord(r)
							.getTupleByteArray().length, keyType,
					NodeType.INDEX);
			
			k = kdt.key;
			firstRec = true;
		}
		if (!firstRec) {
			return this.getLeftLink();
		} else {
			return ((IndexData) kdt.data).getData();
		}
	}
	public KeyDataEntry getFirst(RID rid) throws IOException, InvalidSlotNumberException, KeyNotMatchException, NodeNotMatchException, ConvertException{
		RID currentRID = this.firstRecord();
		if(currentRID==null)
			return null;
		rid.slotNo = currentRID.slotNo;
		rid.pageNo.pid = currentRID.pageNo.pid;
		Tuple t=this.getRecord(rid);
		return BT.getEntryFromBytes(t.getTupleByteArray(), t.getOffset(), t.getLength(), keyType, NodeType.INDEX);
		
	}
	public KeyDataEntry getNext(RID rid) throws IOException, InvalidSlotNumberException, KeyNotMatchException, NodeNotMatchException, ConvertException{
		
		RID currentRID = this.nextRecord(rid);
		if(currentRID==null)
			return null;
		rid.pageNo.pid= currentRID.pageNo.pid;
		rid.slotNo=currentRID.slotNo;
		Tuple t=this.getRecord(rid);
		return BT.getEntryFromBytes(t.getTupleByteArray(), t.getOffset(), t.getLength(), keyType, NodeType.INDEX);
		
	}
	public RID insertKey(KeyClass key, PageId pageNo) throws InsertRecException
	{
		KeyDataEntry kdt = new KeyDataEntry(key, pageNo);
		return this.insertRecord(kdt);
		
	}

}
