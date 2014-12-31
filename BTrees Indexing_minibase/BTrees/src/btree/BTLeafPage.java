package btree;

import java.io.IOException;

import global.PageId;
import global.RID;
import heap.HFPage;
import heap.InvalidSlotNumberException;
import heap.Tuple;
import diskmgr.Page;

public class BTLeafPage extends BTSortedPage {
	public BTLeafPage(int arg0) throws ConstructPageException, IOException {
		super(arg0);
		this.setType(NodeType.LEAF);
		

		// TODO Auto-generated constructor stub
	}


	public BTLeafPage(Page arg0, int arg1) throws IOException {
		super(arg0, arg1);
		this.setType(NodeType.LEAF);

		// TODO Auto-generated constructor stub
	}
	
	public BTLeafPage(PageId arg0, int arg1) throws ConstructPageException, IOException {
		super(arg0, arg1);
		this.setType(NodeType.LEAF);

		// TODO Auto-generated constructor stub
	}
	public boolean delEntry(KeyDataEntry entry)
	{
		
		try
		{
			RID rid = new RID();
			this.getFirst(rid);
			while(this.getCurrent(rid)!=null)
			{
				if(((LeafData)this.getCurrent(rid).data).getData().slotNo == ((LeafData)entry.data).getData().slotNo
						&& ((LeafData)this.getCurrent(rid).data).getData().pageNo.pid == ((LeafData)entry.data).getData().pageNo.pid)
				{
					this.deleteSortedRecord(rid);
					return true;
				}
				this.getNext(rid);
			}
			
			return false;
		}catch(Exception e)
		{
			return false;
		}
	}
	public KeyDataEntry getNext(RID rid) throws IOException, InvalidSlotNumberException, KeyNotMatchException, NodeNotMatchException, ConvertException
	{
		RID currentRID = this.nextRecord(rid);
		if(currentRID==null)
			return null;
		rid.slotNo = currentRID.slotNo;
		rid.pageNo.pid= currentRID.pageNo.pid;
		Tuple t = this.getRecord(rid);
		return BT.getEntryFromBytes(t.getTupleByteArray(), t.getOffset(), t.getLength(), keyType, NodeType.LEAF);
		
	}
	public KeyDataEntry getFirst(RID rid) throws IOException, InvalidSlotNumberException, KeyNotMatchException, NodeNotMatchException, ConvertException
	{
		RID currentRID = this.firstRecord();
		if(currentRID==null)
			return null;
		rid.slotNo=currentRID.slotNo;
		rid.pageNo.pid = currentRID.pageNo.pid;
		Tuple t=this.getRecord(currentRID);
		return BT.getEntryFromBytes(t.getTupleByteArray(), t.getOffset(), t.getLength(), keyType, NodeType.LEAF);
		
	}
	public RID insertRecord(KeyClass key,RID rid) throws InsertRecException
	{
		KeyDataEntry kdt = new KeyDataEntry(key, rid);
		return ((BTSortedPage)this).insertRecord(kdt);
		
		
	}
	public KeyDataEntry getCurrent(RID rid) throws InvalidSlotNumberException, IOException, KeyNotMatchException, NodeNotMatchException, ConvertException
	{
		
		Tuple t = this.getRecord(rid);
		return BT.getEntryFromBytes(t.getTupleByteArray(), t.getOffset(), t.getLength(), keyType, NodeType.LEAF);
		
	}
	
}
