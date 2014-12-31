package btree;

import global.PageId;
import global.RID;
import heap.InvalidSlotNumberException;

import java.io.IOException;

public class BTFileScan extends IndexFileScan {
	BTLeafPage currentLeaf;
	KeyDataEntry kdt;
	RID rid;
	int keytype;
	KeyClass low;
	KeyClass high;
    int keysize;
	public BTFileScan(BTSortedPage root, int keytype,KeyClass low, KeyClass high,int keysize) throws IOException, ConstructPageException
	{
		this.keysize = keysize;
		this.low=low;
		this.high=high;
		rid = new RID();
		this.keytype = keytype;
		if(root.getType()==NodeType.LEAF)
		{
			root = new BTLeafPage(root,keytype);
			currentLeaf=(BTLeafPage)root;
						
		}
		else
		{
			
			root = new BTIndexPage(root,keytype);
			BTSortedPage temp = new BTSortedPage(((BTIndexPage)root).getLeftLink(),keytype);
			while(temp.getType()!=NodeType.LEAF)
			{
				temp = new BTIndexPage(temp,keytype);
				temp = new BTSortedPage(((BTIndexPage)temp).getLeftLink(),keytype);
			}
			temp = new BTLeafPage(temp,keytype);
			currentLeaf = (BTLeafPage)temp;
			
		}
		
	}
	public KeyDataEntry get_next() {
		try{
			
			if(kdt == null )
			{
			
				  kdt = goToStart(kdt);
				  return kdt;
			}
			else{
				if(endNotYet(kdt))
				kdt = currentLeaf.getNext(rid);
				else return null;
				if(kdt == null)
				{
					PageId pid =currentLeaf.getNextPage();
					if(pid.pid==-1)
					{
						return null;
					}
					else 
					{
						currentLeaf = new BTLeafPage(pid,keytype);
						kdt = currentLeaf.getFirst(rid);
						return kdt;
					}
				}
				else return kdt;
				
			}
		}catch(Exception e)
		{
			return null;
		}
			
	}

	
	private boolean endNotYet(KeyDataEntry kdt2) throws KeyNotMatchException {
		if(high ==null)
			return true;
		else{
			if(BT.keyCompare(high, kdt2.key)>0)
				return true;
			else return false;
		}
		
	}
	private KeyDataEntry goToStart(KeyDataEntry kdt) throws InvalidSlotNumberException, KeyNotMatchException, NodeNotMatchException, ConvertException, IOException, ConstructPageException {
		kdt= currentLeaf.getFirst(rid);
		if(low ==null)
			return currentLeaf.getFirst(rid);
		else{
			PageId pid = currentLeaf.getCurPage();
			while(pid.pid!=-1)
			{
				if(BT.keyCompare(kdt.key, low)<0)
				{
					kdt = currentLeaf.getNext(rid);
				}
				else return kdt;
				if(kdt==null)
				{
					pid = currentLeaf.getNextPage();
					if(pid.pid!=-1)
						{currentLeaf = new BTLeafPage(pid,keytype);
						 kdt = currentLeaf.getFirst(rid);
						}
				}
				
			}
			return null;
		}
	}
	
	public void delete_current() {
		if(kdt!=null)
		currentLeaf.delEntry(kdt);
	}

	public int keysize() {
		return keysize;
	}
	
	
	
	
	

}
