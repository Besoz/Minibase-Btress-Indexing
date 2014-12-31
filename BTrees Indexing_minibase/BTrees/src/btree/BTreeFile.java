package btree;

import global.PageId;
import global.RID;
import global.SystemDefs;
import heap.HFPage;
import heap.InvalidSlotNumberException;

import java.io.IOException;

import bufmgr.BufMgrException;
import bufmgr.BufferPoolExceededException;
import bufmgr.HashEntryNotFoundException;
import bufmgr.HashOperationException;
import bufmgr.InvalidBufferException;
import bufmgr.InvalidFrameNumberException;
import bufmgr.PageNotReadException;
import bufmgr.PagePinnedException;
import bufmgr.PageUnpinnedException;
import bufmgr.ReplacerException;
import diskmgr.DiskMgrException;
import diskmgr.DuplicateEntryException;
import diskmgr.FileIOException;
import diskmgr.FileNameTooLongException;
import diskmgr.InvalidPageNumberException;
import diskmgr.InvalidRunSizeException;
import diskmgr.OutOfSpaceException;
import diskmgr.Page;

public class BTreeFile extends IndexFile {
	BTreeHeaderPage  header;
	int indexPages;
	int leafPages;
	private RID keytypeID;
	BTSortedPage root;
	String name;
	public BTreeFile(String fname) throws FileIOException, InvalidPageNumberException, DiskMgrException, IOException, ReplacerException, HashOperationException, PageUnpinnedException, InvalidFrameNumberException, PageNotReadException, BufferPoolExceededException, PagePinnedException, BufMgrException, InvalidSlotNumberException, HashEntryNotFoundException
	{
		this.name = fname;
		PageId pid=SystemDefs.JavabaseDB.get_file_entry(fname);
		header = new BTreeHeaderPage (pid);
		SystemDefs.JavabaseBM.pinPage(pid, header, false);
		indexPages = header.indexPages;
		leafPages = header.leafPages;
	}
	public BTreeFile(String fname,int keytype,int keysize,int delete_fashion) throws FileIOException, InvalidPageNumberException, DiskMgrException, IOException, ReplacerException, HashOperationException, PageUnpinnedException, InvalidFrameNumberException, PageNotReadException, BufferPoolExceededException, PagePinnedException, BufMgrException, HashEntryNotFoundException, FileNameTooLongException, InvalidRunSizeException, DuplicateEntryException, OutOfSpaceException, ConstructPageException, InvalidSlotNumberException
	{
		
		this.name = fname;
		PageId pid = SystemDefs.JavabaseDB.get_file_entry(fname);
		
		
		if(pid!=null)
		{
			header = new BTreeHeaderPage(pid);
			SystemDefs.JavabaseBM.pinPage(pid, header, false);
			indexPages = header.indexPages;
			leafPages = header.leafPages;
			
		}
		else
		{
			header = new BTreeHeaderPage();
			Page page=new Page();
			PageId pId2=SystemDefs.JavabaseBM.newPage(page,1);
			PageId headerid = SystemDefs.JavabaseBM.newPage(header,1);
			header.init(headerid, header);
			HFPage hf = new HFPage();
			hf.init(pId2, page);
			root = new BTLeafPage(pId2, keytype);
			root.init(pId2, hf);
			header.initiate();
			header.setKeytype((short)keytype);
			header.setKeysize(keysize);
			header.setRootID(pId2.pid);
			header.setLeafPages(0);
			header.setIndesPages(0);
			SystemDefs.JavabaseDB.add_file_entry(fname, header.getCurPage());
			indexPages=0;
			leafPages=1;
			
		}
		
	}
	
	@Override
	public void insert(KeyClass data, RID rid) {
		if(header == null)
		{
			System.err.println("The File is closed.");
			return;
		}
			
		KeyDataEntry kdt = new KeyDataEntry(data,rid);

		try{
			if(root.getType()==NodeType.LEAF && root.available_space()<BT.getBytesFromEntry(kdt).length)
			{
				splitAndInsertIntoLeafRoot(kdt);
				return;
			}
		insert(root, kdt);
		}catch(Exception e){
			
		}
		
	}

	private void splitAndInsertIntoLeafRoot(KeyDataEntry kdt) throws BufferPoolExceededException, HashOperationException, ReplacerException, HashEntryNotFoundException, InvalidFrameNumberException, PagePinnedException, PageUnpinnedException, PageNotReadException, BufMgrException, DiskMgrException, IOException, ConstructPageException, InvalidSlotNumberException, KeyNotMatchException, NodeNotMatchException, ConvertException, InsertRecException {
		leafPages++;
		indexPages++;
		BTLeafPage p = (BTLeafPage)root;
		BTIndexPage newRoot = new BTIndexPage((int)header.keytype);
		BTLeafPage newLeaf = new BTLeafPage((int)header.keytype);
		BTLeafPage newP = new BTLeafPage((int)header.keytype);
		RID rid = new RID();
		p.getFirst(rid);
		int size = p.numberOfRecords();
		for(int i = 0; i < size; i++)
		{
			if(i<((size+1)/2))
			{
				newP.insertRecord(p.getCurrent(rid));
			}
			else{
				newLeaf.insertRecord(p.getCurrent(rid));
			}
			p.getNext(rid);
		}
		p = newP;
		RID rid2 = new RID();
		if(BT.keyCompare(kdt.key, newLeaf.getFirst(rid2).key)>=0)
			newLeaf.insertRecord(kdt.key,((LeafData) kdt.data).getData());
		else p.insertRecord(kdt.key,((LeafData) kdt.data).getData());
		root = new BTIndexPage(newRoot.getCurPage(),(int)header.keytype);
		header.rootID=newRoot.getCurPage().pid;
		newLeaf.setPrevPage(p.getCurPage());
		p.setNextPage(newLeaf.getCurPage());
		RID rid3= new RID();
		KeyDataEntry newChild = new KeyDataEntry(newLeaf.getFirst(rid3).key, new PageId(newLeaf.getCurPage().pid));
		root.insertRecord(newChild);
		newRoot.setLeftLink(p.getCurPage());
		
	}
	@Override
	public boolean Delete(KeyClass data, RID rid) {
		RID copy = new RID(new PageId(rid.pageNo.pid),rid.slotNo);
		if(header == null)
		{
			System.err.println("The File is closed.");
			return false;
		}
		try{
			if(root.getType()==NodeType.LEAF)
			{
				root = new BTLeafPage(root,header.keytype);
				return ((BTLeafPage)root).delEntry(new KeyDataEntry(data,rid));
				
			}
			else{
				BTSortedPage p = root;
				while(p.getType()!=NodeType.LEAF)
				{
					p = new BTIndexPage(p,header.keytype);
					p=new BTSortedPage(((BTIndexPage)p).getPageNoByKey(data),header.keytype);
					
				}
				p = new BTLeafPage(p,header.keytype);
				BTLeafPage leaf = (BTLeafPage)p;
				RID itRID = new RID();
				leaf.getFirst(itRID);
				KeyDataEntry kdt = leaf.getCurrent(itRID);
				while(kdt!=null)
				{
					if(((LeafData)kdt.data).getData().slotNo==copy.slotNo
							&&((LeafData)leaf.getCurrent(itRID).data).getData().pageNo.pid==copy.pageNo.pid )
					{
						leaf.delEntry(leaf.getCurrent(itRID));
						return true;
					}
					else{
						kdt=leaf.getNext(itRID);
					}
					
					
				}
				return false;
			}
		}
		catch(Exception e)
		{
			return false;
		}
	}
	
	public BTreeHeaderPage  getHeaderPage() {
		return header;
	}
	public void close() {
		try{
			header.setIndesPages(indexPages);
			header.setLeafPages(leafPages);
		SystemDefs.JavabaseBM.unpinPage(header.getCurPage(), true);
		header =null;
		name = null;
		}catch(Exception e){
			
		}
		
	}
	public BTFileScan new_scan(KeyClass lowkey, KeyClass hikey) {
		try{
			if(header == null)
			{
				System.err.println("The File is closed.");
				return null;
			}
		BTFileScan scan = new BTFileScan(root,header.keytype,lowkey,hikey,header.keysize);
		return scan;
		}
		catch(Exception e){
			System.err.println("The File is closed");
		return null;
		}
	}
	public void destroyFile() {
		try{
		SystemDefs.JavabaseDB.delete_file_entry(name);
		
		}catch(Exception e)
		{
			e.printStackTrace();
		}
	}
//	public void destroyFile() {
////		try{
//////		SystemDefs.JavabaseDB.delete_file_entry(name);
////		}catch(Exception e)
////		{
////			e.printStackTrace();
////		}
//		try {
//			if(root.getType() == NodeType.LEAF){
//				try{
//					SystemDefs.JavabaseBM.unpinPage(root.getCurPage(),false);
//				}catch(Exception e){
//					
//				}
//				SystemDefs.JavabaseBM.freePage(root.getCurPage());
////				SystemDefs.JavabaseDB.deallocate_page(root.getCurPage());
//				try{
//					SystemDefs.JavabaseBM.unpinPage(header.getCurPage(),false);
//				}catch(Exception e){
//				}
//				SystemDefs.JavabaseBM.freePage(header.getCurPage());
////				SystemDefs.JavabaseDB.deallocate_page(header.getCurPage());
//			}
//			else{
//				try{
//					SystemDefs.JavabaseBM.unpinPage(header.getCurPage(),false);
//				}catch(Exception e){
//				}
//				SystemDefs.JavabaseBM.freePage(header.getCurPage());
//				root = new BTIndexPage(root, root.keyType);
//				destroyIndexes((BTIndexPage) root,false);
//			}
//		} catch (IOException | InvalidRunSizeException | InvalidPageNumberException | FileIOException | DiskMgrException | ConstructPageException | InvalidSlotNumberException | KeyNotMatchException | NodeNotMatchException | ConvertException | HashEntryNotFoundException | ReplacerException | PageUnpinnedException | InvalidFrameNumberException | IteratorException | InvalidBufferException | HashOperationException | PageNotReadException | BufferPoolExceededException | PagePinnedException | BufMgrException e) {
//			// TODO Auto-generated catch block
//			e.printStackTrace();
//		}
//	}
	public void traceFilename(String string) {
		// TODO Auto-generated method stub
		
	}
	private void destroyIndexes(BTIndexPage root, boolean b) throws IOException, ConstructPageException, InvalidSlotNumberException, KeyNotMatchException, NodeNotMatchException, ConvertException, InvalidRunSizeException, InvalidPageNumberException, FileIOException, DiskMgrException, HashEntryNotFoundException, ReplacerException, PageUnpinnedException, InvalidFrameNumberException, IteratorException, InvalidBufferException, HashOperationException, PageNotReadException, BufferPoolExceededException, PagePinnedException, BufMgrException {
		BTSortedPage firstChildPage=null;
		if (root.getLeftLink() != null){
			firstChildPage = new BTSortedPage(root.getLeftLink(), header.keytype);
		}
		else{
			firstChildPage=null;
		}
		PageId test = new PageId(-1);
		if(firstChildPage.getType() == NodeType.LEAF || root.getLeftLink().pid==test.pid){
//			base case
			try{
				SystemDefs.JavabaseBM.unpinPage(root.getLeftLink(),false);
			}catch(Exception e ){
				
			}			
			SystemDefs.JavabaseBM.freePage(root.getLeftLink());
			root.setLeftLink(new PageId(-1));
//			SystemDefs.JavabaseDB.deallocate_page(root.getLeftLink());
			RID itrRid = new RID();
			KeyDataEntry kdt=root.getFirst(itrRid);
			int numberOfRecoreds = root.numberOfRecords();
			for (int i = 0 ; i < numberOfRecoreds ; i ++) {
//				SystemDefs.JavabaseDB.deallocate_page(((IndexData)kdt.data).getData());
				try{
					SystemDefs.JavabaseBM.unpinPage(((IndexData)kdt.data).getData(),true);
				}catch(Exception e ){
					
				}
				SystemDefs.JavabaseBM.freePage(((IndexData)kdt.data).getData());
				((IndexData)kdt.data).setData(new PageId(-1));
//				SystemDefs.JavabaseDB.deallocate_page(((IndexData)kdt.data).getData());
				kdt=root.getNext(itrRid);
			}
			return;
		}
		else{
//			PageId itr = root.getLeftLink();

			BTIndexPage leftChild = new BTIndexPage(root.getLeftLink(), header.keytype);
			destroyIndexes(leftChild, b);

			RID itrRid = new RID();
			KeyDataEntry kdt=root.getFirst(itrRid);
			
			int numberOfRecoreds = root.numberOfRecords();
			for (int i = 0 ; i < numberOfRecoreds ; i ++) {
				
				BTIndexPage child = new BTIndexPage(((IndexData)kdt.data).getData(), header.keytype);
				destroyIndexes(child, b);
				kdt=root.getNext(itrRid);
				
			}
			
			
			
		}
	}
	
	
private KeyDataEntry insert(BTSortedPage p, KeyDataEntry entry) throws KeyNotMatchException, NodeNotMatchException,ConvertException, InvalidSlotNumberException, IOException,ReplacerException, HashOperationException, PageUnpinnedException,InvalidFrameNumberException, PageNotReadException,BufferPoolExceededException, PagePinnedException, BufMgrException,HashEntryNotFoundException, InsertRecException, DiskMgrException,DeleteRecException, ConstructPageException, IteratorException {
		
		if (p.getType() == NodeType.INDEX) {
//			System.err.println(p.getClass());
			p = new BTIndexPage(p, header.keytype);
			try{
				SystemDefs.JavabaseBM.unpinPage(p.getCurPage(), false);
			}catch(Exception e){
				
			}
			PageId pid = ((BTIndexPage) p).getPageNoByKey(entry.key);
			BTSortedPage hf = new BTSortedPage(pid, (int) header.keytype);
			SystemDefs.JavabaseBM.unpinPage(hf.getCurPage(), false);
			KeyDataEntry newChild = insert(hf, entry);
			if (newChild == null) {
				return newChild;
			} else {
				if (p.available_space() >= BT.getBytesFromEntry(entry).length) {
					p = new BTIndexPage(p, header.keytype);
					try{
						SystemDefs.JavabaseBM.unpinPage(p.getCurPage(), false);
					}catch(Exception e){
						
					}

					((BTIndexPage) p).insertKey(newChild.key,((IndexData) newChild.data).getData());
					return null;
				}
				BTIndexPage L2 = new BTIndexPage(header.keytype);
				SystemDefs.JavabaseBM.unpinPage(L2.getCurPage(), false);
				p = new BTIndexPage(p, header.keytype);
				try{
					SystemDefs.JavabaseBM.unpinPage(p.getCurPage(), false);
				}catch(Exception e){
				}
				p=splitIndex((BTIndexPage) p, L2, newChild);
				RID rid2 = new RID();
				newChild = new KeyDataEntry(L2.getFirst(rid2).key,L2.getCurPage());
				RID rid3 = new RID();
				KeyDataEntry kdt =L2.getFirst(rid3);
				L2.setLeftLink(((IndexData)kdt.data).getData());
				L2.deleteSortedRecord(rid3);
				
				if (p.getCurPage().pid == root.getCurPage().pid) {
					BTIndexPage L3 = new BTIndexPage(header.keytype);
					SystemDefs.JavabaseBM.unpinPage(L3.getCurPage(), false);
					L3.setLeftLink(p.getCurPage());
					L3.insertKey(newChild.key,((IndexData) newChild.data).getData());
					this.root = L3;
					this.header.rootID = L3.getCurPage().pid;
					
//					System.err.println("after in root");
//					System.err.println("root id="+root.getCurPage());
//					System.err.println("l1 id=" + p.getCurPage());
//					System.err.println("l2 id ="+L2.getCurPage());
//					BT.printPage(root.getCurPage(), root.keyType);
//					BT.printPage(p.getCurPage(), root.keyType);
//					BT.printPage(L2.getCurPage(), root.keyType);
					return null;
				}
				return newChild;
			}
		} else {
			if (p.available_space() >= BT.getBytesFromEntry(entry).length) {
				p = new BTLeafPage(p, header.keytype);
				try{
					SystemDefs.JavabaseBM.unpinPage(p.getCurPage(), false);
				}catch(Exception e){
					
				}
				((BTLeafPage) p).insertRecord(entry.key,((LeafData) entry.data).getData());
				return null;
			} else {
				HFPage hf = new HFPage();
				PageId pid = SystemDefs.JavabaseBM.newPage(hf, 1);
				BTLeafPage L2 = new BTLeafPage(pid, (int) header.keytype);
				L2.init(pid, hf);
				SystemDefs.JavabaseBM.unpinPage(L2.getCurPage(), false);
				p = new BTLeafPage(p, header.keytype);
				try{
					SystemDefs.JavabaseBM.unpinPage(p.getCurPage(), false);
				}
				catch(Exception e){
					
				}

				try {
					p = splitLeaf((BTLeafPage) p, L2, entry);

				} catch (IteratorException e) {
					e.printStackTrace();
				}
				// Unsure.
				IndexData data =  new IndexData(L2.getCurPage());
				
				RID rid4 = new RID();
				KeyDataEntry newChild = new KeyDataEntry(L2.getFirst(rid4).key, data);
				L2.setNextPage(p.getNextPage());
				p.setNextPage(L2.getCurPage());
				L2.setPrevPage(p.getCurPage());
				return newChild;

			}
		}
	}


	private BTIndexPage splitIndex(BTIndexPage p, BTIndexPage l2, KeyDataEntry entry)throws IOException, InvalidSlotNumberException,KeyNotMatchException, NodeNotMatchException, ConvertException,InsertRecException, DeleteRecException, ConstructPageException, HashEntryNotFoundException, ReplacerException, PageUnpinnedException, InvalidFrameNumberException, IteratorException {
		indexPages++;
		
		RID rid = new RID();
		KeyDataEntry kdt=p.getFirst(rid);
		int size = p.numberOfRecords();
		for (int i = 0; i < size; i++) {
			if (i < ((size + 1) / 2)) {
				
			} else {
				l2.insertKey(kdt.key ,((IndexData)kdt.data).getData());
			}
			kdt=p.getNext(rid);
		}
		p.getFirst(rid);

		for (int i = 0; i < size; i++) {
			if (i >= (size + 1) / 2)
				p.deleteSortedRecord(rid);
			else
				p.getNext(rid);
		}
		RID rid2 = new RID();
		if (BT.keyCompare(entry.key, l2.getFirst(rid2).key) >=0 ) {
			l2.insertKey(entry.key, ((IndexData) entry.data).getData());

		} else {
			p.insertKey(entry.key, ((IndexData) entry.data).getData());
		}
		
		return p;
	}
	private BTLeafPage splitLeaf(BTLeafPage p, BTLeafPage l2, KeyDataEntry enteryKdt) throws InvalidSlotNumberException, KeyNotMatchException, NodeNotMatchException, ConvertException, IOException, InsertRecException, ConstructPageException, HashEntryNotFoundException, ReplacerException, PageUnpinnedException, InvalidFrameNumberException, IteratorException, DeleteRecException {
		leafPages++;
		BTLeafPage l1 = new BTLeafPage((int)header.keytype);
		RID rid = new RID();
		p.getFirst(rid);
		int size = p.numberOfRecords();
		for(int i = 0; i < size; i++)
		{
			if(i<((size+1)/2))
			{
				
			}
			else{
				l2.insertRecord(p.getCurrent(rid));
			}
			p.getNext(rid);
		}
	      
	      p.getFirst(rid);
			
			for(int i = 0 ; i < size;i++)
			{			
				if(i>=(size+1)/2)
				p.delEntry(p.getCurrent(rid));
				else p.getNext(rid);
			}
		RID rid2 = new RID();
		if(BT.keyCompare(enteryKdt.key, l2.getFirst(rid2).key)>=0)
			l2.insertRecord(enteryKdt.key,((LeafData) enteryKdt.data).getData());
		else p.insertRecord(enteryKdt.key,((LeafData) enteryKdt.data).getData());
		
		
		return p;		
	}
	
	
}
