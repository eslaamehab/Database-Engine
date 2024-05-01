package src.BPTree;

import src.APTree.OverflowPage;
import src.DBGeneralEngine.DBAppException;
import src.Ref.GeneralRef;
import src.Ref.OverflowRef;
import src.Ref.Ref;
import src.APTree.APTreeLeafNode;

import java.io.File;
import java.io.Serializable;
import java.util.ArrayList;


public class BPTreeLeafNode<T extends Comparable<T>> extends BPTreeNode<T> implements Serializable, APTreeLeafNode<T> {


    // attributes

    private final GeneralRef[] records;
    private String nextNodeName;

    public static ArrayList<OverflowRef> pagesToPrint;


    // constructor

    @SuppressWarnings("unchecked")
    public BPTreeLeafNode(int i) throws DBAppException
    {
        super(i);
        setKeys(new Comparable[i]);
        records = new GeneralRef[i];

    }

    // getter and setters ?

    /**
     * @return the next leaf node
     * @throws DBAppException
     */
    public BPTreeLeafNode<T> getNextNode() throws DBAppException
    {
        if ( nextNodeName != null )
        {
            return ((BPTreeLeafNode)deserializeNode(nextNodeName));
        }
        else
        {
            return null;
        }
    }

    @Override
    public String getNextNodeName() throws DBAppException
    {
        return nextNodeName;
    }
    public void setNextNodeName(String nodeName) {
        this.nextNodeName = nodeName;
    }

    /**
     * sets the next leaf node
     * @param node the next leaf node
     */
    public void setNextNodeName(BPTreeLeafNode<T> node)
    {

        if ( nextNodeName != null )
        {
            this.nextNodeName = node.getNodeName();
        }
        else
        {
            System.out.println("Next is null");
            this.nextNodeName = null;
        }
    }


    /**
     * @param i the index to find its record
     * @return the reference of the queried index
     */
    public GeneralRef getRecord(int i)
    {
        return records[i];
    }

    /**
     * sets the record at the given index with the passed reference
     * @param i the index to set the value at
     * @param ref the reference to the record
     */
    public void setRecord(int i, GeneralRef ref)
    {
        records[i] = ref;
    }

    /**
     * @return the reference of the first record
     */
    public GeneralRef getFirstRecord()
    {
        return records[0];
    }

    /**
     * @return the reference of the last record
     */
    public GeneralRef getLastRecord()
    {
        return records[getNumberOfKeys() -1];
    }

    /**
     * finds the minimum number of keys the current node must hold
     */
    public int minKeys()
    {
        if(this.isRoot())
            return 1;
        return (getOrder() + 1) / 2;
    }

    /**
     * insert the specified key associated with a given record reference in the B+ tree
     * @throws DBAppException
     */
    public PushUp<T> insert(T key,
                            Ref recordReference,
                            BPTreeInnerNode<T> parent,
                            int ptr) throws DBAppException
    {

        int index = 0;


        while (index < getNumberOfKeys() && getKey(index).compareTo(key) < 0)
            ++index;

        if(this.isFull())
        {
            BPTreeNode<T> newNode = this.split(key, recordReference);
            Comparable<T> newKey = newNode.getFirstKey();
            newNode.serializeNode();
            return new PushUp<T>(newNode, newKey);
        }

        else if (index< getNumberOfKeys() && getKey(index).compareTo(key)==0) {
            GeneralRef ref = records[index];
            if (ref.isOverflow()) {

                OverflowRef overflowRef=(OverflowRef)ref;
                overflowRef.insert(recordReference);

            }
            else {
                OverflowRef overflowRef = new OverflowRef();
                OverflowPage overflowPage = new OverflowPage(getOrder());
                overflowRef.setFirstPage(overflowPage);
                overflowRef.insert((Ref)ref);
                overflowRef.insert(recordReference);
                records[index]=overflowRef;
            }
            return null;
        }

        else
        {
            this.insertAt(index, key, recordReference);
            return null;
        }
    }

    /**
     * inserts the passed key associated with its record reference in the specified index
     * @param index the index at which the key will be inserted
     * @param key the key to be inserted
     * @param generalRef the pointer to the record associated with the key
     */
    private void insertAt(int index, Comparable<T> key, GeneralRef generalRef)
    {
        for (int i = getNumberOfKeys() - 1; i >= index; --i)
        {
            this.setKey(i + 1, getKey(i));
            this.setRecord(i + 1, getRecord(i));
        }

        this.setKey(index, key);
        this.setRecord(index, generalRef);
        setNumberOfKeys( getNumberOfKeys() + 1 );
    }

    /**
     * splits the current node
     * @param key the new key that caused the split
     * @param generalRef the reference of the new key
     * @return the new node that results from the split
     * @throws DBAppException
     */
    public BPTreeNode<T> split(T key, GeneralRef generalRef) throws DBAppException
    {
        int keyIndex = this.findIndex(key);
        int midIndex = getNumberOfKeys() / 2;
        int totalKeys = getNumberOfKeys() + 1;

        if((getNumberOfKeys() & 1) == 1 && keyIndex > midIndex)
            ++midIndex;

        BPTreeLeafNode<T> newNode = new BPTreeLeafNode<T>(getOrder());
        for (int i = midIndex; i < totalKeys - 1; ++i)
        {
            newNode.insertAt(i - midIndex, this.getKey(i), this.getRecord(i));
            setNumberOfKeys( getNumberOfKeys() - 1 );
        }

        if(keyIndex < totalKeys / 2)
            this.insertAt(keyIndex, key, generalRef);
        else
            newNode.insertAt(keyIndex - midIndex, key, generalRef);


        newNode.setNextNodeName(this.getNextNode());
        this.setNextNodeName((BPTreeLeafNode<T>) newNode.getNextNodeName());

        return newNode;
    }

    /**
     * finds the index at which the passed key must be located
     * @param key the key to be checked for its location
     * @return the expected index of the key
     */
    public int findIndex(T key)
    {
        for (int i = 0; i < getNumberOfKeys(); ++i)
        {
            int cmp = getKey(i).compareTo(key);
            if (cmp > 0)
                return i;
        }
        return getNumberOfKeys();
    }

    /**
     * returns the record reference with the passed key and null if it does not exist
     */
    @Override
    public GeneralRef search(T key)
    {
        for(int i = 0; i < getNumberOfKeys(); ++i)
            if(this.getKey(i).compareTo(key) == 0)
                return this.getRecord(i);
        return null;
    }

    public Ref searchForInsertion(T key,int tableLength)throws DBAppException
    {
        int i=0;
        for(; i < getNumberOfKeys(); i++){
            if(this.getKey(i).compareTo(key) >= 0)
                return this.getRef((this.getRecord(i)),tableLength);
        }
        if(i>0){
            return this.getRef(this.getRecord(i-1),tableLength);
        }
        return null;
    }
    public Ref getRef(GeneralRef generalRef, int tableLength) throws DBAppException {
        if(generalRef instanceof Ref){
            return (Ref)generalRef;
        }else{
            OverflowRef overflowRef = (OverflowRef) generalRef;
            String pageName = overflowRef.getFirstPageName();
            OverflowPage overflowPage = overflowRef.deserializeOverflowPage(pageName);

            return overflowPage.getMaxRefPage(tableLength);

        }
    }

    /**
     * delete the passed key from the B+ tree
     * @throws DBAppException
     */
    public boolean delete(T key, BPTreeInnerNode<T> parent, int ptr) throws DBAppException
    {
        for(int i = 0; i < getNumberOfKeys(); ++i)
            if(getKeys()[i].compareTo(key) == 0)
            {
                this.deleteAt(i);
                if(i == 0 && ptr > 0)
                {
                    //update key at parent
                    parent.setKey(ptr - 1, this.getFirstKey());
                }

                if(!this.isRoot() && getNumberOfKeys() < this.minKeys())
                {
                    if(borrow(parent, ptr))
                        return true;

                    merge(parent, ptr);
                }
                return true;
            }
        return false;
    }


    public boolean delete(T key, BPTreeInnerNode<T> parent, int ptr,String pageName) throws DBAppException
    {
        for(int i = 0; i < getNumberOfKeys(); ++i)
            if(getKeys()[i].compareTo(key) == 0)
            {

                if(records[i] instanceof Ref) {
                    this.deleteAt(i);
                }
                else
                {
                    OverflowRef overflowRef = (OverflowRef) records[i];
                    overflowRef.deleteRef(pageName);
                    if(overflowRef.getTotalSize() == 1)
                    {
                        OverflowPage overflowPage = overflowRef.deserializeOverflowPage(overflowRef.getFirstPageName());
                        Ref ref = overflowPage.getRefs().firstElement();
                        records[i] = ref;

                        File file = new File("data: "+overflowRef.getFirstPageName() + ".class");
                        file.delete();
                    }

                }


                if(i == 0 && ptr > 0)
                {
                    parent.setKey(ptr - 1, this.getFirstKey());

                }
                if(!this.isRoot() && getNumberOfKeys() < this.minKeys())
                {
                    if(borrow(parent, ptr)) {
                        return true;
                    }
                    merge(parent, ptr);
                }
                return true;
            }

        return false;
    }

    /**
     * delete a key at the specified index of the node
     * @param index the index of the key to be deleted
     */
    public void deleteAt(int index)
    {
        for(int i = index; i < getNumberOfKeys() - 1; ++i)
        {
            getKeys()[i] = getKeys()[i+1];
            records[i] = records[i+1];
        }
        setNumberOfKeys( getNumberOfKeys() - 1 );
    }

    /**
     * tries to borrow a key from the left or right sibling
     * @param parent the parent of the current node
     * @param ptr the index of the parent pointer that points to this node
     * @return true if borrow is done successfully and false otherwise
     * @throws DBAppException
     */
    public boolean borrow(BPTreeInnerNode<T> parent, int ptr) throws DBAppException
    {
        // left side
        if(ptr > 0)
        {
            BPTreeLeafNode<T> leftSibling = (BPTreeLeafNode<T>) parent.getChild(ptr-1);
            if(leftSibling.getNumberOfKeys() > leftSibling.minKeys())
            {
                this.insertAt(0, leftSibling.getLastKey(), leftSibling.getLastRecord());
                leftSibling.deleteAt(leftSibling.getNumberOfKeys() - 1);
                parent.setKey(ptr - 1, getKeys()[0]);
                leftSibling.serializeNode();
                return true;
            }
        }

        // right side
        if(ptr < parent.getNumberOfKeys())
        {
            BPTreeLeafNode<T> rightSibling = (BPTreeLeafNode<T>) parent.getChild(ptr+1);
            if(rightSibling.getNumberOfKeys() > rightSibling.minKeys())
            {
                this.insertAt(getNumberOfKeys(), rightSibling.getFirstKey(), rightSibling.getFirstRecord());
                rightSibling.deleteAt(0);
                parent.setKey(ptr, rightSibling.getFirstKey());
                rightSibling.serializeNode();
                return true;
            }
        }
        return false;
    }

    /**
     * merges the current node with its left or right sibling
     * @param parent the parent of the current node
     * @param ptr the index of the parent pointer that points to this node
     * @throws DBAppException
     */
    public void merge(BPTreeInnerNode<T> parent, int ptr) throws DBAppException
    {
        if(ptr > 0)
        {
            // merge with left side
            BPTreeLeafNode<T> leftSibling = (BPTreeLeafNode<T>) parent.getChild(ptr-1);
            leftSibling.merge(this);
            parent.deleteAt(ptr-1);
            leftSibling.serializeNode();
        }
        else
        {
            // merge with right side
            BPTreeLeafNode<T> rightSibling = (BPTreeLeafNode<T>) parent.getChild(ptr+1);
            this.merge(rightSibling);
            parent.deleteAt(ptr);
            rightSibling.serializeNode();
        }

    }

    /**
     * merge the current node with the specified node. The foreign node will be deleted
     * @param foreignNode the node to be merged with the current node
     * @throws DBAppException
     */
    public void merge(BPTreeLeafNode<T> foreignNode) throws DBAppException
    {
        for(int i = 0; i < foreignNode.getNumberOfKeys(); ++i)
            this.insertAt(getNumberOfKeys(), foreignNode.getKey(i), foreignNode.getRecord(i));

        this.setNextNodeName((BPTreeLeafNode<T>) foreignNode.getNextNodeName());
    }



    public String toString()
    {
        StringBuilder str = new StringBuilder("(" + getIndex() + ")");

        str.append("[");

        for (int i = 0; i < getOrder(); i++)
        {
            String key = " ";
            if(i < getNumberOfKeys()) {
                key = getKeys()[i].toString();

                if(records[i] instanceof Ref)
                {
                    key += "," + records[i];
                }
                else
                {
                    key += ","+((OverflowRef)records[i]).getFirstPageName();

                    if ( pagesToPrint == null )
                        pagesToPrint = new ArrayList<>();

                    pagesToPrint.add((OverflowRef) records[i]);
                }

            }
            str.append(key);
            if(i < getOrder() - 1)
                str.append("|");
        }
        str.append("]");
        return str.toString();
    }

    public ArrayList<GeneralRef> searchMTE(T key) throws DBAppException{
        ArrayList<GeneralRef> refResult = new ArrayList<>();
        searchMTE(key,refResult);
        return refResult;
    }
    public ArrayList<GeneralRef> searchMT(T key)throws DBAppException{
        ArrayList<GeneralRef> refResult = new ArrayList<>();
        searchMT(key,refResult);
        return refResult;
    }

    public void searchMTE(T key,ArrayList<GeneralRef> refResult)throws DBAppException{
        int i = 0;
        for(; i < getNumberOfKeys(); ++i) {
            if(this.getKey(i).compareTo(key) >= 0)
                refResult.add(this.getRecord(i));
        }
        if ( nextNodeName != null){
            BPTreeLeafNode nxt = (BPTreeLeafNode)deserializeNode(nextNodeName);
            nxt.searchMTE(key,refResult);
        }

    }
    public void searchMT(T key, ArrayList<GeneralRef> refResult) throws DBAppException{

        for(int i = 0; i < getNumberOfKeys(); ++i)
            if(this.getKey(i).compareTo(key) > 0)
                refResult.add(this.getRecord(i));

        if (nextNodeName !=null) {
            BPTreeLeafNode<T> nxt = (BPTreeLeafNode<T>)deserializeNode(nextNodeName);
            nxt.searchMT(key,refResult);
        }
    }

    public APTreeLeafNode<T> searchForUpdateRef(T key) {
        return this;
    }

    public void updateRef(String oldPage,String newPage,T key) throws DBAppException{
        GeneralRef generalRef;

        for(int i = 0; i < getNumberOfKeys(); ++i)
            if(this.getKey(i).compareTo(key) == 0) {
                generalRef = getRecord(i);
                generalRef.updateRef(oldPage, newPage);
                if (generalRef instanceof Ref) {
                    this.serializeNode();
                }
                return;
            }
    }

}

