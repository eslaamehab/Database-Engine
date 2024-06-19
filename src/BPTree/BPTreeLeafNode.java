package src.BPTree;

import src.DBGeneralEngine.OverflowPage;
import src.DBGeneralEngine.DBAppException;
import src.Ref.GeneralRef;
import src.Ref.OverflowRef;
import src.Ref.Ref;
import src.DBGeneralEngine.LeafNode;

import java.io.File;
import java.io.Serializable;
import java.util.ArrayList;


public class BPTreeLeafNode<T extends Comparable<T>> extends BPTreeNode<T> implements Serializable, LeafNode<T> {


    /**
     * Attributes
     *
     * Define each attribute
     */
    private final GeneralRef[] records;
    private String nextNodeName;
    public static ArrayList<OverflowRef> pagesToPrint;


    /**
     * Constructor
     *
     * Initializes a new BPTreeLeafNode with the specified capacity.
     * It calls the constructor of the superclass (BPTreeNode) to set the node's capacity
     * Then it creates new arrays to store the node's keys and records.
     *
     * @param capacity The capacity of the new BPTreeLeafNode.
     * @throws DBAppException if the capacity is less than or equal to 0.
     */
    @SuppressWarnings("unchecked")
    public BPTreeLeafNode(int capacity) throws DBAppException {
        super(capacity);
        setKeys(new Comparable[capacity]);
        records = new GeneralRef[capacity];
    }


    /**
     * Getters & Setters
     *
     */
    public GeneralRef[] getRecords() {
        return records;
    }

    public static ArrayList<OverflowRef> getPagesToPrint() {
        return pagesToPrint;
    }

    public static void setPagesToPrint(ArrayList<OverflowRef> pagesToPrint) {
        BPTreeLeafNode.pagesToPrint = pagesToPrint;
    }

    /**
     * Gets the next BPTreeLeafNode in the linked list of leaf nodes.
     *
     * @return The next BPTreeLeafNode in the linked list, or null if this is the last node.
     * @throws DBAppException if an error occurs during the deserialization process.
     */
    public BPTreeLeafNode getNextNode() throws DBAppException {
        return (nextNodeName != null) ? ((BPTreeLeafNode)deserializeNode(nextNodeName)) : null;
    }

    /**
     *Gets the name of the next BPTreeLeafNode in the linked list.
     *
     * @return The name of the next BPTreeLeafNode, or null if this is the last node.
     * @throws DBAppException if an error occurs while accessing the `nextNodeName` field.
     */
    @Override
    public String getNextNodeName() throws DBAppException
    {
        return nextNodeName;
    }

    /**
     * Sets the name of the next BPTreeLeafNode in the linked list.
     *
     * This method sets the `nextNodeName` field to the provided String value.
     * This represents the name of the next BPTreeLeafNode in the linked list of leaf nodes.
     *
     * @param nodeName The name of the next BPTreeLeafNode, or null if this is the last node.
     */
    public void setNextNodeName(String nodeName) {
        this.nextNodeName = nodeName;
    }

    /**
     * Sets the next BPTreeLeafNode in the linked list.
     *
     * This method sets the `nextNodeName` field to the name of the provided BPTreeLeafNode.
     * If the `nextNodeName` is already null, it sets the `nextNodeName` to null.
     * Indicating that this is the last node in the linked list.
     *
     * @param node The next BPTreeLeafNode in the linked list, or null if this is the last node.
     */
    public void setNextNodeName(BPTreeLeafNode<T> node) {
        this.nextNodeName = (nextNodeName != null) ? node.getNodeName() : null;
    }

    /**
     * Retrieves the record reference stored at the specified index.
     *
     * @param i The index of the record to retrieve.
     * @return the GeneralReference of the queried index.
     */
    public GeneralRef getRecord(int i)
    {
        return records[i];
    }


    /**
     * Sets the record at the given index with the given reference
     *
     * @param i the index of the record to update/set
     * @param ref the GeneralReference to store at the given index
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
     * Finds the minimum number of keys the current node could hold.
     */
    public int minKeys()
    {
        return this.isRoot() ? 1 : (getOrder() + 1) / 2;
    }


    /**
     * Inserts a new key-record pair into the B+-tree leaf node.
     *
     * This method handles the insertion of the given new key and its corresponding record
     * reference into the leaf node of the B+-tree.
     *
     * @param key The new key to be inserted.
     * @param recordReference The record reference associated with the new key.
     * @param parent The parent node of this leaf node.
     * @param ptr The index of this leaf node in the parent node.
     * @return A new PushUpBPTree object if the node was split, or null if the
     *         insertion was successful without requiring a split.
     * @throws DBAppException If an error occurs during the insertion process.
     */
    public PushUpBPTree<T> insert(T key,
                                  Ref recordReference,
                                  BPTreeInnerNode<T> parent,
                                  int ptr) throws DBAppException
    {

        int index = 0;


        while (index < getNumberOfKeys() && getKey(index).compareTo(key) < 0)
            ++index;

        // If the node is full, it splits the node into two,
        // and Returns a new PushUpBPTree object with the new middle key and the two child nodes.
        if(this.isFull())
        {
            BPTreeNode<T> newNode = this.split(key, recordReference);
            Comparable<T> newKey = newNode.getFirstKey();
            newNode.serializeNode();
            return new PushUpBPTree<T>(newNode, newKey);
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
     * Inserts a new key-record pair at the given index in the leaf node.
     *
     * @param index The index at which the new key-record pair should be inserted.
     * @param key The new key to be inserted.
     * @param generalRef The new record reference to be inserted.
     */
    public void insertAt(int index, Comparable<T> key, GeneralRef generalRef)
    {
        // Shift all existing keys and records to the right to insert the new key-record pair.
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
     * Splits the current leaf node and returns the newly created leaf node.
     *
     * @param key The new key to be inserted.
     * @param generalRef The new record reference to be inserted.
     * @return The newly created leaf node.
     * @throws DBAppException If an error occurs during the split operation.
     */
    public BPTreeNode<T> split(T key, GeneralRef generalRef) throws DBAppException
    {
        // Find the index at which the new key should be inserted
        int keyIndex = this.findIndex(key);
        // get the middle index, which is the split point
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

        // Update the next node pointers
        newNode.setNextNodeName(this.getNextNode());
        this.setNextNodeName(newNode.getNextNodeName());

        return newNode;
    }


    /**
     * Finds the index at which the given key should be located in the node.
     * Linear searches through the keys in the node.
     *
     * @param key the key to be checked for its location
     * @return the index at which the key should be located
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
     * Searches for the given key in the node and returns the corresponding reference.
     *
     * @param key The key to search for.
     * @return The GeneralRef of the record associated with the given key, or null if the key does not exist.
     */
    @Override
    public GeneralRef search(T key)
    {
        // Iterate through the keys in the node
        for(int i = 0; i < getNumberOfKeys(); ++i)
            if(this.getKey(i).compareTo(key) == 0)
                return this.getRecord(i);
        return null;
    }


    /**
     * Searches for the corresponding reference for inserting the given key.
     *
     * @param key The key to be inserted.
     * @param tableLength The length of the table.
     * @return The Ref object of the record where the key should be inserted, or null if it does not exist.
     * @throws DBAppException If an error occurs during the search process.
     */
    public Ref searchForInsertion(T key,int tableLength)throws DBAppException
    {
        // Iterate through the keys to find the insertion point
        int i=0;
        for(; i < getNumberOfKeys(); i++){
            if(this.getKey(i).compareTo(key) >= 0)
                return this.getRef((this.getRecord(i)),tableLength);
        }
        // If the key is greater than all existing keys, return the reference of the last key
        if( i>0 ){
            return this.getRef(this.getRecord(i-1),tableLength);
        }
        return null;
    }


    /**
     * Gets the reference from a GeneralRef object.
     * If the GeneralRef is an instance of Ref, it is returned directly.
     * If it is an instance of OverflowRef, it retrieves the maximum reference page.
     *
     * @param generalRef The GeneralRef object to get the reference from.
     * @param tableLength The length of the table.
     * @return The Ref object retrieved from the GeneralRef.
     * @throws DBAppException If an error occurs during the process.
     */
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
     * Deletes the given key from the B+ Tree
     *
     * @param key    The key to be deleted.
     * @param parent The parent node of the current node.
     * @param ptr    The position of the current node in the parent's children array.
     * @return true if the key was successfully deleted, false otherwise.
     * @throws DBAppException If an error occurs during the deletion process.
     */
    public boolean delete(T key, BPTreeInnerNode<T> parent, int ptr) throws DBAppException {
        for (int i = 0; i < getNumberOfKeys(); ++i) {
            if (getKeys()[i].compareTo(key) == 0) {
                this.deleteAt(i);

                // If the deleted key is the first key and this is not the first child, update the parent key
                if (i == 0 && ptr > 0) {
                    parent.setKey(ptr - 1, this.getFirstKey());
                }

                // If the node is not the root and it has fewer keys than the minimum required, handle underflow
                if (!this.isRoot() && getNumberOfKeys() < this.minKeys()) {
                    // Attempt to borrow a key from a sibling node
                    if (borrow(parent, ptr))
                        return true;

                    // If borrowing fails, merge with a sibling node
                    merge(parent, ptr);
                }
                return true;
            }
        }
        return false;
    }


    /**
     * Deletes the given key from the B+ Tree.
     *
     * @param key      The key to be deleted.
     * @param parent   The parent node of the current node.
     * @param ptr      The position of the current node in the parent's children array.
     * @param pageName The name of the page associated with the key.
     * @return true if the key was successfully deleted, false otherwise.
     * @throws DBAppException If an error occurs during the deletion process.
     */
    public boolean delete(T key, BPTreeInnerNode<T> parent, int ptr, String pageName) throws DBAppException {
        for (int i = 0; i < getNumberOfKeys(); ++i) {
            if (getKeys()[i].compareTo(key) == 0) {
                // Handle regular reference deletion
                if (records[i] instanceof Ref) {
                    this.deleteAt(i);
                } else { // Handle overflow reference deletion
                    OverflowRef overflowRef = (OverflowRef) records[i];
                    overflowRef.deleteRef(pageName);

                    // If the overflow reference has only one element left, convert it back to a regular reference
                    if (overflowRef.getTotalSize() == 1) {
                        OverflowPage overflowPage = overflowRef.deserializeOverflowPage(overflowRef.getFirstPageName());
                        Ref ref = overflowPage.getRefs().firstElement();
                        records[i] = ref;

                        // Delete the overflow page file
                        File file = new File("data: " + overflowRef.getFirstPageName() + ".class");
                        file.delete();
                    }
                }

                // Update the parent's key if necessary
                if (i == 0 && ptr > 0) {
                    parent.setKey(ptr - 1, this.getFirstKey());
                }

                // Handle underflow by borrowing from or merging with sibling nodes
                if (!this.isRoot() && getNumberOfKeys() < this.minKeys()) {
                    if (borrow(parent, ptr)) {
                        return true;
                    }
                    merge(parent, ptr);
                }
                return true;
            }
        }
        return false;
    }

    /**
     * Searches for the leaf node that should be updated for the given key.
     * Since this is a leaf node, it returns itself.
     *
     * @param key The key to search for.
     * @return This BPTreeLeafNode, as leaf nodes contain the references to be updated.
     */
    @Override
    public BPTreeLeafNode searchForUpdateRef(T key) {
        return this;
    }


    /**
     * Deletes the key-record at the given index fromm the B+ tre.
     *
     * @param index the index of the key-record to be deleted
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
     * Borrows a key from a sibling node (either left or right).
     *
     * @param parent the parent of the current node
     * @param ptr the index of the parent pointer that points to this node
     * @return true if the borrow is done successfully or false otherwise
     * @throws DBAppException If an error occurs during the borrowing process.
     */
    public boolean borrow(BPTreeInnerNode<T> parent, int ptr) throws DBAppException
    {
        // Check left side for enough keys to borrow
        if(ptr > 0)
        {
            BPTreeLeafNode<T> leftSibling = (BPTreeLeafNode<T>) parent.getChild(ptr-1);
            if(leftSibling.getNumberOfKeys() > leftSibling.minKeys())
            {
                // Borrow a key and update the nodes
                this.insertAt(0, leftSibling.getLastKey(), leftSibling.getLastRecord());
                leftSibling.deleteAt(leftSibling.getNumberOfKeys() - 1);
                parent.setKey(ptr - 1, getKeys()[0]);
                leftSibling.serializeNode();
                return true;
            }
        }

        // Check right side for enough keys to borrow
        if(ptr < parent.getNumberOfKeys())
        {
            BPTreeLeafNode<T> rightSibling = (BPTreeLeafNode<T>) parent.getChild(ptr+1);
            if(rightSibling.getNumberOfKeys() > rightSibling.minKeys())
            {
                // Borrow a key and update the nodes
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
     * Merges the current node with its sibling node (either left or right)
     *
     * @param parent the parent of the current node
     * @param ptr the index of the parent pointer that points to this node
     * @throws DBAppException If an error occurs during the merging process.
     */
    public void merge(BPTreeInnerNode<T> parent, int ptr) throws DBAppException
    {
        // If node is on the left side, merge with the left sibling
        if(ptr > 0)
        {
            BPTreeLeafNode<T> leftSibling = (BPTreeLeafNode<T>) parent.getChild(ptr-1);
            leftSibling.merge(this);
            parent.deleteAt(ptr-1);
            leftSibling.serializeNode();
        }
        // node is on the right side, hence merge with right sibling
        else
        {
            BPTreeLeafNode<T> rightSibling = (BPTreeLeafNode<T>) parent.getChild(ptr+1);
            this.merge(rightSibling);
            parent.deleteAt(ptr);
            rightSibling.serializeNode();
        }

    }


    /**
     * Merges the current node with the given foreign node.
     *
     * @param foreignNode the node to be merged with the current node
     * @throws DBAppException If an error occurs during the merging process.
     */
    public void merge(BPTreeLeafNode<T> foreignNode) throws DBAppException
    {
        // Insert all the keys and records from the foreign node into this node
        for(int i = 0; i < foreignNode.getNumberOfKeys(); ++i)
            this.insertAt(getNumberOfKeys(), foreignNode.getKey(i), foreignNode.getRecord(i));

        // Update the next node name in current node to point to the same node as the foreign node
        this.setNextNodeName(foreignNode.getNextNodeName());
    }


    /**
     * Returns a string representation of this leaf node.
     * Mainly for debugging.
     *
     * @return A string representation of this node, in the format "(index) [key1, key2, ...]"
     */
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


    /**
     * Searches for the given key in the current node and returns a list of matching references.
     *
     * @param key The key to search for.
     * @return A list of references that match the given key.
     * @throws DBAppException If an error occurs during the search process.
     */
    public ArrayList<GeneralRef> searchMTE(T key) throws DBAppException{
        ArrayList<GeneralRef> refResult = new ArrayList<>();
        searchMTE(key,refResult);
        return refResult;
    }


    /**
     * Searches for the given key in the current node and its subtree
     * Then adds the matching references to the given referenceResult list.
     *
     * @param key The key to search for.
     * @param refResult The list of references to add matching keys to.
     * @throws DBAppException If an error occurs during the search process.
     */
    public void searchMTE(T key,ArrayList<GeneralRef> refResult)throws DBAppException{
        // Iterate through the keys in the node
        int i = 0;
        for(; i < getNumberOfKeys(); ++i) {
            if(this.getKey(i).compareTo(key) >= 0)
                refResult.add(this.getRecord(i));
        }
        // If there is a next node, recursively search it for matching keys
        if ( nextNodeName != null){
            BPTreeLeafNode nxt = (BPTreeLeafNode)deserializeNode(nextNodeName);
            nxt.searchMTE(key,refResult);
        }

    }


    /**
     * Searches for the given key in this node and returns a list of matching references.
     *
     * @param key The key to search for.
     * @return A list of references that match the given key.
     * @throws DBAppException If an error occurs during the search process.
     */
    public ArrayList<GeneralRef> searchMT(T key)throws DBAppException{
        ArrayList<GeneralRef> refResult = new ArrayList<>();
        searchMT(key,refResult);
        return refResult;
    }


    /**
     * Searches for the given key in this node and adds matching references to the provided result list.
     *
     * @param key The key to search for.
     * @param refResult The list of references to add matching keys to.
     * @throws DBAppException If an error occurs during the search process.
     */
    public void searchMT(T key, ArrayList<GeneralRef> refResult) throws DBAppException{
        // Iterate through the keys in the node
        for(int i = 0; i < getNumberOfKeys(); ++i)
            if(this.getKey(i).compareTo(key) > 0)
                refResult.add(this.getRecord(i));
        // If there is a next node, recursively search it for matching keys and add them to the result list
        if (nextNodeName !=null) {
            BPTreeLeafNode<T> nxt = (BPTreeLeafNode<T>)deserializeNode(nextNodeName);
            nxt.searchMT(key,refResult);
        }
    }


    /**
     * Updates the page reference of a record in the current node with the given key.
     *
     * @param oldPage The current page reference of the record to update.
     * @param newPage The new page reference to update the record with.
     * @param key The key of the record to update.
     * @throws DBAppException If an error occurs during the update process.
     */
    public void updateRef(String oldPage,String newPage,T key) throws DBAppException{
        GeneralRef generalRef;
        // Iterate through the records in this node, to find the record with the matching key
        for(int i = 0; i < getNumberOfKeys(); ++i)
            if(this.getKey(i).compareTo(key) == 0) {
                // Get the record and update its page reference
                generalRef = getRecord(i);
                generalRef.updateRef(oldPage, newPage);
                if (generalRef instanceof Ref) {
                    this.serializeNode();
                }
                return;
            }
    }

}

