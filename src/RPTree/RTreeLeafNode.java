package src.RPTree;

import src.DBGeneralEngine.LeafNode;
import src.DBGeneralEngine.OverflowPage;
import src.DBGeneralEngine.DBAppException;
import src.Ref.GeneralRef;
import src.Ref.OverflowRef;
import src.Ref.Ref;

import java.io.Serializable;
import java.util.ArrayList;

public class RTreeLeafNode<CustomPolygon extends Comparable<CustomPolygon>> extends RTreeNode<CustomPolygon> implements Serializable, LeafNode<CustomPolygon> {

    // attributes
    private final GeneralRef[] recordsReference;
    private String nextNode;

    public static ArrayList<OverflowRef> pagesToPrint;


    // constructors
    @SuppressWarnings("unchecked")
    public RTreeLeafNode(int n) throws DBAppException {
        super(n);
        keys = new Comparable[n];
        recordsReference = new GeneralRef[n];

    }

    /**
     * @return the next leaf node
     * @throws DBAppException
     */
    public RTreeLeafNode<CustomPolygon> getNextNode() throws DBAppException {
        return (nextNode == null) ? null : ((RTreeLeafNode) deserializeNode(nextNode));
    }


    //  getters and setters
    public String getNextNodeName() throws DBAppException {
        return nextNode;
    }


    /**
     * sets the next leaf node
     *
     * @param node the next leaf node
     */
    public void setNextNode(RTreeLeafNode<CustomPolygon> node) {
        this.nextNode = (node != null) ? node.nodeName : null;
    }


    public void setNextNodeName(String nodeName) {
        this.nextNode = nodeName;
    }


    /**
     * @param index the index to find its record
     * @return the reference of the queried index
     */
    public GeneralRef getRecord(int index) {
        return recordsReference[index];
    }


    /**
     * sets the record at the given index with the passed reference
     *
     * @param index the index to set the value at
     * @param ref   the reference to the record
     */
    public void setRecord(int index, GeneralRef ref) {
        recordsReference[index] = ref;
    }


    /**
     * @return the reference of the last record
     */
    public GeneralRef getFirstRecord() {
        return recordsReference[0];
    }


    /**
     * @return the reference of the last record
     */
    public GeneralRef getLastRecord() {
        return recordsReference[numberOfKeys - 1];
    }


    /**
     * finds the minimum number of keys the current node must hold
     */
    public int minKeys() {
        return this.isRoot() ? 1 : (order + 1) / 2;
    }


    /**
     * insert the specified key associated with a given record refernce in the R tree
     *
     * @throws DBAppException
     */
    public PushUp<CustomPolygon> insert(CustomPolygon key,
                                        Ref recordReference,
                                        RTreeInnerNode<CustomPolygon> parent,
                                        int ptr) throws DBAppException {

        int index = 0;
        while (index < numberOfKeys && getKey(index).compareTo(key) < 0)
            ++index;

        if (index < numberOfKeys && getKey(index).compareTo(key) == 0) {
            GeneralRef ref = recordsReference[index];
            if (ref.isOverflow()) {
                OverflowRef overflowRef = (OverflowRef) ref;
                overflowRef.insert(recordReference);

            } else {
                OverflowRef overflowRef = new OverflowRef();
                OverflowPage overflowPage = new OverflowPage(order);

                overflowRef.setFirstPage(overflowPage);
                overflowRef.insert((Ref) ref);
                overflowRef.insert(recordReference);
                recordsReference[index] = overflowRef;
            }

            return null;
        } else if (this.isFull()) {
            RTreeNode<CustomPolygon> newNode = this.split(key, recordReference);
            Comparable<CustomPolygon> newKey = newNode.getFirstKey();
            newNode.serializeNode();

            return new PushUp<>(newNode, newKey);
        } else {
            this.insertAt(index, key, recordReference);
            return null;
        }
    }


    /**
     * inserts the passed key associated with its record reference in the specified index
     *
     * @param index     the index at which the key will be inserted
     * @param key       the key to be inserted
     * @param recordPtr the pointer to the record associated with the key
     */
    private void insertAt(int index, Comparable<CustomPolygon> key, GeneralRef recordPtr) {
        for (int i = numberOfKeys - 1; i >= index; --i) {
            this.setKey(i + 1, getKey(i));
            this.setRecord(i + 1, getRecord(i));
        }

        this.setKey(index, key);
        this.setRecord(index, recordPtr);
        ++numberOfKeys;
    }


    /**
     * splits the current node
     *
     * @param key       the new key that caused the split
     * @param newKeyRef the reference of the new key
     * @return the new node that results from the split
     * @throws DBAppException
     */
    public RTreeNode<CustomPolygon> split(CustomPolygon key, GeneralRef newKeyRef) throws DBAppException {
        int keyIndex = this.findIndex(key);

        // split nodes
        int midIndex = numberOfKeys / 2;
        if ((numberOfKeys & 1) == 1 && keyIndex > midIndex)
            ++midIndex;

        int totalKeys = numberOfKeys + 1;

        // move keys to new node
        RTreeLeafNode<CustomPolygon> newNode = new RTreeLeafNode<CustomPolygon>(order);
        for (int i = midIndex; i < totalKeys - 1; ++i) {
            newNode.insertAt(i - midIndex, this.getKey(i), this.getRecord(i));
            numberOfKeys--;
        }

        //insert new key
        if (keyIndex < totalKeys / 2)
            this.insertAt(keyIndex, key, newKeyRef);
        else
            newNode.insertAt(keyIndex - midIndex, key, newKeyRef);

        newNode.setNextNodeName(this.getNextNodeName());
        this.setNextNodeName(newNode.nodeName);

        return newNode;
    }


    /**
     * finds the index at which the passed key must be located
     *
     * @param key the key to be checked for its location
     * @return the expected index of the key
     */
    public int findIndex(CustomPolygon key) {
        for (int i = 0; i < numberOfKeys; ++i) {
            int compareKeys = getKey(i).compareTo(key);
            if (compareKeys > 0)
                return i;
        }
        return numberOfKeys;
    }


    /**
     * returns the record reference with the passed key and null if does not exist
     */
    @Override
    public GeneralRef search(CustomPolygon key) {
        for (int i = 0; i < numberOfKeys; ++i)
            if (this.getKey(i).compareTo(key) == 0)
                return this.getRecord(i);
        return null;
    }


    public Ref searchForInsertion(CustomPolygon key, int tableLength) throws DBAppException {
        int i = 0;
        for (; i < numberOfKeys; i++) {
            if (this.getKey(i).compareTo(key) >= 0)
                return this.refReference((this.getRecord(i)), tableLength);
        }
        if (i > 0) {
            return this.refReference(this.getRecord(i - 1), tableLength);
        }
        return null;
    }


    private Ref refReference(GeneralRef generalReference, int tableLength) throws DBAppException {
        if (generalReference instanceof Ref) {
            return (Ref) generalReference;
        } else {
            OverflowRef overflowRef = (OverflowRef) generalReference;
            String firstPageName = overflowRef.getFirstPageName();
            OverflowPage overflowPage = overflowRef.deserializeOverflowPage(firstPageName);

            return overflowPage.getMaxRefPage(tableLength);
        }
    }


    /**
     * delete the passed key from the R tree
     *
     * @throws DBAppException
     */
    public boolean delete(CustomPolygon key, RTreeInnerNode<CustomPolygon> parent, int ptr) throws DBAppException {
        for (int i = 0; i < numberOfKeys; ++i)
            if (keys[i].compareTo(key) == 0) {
                this.deleteAt(i);
                if (i == 0 && ptr > 0) {
                    parent.setKey(ptr - 1, this.getFirstKey());
                }
                if (!this.isRoot() && numberOfKeys < this.minKeys()) {
                    if (borrow(parent, ptr))
                        return true;
                    merge(parent, ptr);
                }
                return true;
            }
        return false;
    }


    public boolean delete(CustomPolygon key, RTreeInnerNode<CustomPolygon> parent, int ptr, String pageName) throws DBAppException {
        for (int i = 0; i < numberOfKeys; ++i)
            if (keys[i].compareTo(key) == 0) {
                if (recordsReference[i] instanceof Ref)
                    this.deleteAt(i);
                else {
                    OverflowRef overflowRef = (OverflowRef) recordsReference[i];
                    overflowRef.deleteRef(pageName);
                    if (overflowRef.getTotalSize() == 1) {
                        OverflowPage firstPageName = overflowRef.deserializeOverflowPage(overflowRef.getFirstPageName());
                        Ref ref = firstPageName.getRefs().firstElement();
                        recordsReference[i] = ref;
                    }
                }

                if (i == 0 && ptr > 0) {
                    parent.setKey(ptr - 1, this.getFirstKey());
                }
                if (!this.isRoot() && numberOfKeys < this.minKeys()) {
                    if (borrow(parent, ptr)) {
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
     *
     * @param index the index of the key to be deleted
     */
    public void deleteAt(int index) {
        for (int i = index; i < numberOfKeys - 1; ++i) {
            keys[i] = keys[i + 1];
            recordsReference[i] = recordsReference[i + 1];
        }
        numberOfKeys--;
    }


    /**
     * tries to borrow a key from the left or right sibling
     *
     * @param parent the parent of the current node
     * @param ptr    the index of the parent pointer that points to this node
     * @return true if borrow is done successfully and false otherwise
     * @throws DBAppException
     */
    public boolean borrow(RTreeInnerNode<CustomPolygon> parent, int ptr) throws DBAppException {
        // left sibling
        if (ptr > 0) {
            RTreeLeafNode<CustomPolygon> leftSibling = (RTreeLeafNode<CustomPolygon>) parent.getChild(ptr - 1);
            if (leftSibling.numberOfKeys > leftSibling.minKeys()) {
                this.insertAt(0, leftSibling.getLastKey(), leftSibling.getLastRecord());
                leftSibling.deleteAt(leftSibling.numberOfKeys - 1);
                parent.setKey(ptr - 1, keys[0]);
                leftSibling.serializeNode();
                return true;
            }
        }

        // right sibling
        if (ptr < parent.numberOfKeys) {
            RTreeLeafNode<CustomPolygon> rightSibling = (RTreeLeafNode<CustomPolygon>) parent.getChild(ptr + 1);
            if (rightSibling.numberOfKeys > rightSibling.minKeys()) {
                this.insertAt(numberOfKeys, rightSibling.getFirstKey(), rightSibling.getFirstRecord());
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
     *
     * @param parent the parent of the current node
     * @param ptr    the index of the parent pointer that points to this node
     * @throws DBAppException
     */
    public void merge(RTreeInnerNode<CustomPolygon> parent, int ptr) throws DBAppException {
        if (ptr > 0) {
            //merge with left
            RTreeLeafNode<CustomPolygon> leftSibling = (RTreeLeafNode<CustomPolygon>) parent.getChild(ptr - 1);
            leftSibling.merge(this);
            parent.deleteAt(ptr - 1);
            leftSibling.serializeNode();
        } else {
            //merge with right
            RTreeLeafNode<CustomPolygon> rightSibling = (RTreeLeafNode<CustomPolygon>) parent.getChild(ptr + 1);
            this.merge(rightSibling);
            parent.deleteAt(ptr);
            rightSibling.serializeNode();
        }
    }


    /**
     * merge the current node with the specified node. The foreign node will be deleted
     *
     * @param foreignNode the node to be merged with the current node
     * @throws DBAppException
     */
    public void merge(RTreeLeafNode<CustomPolygon> foreignNode) throws DBAppException {
        for (int i = 0; i < foreignNode.numberOfKeys; ++i)
            this.insertAt(numberOfKeys, foreignNode.getKey(i), foreignNode.getRecord(i));

        this.setNextNodeName(foreignNode.getNextNodeName());
    }


    public String toString() {
        StringBuilder stringBuilder = new StringBuilder("(" + index + ")");

        stringBuilder.append("[");
        for (int i = 0; i < order; i++) {
            String key = " ";
            if (i < numberOfKeys) {
                key = keys[i].toString();

                if (recordsReference[i] instanceof Ref) {
                    key += "," + recordsReference[i];
                } else {
                    key += "," + ((OverflowRef) recordsReference[i]).getFirstPageName();
                    if (pagesToPrint == null)
                        pagesToPrint = new ArrayList<>();

                    pagesToPrint.add((OverflowRef) recordsReference[i]);
                }

            }
            stringBuilder.append(key);
            if (i < order - 1)
                stringBuilder.append("|");
        }
        stringBuilder.append("]");
        return stringBuilder.toString();
    }


    public ArrayList<GeneralRef> searchMTE(CustomPolygon key) throws DBAppException {
        ArrayList<GeneralRef> res = new ArrayList<GeneralRef>();
        searchMTE(key, res);
        return res;
    }


    public ArrayList<GeneralRef> searchMT(CustomPolygon key) throws DBAppException {
        ArrayList<GeneralRef> res = new ArrayList<GeneralRef>();
        searchMT(key, res);
        return res;
    }


    public void searchMTE(CustomPolygon key, ArrayList<GeneralRef> res) throws DBAppException {
        int i = 0;
        for (; i < numberOfKeys; ++i) {
            if (this.getKey(i).compareTo(key) >= 0)
                res.add(this.getRecord(i));
        }
        if (nextNode != null) {
            RTreeLeafNode nxt = (RTreeLeafNode) deserializeNode(nextNode);
            nxt.searchMTE(key, res);
        }

    }


    public void searchMT(CustomPolygon key, ArrayList<GeneralRef> res) throws DBAppException {
        for (int i = 0; i < numberOfKeys; ++i)
            if (this.getKey(i).compareTo(key) > 0)
                res.add(this.getRecord(i));

        if (nextNode != null) {
            RTreeLeafNode<CustomPolygon> nextLeafNode = (RTreeLeafNode<CustomPolygon>) deserializeNode(nextNode);
            nextLeafNode.searchMT(key, res);
        }
    }


    public RTreeLeafNode searchForUpdateRef(CustomPolygon key) {
        return this;
    }


    public void updateRef(String oldPage, String newPage, CustomPolygon key) throws DBAppException {
        GeneralRef generalRef;

        for (int i = 0; i < numberOfKeys; ++i)
            if (this.getKey(i).compareTo(key) == 0) {
                generalRef = getRecord(i);
                generalRef.updateRef(oldPage, newPage);

                if (generalRef instanceof Ref) {
                    this.serializeNode();
                }
                return;
            }
    }
}
