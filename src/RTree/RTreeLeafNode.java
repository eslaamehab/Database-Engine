package src.RTree;

import src.DBGeneralEngine.LeafNode;
import src.DBGeneralEngine.OverflowPage;
import src.DBGeneralEngine.DBAppException;
import src.Ref.GeneralRef;
import src.Ref.OverflowRef;
import src.Ref.Ref;

import java.io.Serializable;
import java.util.ArrayList;

public class RTreeLeafNode<CustomPolygon extends Comparable<CustomPolygon>> extends RTreeNode<CustomPolygon> implements Serializable, LeafNode<CustomPolygon> {

    /**
     * Attributes
     */
    private final GeneralRef[] recordsReference;
    private String nextNode;
    public static ArrayList<OverflowRef> pagesToPrint;


    /**
     * Constructor
     */
    @SuppressWarnings("unchecked")
    public RTreeLeafNode(int n) throws DBAppException {
        super(n);
        setKeys(new Comparable[n]);
        recordsReference = new GeneralRef[n];

    }

    /**
     * Getters & Setters
     */
    public String getNextNodeName() throws DBAppException {
        return nextNode;
    }

    /**
     * Gets the minimum number of keys the current should hold
     */
    public int getMinKeys() {
        return this.isRoot() ? 1 : (getOrder() + 1) / 2;
    }


    /**
     * @param node the next leaf node
     */
    public void setNextNode(RTreeLeafNode<CustomPolygon> node) {
        this.nextNode = (node != null) ? node.getNodeName() : null;
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
     * Sets the given record reference at given index
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
        return recordsReference[getNumberOfKeys() - 1];
    }


    /**
     * @return the next leaf node
     */
    public RTreeLeafNode<CustomPolygon> getNextNode() throws DBAppException {
        return (nextNode == null) ? null : ((RTreeLeafNode) deserializeNode(nextNode));
    }


    /**
     * Inserts the specified key associated with a given record reference in the R tree
     * @param key specific key which is associated with the given record reference
     * @param recordReference the record reference which the given key is associated with
     * @param parent is the parent node
     * @param ptr is the pointer
     */
    public PushUpRTree<CustomPolygon> insert(CustomPolygon key,
                                             Ref recordReference,
                                             RTreeInnerNode<CustomPolygon> parent,
                                             int ptr) throws DBAppException {

        int index = 0;
        while (index < getNumberOfKeys() && getKey(index).compareTo(key) < 0)
            ++index;

        if (index < getNumberOfKeys() && getKey(index).compareTo(key) == 0) {
            GeneralRef ref = recordsReference[index];
            if (ref.isOverflow()) {
                OverflowRef overflowRef = (OverflowRef) ref;
                overflowRef.insert(recordReference);

            } else {
                OverflowRef overflowRef = new OverflowRef();
                OverflowPage overflowPage = new OverflowPage(getOrder());

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

            return new PushUpRTree<>(newNode, newKey);
        } else {
            this.insertAt(index, key, recordReference);
            return null;
        }
    }


    /**
     * Inserts at given index the given key associated with its record reference
     * @param index     the index at which the key will be inserted
     * @param key       the key to be inserted
     * @param recordPtr the pointer to the record associated with the key
     */
    private void insertAt(int index, Comparable<CustomPolygon> key, GeneralRef recordPtr) {
        for (int i = getNumberOfKeys() - 1; i >= index; --i) {
            this.setKey(i + 1, getKey(i));
            this.setRecord(i + 1, getRecord(i));
        }

        this.setKey(index, key);
        this.setRecord(index, recordPtr);
        setNumberOfKeys(getNumberOfKeys()+1);
    }


    /**
     * Splits the current node
     * @param key       the new key that caused the split
     * @param newKeyRef the reference of the new key
     * @return the new node that results from the split
     */
    public RTreeNode<CustomPolygon> split(CustomPolygon key, GeneralRef newKeyRef) throws DBAppException {
        int keyIndex = this.findIndex(key);

        // split nodes
        int midIndex = getNumberOfKeys() / 2;
        if ((getNumberOfKeys() & 1) == 1 && keyIndex > midIndex)
            ++midIndex;

        int totalKeys = getNumberOfKeys() + 1;

        // move keys to new node
        RTreeLeafNode<CustomPolygon> newNode = new RTreeLeafNode<>(getOrder());
        for (int i = midIndex; i < totalKeys - 1; ++i) {
            newNode.insertAt(i - midIndex, this.getKey(i), this.getRecord(i));
            setNumberOfKeys(getNumberOfKeys()-1);
        }

        //insert new key
        if (keyIndex < totalKeys / 2)
            this.insertAt(keyIndex, key, newKeyRef);
        else
            newNode.insertAt(keyIndex - midIndex, key, newKeyRef);

        newNode.setNextNodeName(this.getNextNodeName());
        this.setNextNodeName(newNode.getNodeName());

        return newNode;
    }


    /**
     * Finds the index of the given key
     * @param key the key to be checked for its location
     * @return the expected index of the key
     */
    public int findIndex(CustomPolygon key) {
        for (int i = 0; i < getNumberOfKeys(); ++i) {
            int compareKeys = getKey(i).compareTo(key);
            if (compareKeys > 0)
                return i;
        }
        return getNumberOfKeys();
    }


    /**
     * @return Either the record reference at the given key or null if it does not exist
     */
    @Override
    public GeneralRef search(CustomPolygon key) {
        for (int i = 0; i < getNumberOfKeys(); ++i)
            if (this.getKey(i).compareTo(key) == 0)
                return this.getRecord(i);
        return null;
    }


    public Ref searchForInsertion(CustomPolygon key, int tableLength) throws DBAppException {
        int i = 0;
        for (; i < getNumberOfKeys(); i++) {
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
     * Deletes the given key from the Rtree
     * @param key the key to be deleted
     * @param parent the parent of the key to be deleted which the pointer will point to
     * @param ptr the index of the parent pointer that points to this node
     */
    public boolean delete(CustomPolygon key, RTreeInnerNode<CustomPolygon> parent, int ptr) throws DBAppException {
        for (int i = 0; i < getNumberOfKeys(); ++i)
            if (getKeys()[i].compareTo(key) == 0) {
                this.deleteAt(i);
                if (i == 0 && ptr > 0) {
                    parent.setKey(ptr - 1, this.getFirstKey());
                }
                if (!this.isRoot() && getNumberOfKeys() < this.getMinKeys()) {
                    if (borrow(parent, ptr))
                        return true;
                    merge(parent, ptr);
                }
                return true;
            }
        return false;
    }


    public boolean delete(CustomPolygon key, RTreeInnerNode<CustomPolygon> parent, int ptr, String pageName) throws DBAppException {
        for (int i = 0; i < getNumberOfKeys(); ++i)
            if (getKeys()[i].compareTo(key) == 0) {
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
                if (!this.isRoot() && getNumberOfKeys() < this.getMinKeys()) {
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
     * Deletes the key from the node at the given index
     * @param index the index of the key to be deleted
     */
    public void deleteAt(int index) {
        for (int i = index; i < getNumberOfKeys() - 1; ++i) {
            setKeys(new Comparable[]{getKeys()[i + 1]});
            recordsReference[i] = recordsReference[i + 1];
        }
        setNumberOfKeys(getNumberOfKeys()-1);
    }


    /**
     * Borrows a key from either the left or right sibling
     *
     * @param parent the parent of the current node
     * @param ptr    the index of the parent pointer that points to this node
     * @return true if the key is borrowed from either node or false otherwise
     */
    public boolean borrow(RTreeInnerNode<CustomPolygon> parent, int ptr) throws DBAppException {
        // left sibling
        if (ptr > 0) {
            RTreeLeafNode<CustomPolygon> leftSibling = (RTreeLeafNode<CustomPolygon>) parent.getChild(ptr - 1);
            if (leftSibling.getNumberOfKeys() > leftSibling.getMinKeys()) {
                this.insertAt(0, leftSibling.getLastKey(), leftSibling.getLastRecord());
                leftSibling.deleteAt(leftSibling.getNumberOfKeys() - 1);
                parent.setKey(ptr - 1, getKeys()[0]);
                leftSibling.serializeNode();
                return true;
            }
        }

        // right sibling
        if (ptr < parent.getNumberOfKeys()) {
            RTreeLeafNode<CustomPolygon> rightSibling = (RTreeLeafNode<CustomPolygon>) parent.getChild(ptr + 1);
            if (rightSibling.getNumberOfKeys() > rightSibling.getMinKeys()) {
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
     * Merges the current node with either its left or right sibling
     * @param parent the parent of the current node
     * @param ptr    the index of the parent pointer that points to this node
     */
    public void merge(RTreeInnerNode<CustomPolygon> parent, int ptr) throws DBAppException {
        if (ptr > 0) {
            //merge with left
            RTreeLeafNode<CustomPolygon> leftSibling = (RTreeLeafNode<CustomPolygon>) parent.getChild(ptr - 1);
            leftSibling.merge(this);
            parent.deleteAt(ptr - 1);
            leftSibling.serializeNode();
        }
        else {
            //merge with right
            RTreeLeafNode<CustomPolygon> rightSibling = (RTreeLeafNode<CustomPolygon>) parent.getChild(ptr + 1);
            this.merge(rightSibling);
            parent.deleteAt(ptr);
            rightSibling.serializeNode();
        }
    }


    /**
     * Merges the current node with the given foreign node
     * @param foreignNode the node to be merged with the current node
     */
    public void merge(RTreeLeafNode<CustomPolygon> foreignNode) throws DBAppException {
        for (int i = 0; i < foreignNode.getNumberOfKeys(); ++i)
            this.insertAt(getNumberOfKeys(), foreignNode.getKey(i), foreignNode.getRecord(i));

        this.setNextNodeName(foreignNode.getNextNodeName());
    }


    public String toString() {
        StringBuilder stringBuilder = new StringBuilder("(" + getIndex() + ")");

        stringBuilder.append("[");
        for (int i = 0; i < getOrder(); i++) {
            String key = " ";
            if (i < getNumberOfKeys()) {
                key = getKeys()[i].toString();

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
            if (i < getOrder() - 1)
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
        for (; i < getNumberOfKeys(); ++i) {
            if (this.getKey(i).compareTo(key) >= 0)
                res.add(this.getRecord(i));
        }
        if (nextNode != null) {
            RTreeLeafNode nxt = (RTreeLeafNode) deserializeNode(nextNode);
            nxt.searchMTE(key, res);
        }

    }


    public void searchMT(CustomPolygon key, ArrayList<GeneralRef> res) throws DBAppException {
        for (int i = 0; i < getNumberOfKeys(); ++i)
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

        for (int i = 0; i < getNumberOfKeys(); ++i)
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
