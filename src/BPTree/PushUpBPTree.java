package src.BPTree;


/**
 * This class is used for pushing keys up to the inner nodes in case of splitting at a lower level in a B+ Tree data structure.
 * <p>
 * It is used to encapsulate/insert a new node into the tree and updating the parent nodes accordingly.
 */
public class PushUpBPTree<T extends Comparable<T>> {

    /**
     * Attributes
     * <p>
     * <p>
     * newNode  -> the new node to be inserted into the tree.
     * key      -> the key value associated with the new node.
     */
    BPTreeNode<T> newNode;
    Comparable<T> key;


    /**
     * Constructor
     * Initializes the PushUpBPTree object with the given newNode and key.
     *
     * @param newNode the new node to be inserted into the tree
     * @param key     the key value associated with the new node
     */
    public PushUpBPTree(BPTreeNode<T> newNode, Comparable<T> key) {
        this.newNode = newNode;
        this.key = key;
    }
}
