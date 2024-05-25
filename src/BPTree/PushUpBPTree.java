package src.BPTree;

/**
 * This class is used for push keys up to the inner nodes in case
 * of splitting at a lower level
 */
public class PushUpBPTree<T extends Comparable<T>> {



    /**
     * Attributes
     */
    BPTreeNode<T> newNode;
    Comparable<T> key;


    /**
     * Constructor
     */
    public PushUpBPTree(BPTreeNode<T> newNode, Comparable<T> key)
    {
        this.newNode = newNode;
        this.key = key;
    }
}
