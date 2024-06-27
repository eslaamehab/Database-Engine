package src.RTree;

import src.DBGeneralEngine.DBAppException;
import src.DBGeneralEngine.TreeIndex;
import src.Ref.GeneralRef;
import src.Ref.OverflowRef;
import src.Ref.Ref;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.LinkedList;
import java.util.Queue;


/**
 * The `RTree` class is an implementation of the R-Tree data structure, a spatial index that is used to efficiently
 * Store and query data objects with spatial properties, such as two-dimensional geometric shapes or regions.
 *
 * @param <CustomPolygon> the type of the custom polygon objects stored in the R-Tree, which must implement the `Comparable` interface.
 */
public class RTree<CustomPolygon extends Comparable<CustomPolygon>> implements Serializable, TreeIndex<CustomPolygon> {


    /**
     * Attributes
     * <p>
     * order ->     the maximum number of keys (child nodes or data objects) that can be stored in each internal or leaf node of the R-Tree.
     * root ->      the root node of the R-Tree, which can be either an internal node or a leaf node.
     */

    private final int order;
    private RTreeNode<CustomPolygon> root;


    /**
     * Constructor
     * Initializes an empty R tree with the given order
     * It creates a new `RTreeLeafNode` instance and sets it as the root of the tree. The root node is marked as the root of the tree.
     *
     * @param order the maximum number of keys in the nodes that can be stored in each internal or leaf node of the R-Tree.
     * @throws DBAppException if the provided order is less than 3 or not a positive integer.
     */
    public RTree(int order) throws DBAppException
    {
        this.order = order;
        root = new RTreeLeafNode<>(this.order);
        root.setRoot(true);
    }


    /**
     Getters & Setters
     */
    public int getOrder() {
        return order;
    }

    public RTreeNode<CustomPolygon> getRoot() {
        return root;
    }

    public void setRoot(RTreeNode<CustomPolygon> root) {
        this.root = root;
    }


    /**
     * Retrieves the leftmost leaf node in the R-Tree.
     * This method is useful for operations like range queries or iteration over the data objects in the R-Tree.
     *
     * @return the leftmost leaf node in the R-Tree.
     * @throws DBAppException if an error occurs during the traversal, such as the R-Tree being empty.
     */
    public RTreeLeafNode getLeftmostLeaf() throws DBAppException {
        RTreeNode<CustomPolygon> currentNode = root;

        while(!(currentNode instanceof RTreeLeafNode)) {
            RTreeInnerNode rTreeInnerNode = (RTreeInnerNode) currentNode;
            currentNode = rTreeInnerNode.getFirstChild();
        }

        return (RTreeLeafNode) currentNode;
    }

    /**
     * Updates the reference for a given data object in the R-Tree.
     *
     * @param oldPage the old page reference to be updated.
     * @param newPage the new page reference to be associated with the data object.
     * @param key the data object (represented by a `CustomPolygon`) whose reference is to be updated.
     * @throws DBAppException if an error occurs during the update operation,
     * Such as the data object not being found in the R-Tree or an error serializing the modified leaf node.
     */
    public void updateRef(String oldPage, String newPage, CustomPolygon key) throws DBAppException{

        RTreeLeafNode leaf = searchForUpdateRef(key);
        leaf.updateRef(oldPage,newPage,key);

        leaf.serializeNode();
    }

    /**
     * Searches the R-Tree for the leaf node containing the given data object.
     *
     * @param key the data object (CustomPolygon) to search for.
     * @return the leaf node that contains the data object.
     * @throws DBAppException if the data object is not found in the R-Tree.
     */
    public RTreeLeafNode searchForUpdateRef(CustomPolygon key) throws DBAppException{
        return root.searchForUpdateRef(key);
    }

    /**
     * Searches the R-Tree for the appropriate leaf node to insert a new data object.
     *
     * @param key         the data object (CustomPolygon) to insert.
     * @param tableLength the maximum number of entries allowed in a leaf node.
     * @return the reference to the appropriate leaf node where the new data object should be inserted.
     * @throws DBAppException if an error occurs during the search.
     */
    public Ref searchForInsertion(CustomPolygon key,int tableLength) throws DBAppException {
        return root.searchForInsertion(key, tableLength);
    }

    /**
     * Performs a "Multi-Tuple Exact" search on the R-Tree.
     *
     * @param key the data object (CustomPolygon) to search for.
     * @return an `ArrayList<GeneralRef>` containing the references to the matching data objects.
     * @throws DBAppException if an error occurs during the search.
     */
    @Override
    public ArrayList<GeneralRef> searchMTE(CustomPolygon key) throws DBAppException {
        return root.searchMTE(key);
    }

    /**
     * Performs a "Multi-Tuple" search on the R-Tree.
     *
     * @param key the data object (CustomPolygon) to search for.
     * @return an `ArrayList<GeneralRef>` containing the references to the matching data objects.
     * @throws DBAppException if an error occurs during the search.
     */
    @Override
    public ArrayList<GeneralRef> searchMT(CustomPolygon key) throws DBAppException {
        return root.searchMT(key);
    }

    /**
     * Inserts the given key of a new data object (CustomPolygon) associated with the given record reference in the R-Tree.
     *
     * @param key the key of the new data object to be inserted.
     * @param ref the reference of the record associated with the key to be inserted.
     * @throws DBAppException if an error occurs during insertion.
     */
    public void insert(CustomPolygon key, Ref ref) throws DBAppException
    {
        PushUpRTree<CustomPolygon> pushUpRTree = root.insert(key, ref, null, -1);
        if(pushUpRTree != null)
        {
            RTreeInnerNode<CustomPolygon> newRoot = new RTreeInnerNode<>(order);

            newRoot.insertLeftAt(0, pushUpRTree.key, root);
            newRoot.setChild(1, pushUpRTree.newNode);
            root.setRoot(false);
            root.serializeNode();
            root = newRoot;
            root.setRoot(true);
        }
    }

    /**
     * Looks up for the record that is associated with the given key
     *
     * @param key the key (CustomPolygon) to search for its record
     * @return The `GeneralRef` object that corresponds to the given key, or null if the key is not found in the R-Tree.
     * @throws DBAppException If an error occurs during the search operation.
     */
    public GeneralRef search(CustomPolygon key) throws DBAppException
    {
        return root.search(key);
    }

    /**
     * Deletes the data object associated with the given `CustomPolygon` key from the R-Tree.
     *
     * @param key the key (CustomPolygon) of the data object to be deleted
     * @return `true` if the deletion was successful, `false` otherwise.
     * @throws DBAppException If an error occurs during the deletion operation.
     */
    public boolean delete(CustomPolygon key) throws DBAppException
    {
        boolean done = root.delete(key, null, -1);

        while(root instanceof RTreeInnerNode && !root.isRoot())
            root = ((RTreeInnerNode<CustomPolygon>) root).getFirstChild();
        return done;
    }

    /**
     * Deletes the single data object associated with the given (CustomPolygon) key and `pageName` from the R-Tree.
     *
     * @param key the key to be deleted
     * @param pageName The name of the page where the data object is stored.
     * @return `true` if the deletion was successful, `false` otherwise.
     * @throws DBAppException If an error occurs during the deletion operation.
     */
    public boolean delete(CustomPolygon key, String pageName) throws DBAppException{
        boolean done = root.delete(key, null, -1,pageName);

        while(root instanceof RTreeInnerNode && !root.isRoot())
            root = ((RTreeInnerNode<CustomPolygon>) root).getFirstChild();
        return done;
    }

    /**
     * Provides a string representation of the R-Tree data structure for debugging and visualization of the R-Tree data structure.
     * Where the output format is :-
     * Each node is represented on a new line, with leaf nodes indicated by an arrow (->) and inner nodes enclosed in curly braces {}.
     * The contents of each node (e.g., index values) are printed, separated by commas.
     * After the tree representation, a section is included that lists any overflow references, with each reference numbered and its contents printed.
     *
     * @return a string representation of the R-Tree
     */
    public String toString()
    {

        StringBuilder stringBuilder = new StringBuilder();
        RTreeLeafNode.pagesToPrint = new ArrayList<>();

        Queue<RTreeNode> currentNode = new LinkedList<>(), nextNode;
        currentNode.add(root);

        while(!currentNode.isEmpty())
        {
            nextNode = new LinkedList<>();

            while(!currentNode.isEmpty())
            {
                RTreeNode curNode = currentNode.remove();
                stringBuilder.append(curNode);
                if(curNode instanceof RTreeLeafNode)
                    stringBuilder.append("->");
                else
                {
                    stringBuilder.append("{");
                    RTreeInnerNode parent = (RTreeInnerNode) curNode;

                    for(int i = 0; i <= ((RTreeInnerNode<?>) parent).getNumberOfKeys(); ++i)
                    {
                        try {
                            stringBuilder.append(parent.getChild(i).getIndex()).append(",");
                        } catch (DBAppException e) {
                            e.printStackTrace();
                            throw new RuntimeException(e);
                        }
                        try {
                            nextNode.add(parent.getChild(i));
                        } catch (DBAppException e) {
                            e.printStackTrace();
                            throw new RuntimeException(e);
                        }
                    }
                    stringBuilder.append("}");
                }

            }
            stringBuilder.append("\n");
            currentNode = nextNode;
        }

        ArrayList<OverflowRef> print  = RTreeLeafNode.pagesToPrint;
        stringBuilder.append("\n The overflow reference # \n");

        for(int i=0;i<print.size();i++)
        {
            stringBuilder.append("reference #: ").append(i + 1).append(" is :\n");
            stringBuilder.append(print.get(i).toString());
        }
        return stringBuilder.toString();
    }

}

