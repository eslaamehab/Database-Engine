package src.RPTree;

import src.DBGeneralEngine.DBAppException;
import src.DBGeneralEngine.TreeIndex;
import src.Ref.GeneralRef;
import src.Ref.OverflowRef;
import src.Ref.Ref;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.LinkedList;
import java.util.Queue;

public class RTree<CustomPolygon extends Comparable<CustomPolygon>> implements Serializable, TreeIndex<CustomPolygon> {


    /**
     attributes
     */

    private final int order;
    private RTreeNode<CustomPolygon> root;


    /**
     * Creates an empty R tree
     * @param order the maximum number of keys in the nodes of the tree
     * @throws DBAppException
     */
    public RTree(int order) throws DBAppException
    {

        this.order = order;
        root = new RTreeLeafNode<>(this.order);
        root.setRoot(true);
    }


    public void updateRef(String oldPage,String newPage,CustomPolygon key) throws DBAppException{

        RTreeLeafNode leaf = searchForUpdateRef(key);
        leaf.updateRef(oldPage,newPage,key);

        leaf.serializeNode();
    }


    public RTreeLeafNode searchForUpdateRef(CustomPolygon key) throws DBAppException{
        return root.searchForUpdateRef(key);
    }


    /**
     * Inserts the specified key associated with the given record in the R tree
     * @param key the key to be inserted
     * @param ref the reference of the record associated with the key
     * @throws DBAppException
     */
    public void insert(CustomPolygon key, Ref ref) throws DBAppException
    {
        PushUp<CustomPolygon> pushUp = root.insert(key, ref, null, -1);
        if(pushUp != null)
        {
            RTreeInnerNode<CustomPolygon> newRoot = new RTreeInnerNode<>(order);

            newRoot.insertLeftAt(0, pushUp.key, root);
            newRoot.setChild(1, pushUp.newNode);
            root.setRoot(false);
            root.serializeNode();
            root = newRoot;
            root.setRoot(true);
        }
    }


    /**
     * Looks up for the record that is associated with the specified key
     * @param key the key to find its record
     * @return the reference of the record associated with this key
     * @throws DBAppException
     */
    public GeneralRef search(CustomPolygon key) throws DBAppException
    {
        return root.search(key);
    }


    /**
     * Delete a key and its associated record from the tree.
     * @param key the key to be deleted
     * @return a boolean to indicate whether the key is successfully deleted or it was not in the tree
     * @throws DBAppException
     */
    public boolean delete(CustomPolygon key) throws DBAppException
    {
        boolean done = root.delete(key, null, -1);

        while(root instanceof RTreeInnerNode && !root.isRoot())
            root = ((RTreeInnerNode<CustomPolygon>) root).getFirstChild();
        return done;
    }


    /**
     * Delete 1 Ref(either Ref/or single Ref inside an overflow page)
     *  only not the key with its pointer
     * @param key the key to be deleted
     * @return a boolean to indicate whether the key is successfully deleted or it
     *         was not in the tree
     * @throws DBAppException
     */
    public boolean delete(CustomPolygon key, String Page_name) throws DBAppException{
        boolean done = root.delete(key, null, -1,Page_name);

        while(root instanceof RTreeInnerNode && !root.isRoot())
            root = ((RTreeInnerNode<CustomPolygon>) root).getFirstChild();
        return done;
    }


    public String toString()
    {

        StringBuilder stringBuilder = new StringBuilder();
        RTreeLeafNode.pagesToPrint = new ArrayList<OverflowRef>();

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

                    for(int i = 0; i <= parent.numberOfKeys; ++i)
                    {
                        try {
                            stringBuilder.append(parent.getChild(i).index).append(",");
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



    public Ref searchForInsertion(CustomPolygon key,int tableLength) throws DBAppException {
        return root.searchForInsertion(key, tableLength);
    }


    public RTreeLeafNode getLeftmostLeaf() throws DBAppException {
        RTreeNode<CustomPolygon> currentNode = root;

        while(!(currentNode instanceof RTreeLeafNode)) {
            RTreeInnerNode rTreeInnerNode = (RTreeInnerNode) currentNode;
            currentNode = rTreeInnerNode.getFirstChild();
        }

        return (RTreeLeafNode) currentNode;
    }


    @Override
    public ArrayList<GeneralRef> searchMTE(CustomPolygon key) throws DBAppException {
        return root.searchMTE(key);
    }


    @Override
    public ArrayList<GeneralRef> searchMT(CustomPolygon key) throws DBAppException {
        return root.searchMT(key);
    }

}

