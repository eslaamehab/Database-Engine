package src.RTree;

import src.DBGeneralEngine.DBAppException;
import src.Ref.GeneralRef;
import src.Ref.Ref;

import java.io.*;
import java.util.ArrayList;
import java.util.Vector;

/**
 * Abstract class to collects the functionalities of both inner and leaf nodes
 */
public abstract class RTreeNode<CustomPolygon extends Comparable<CustomPolygon>> implements Serializable {


    /**
     * Attributes
     */
    private Comparable<CustomPolygon>[] keys;

    private static int nextIndex = 0;
    private int numberOfKeys;
    private int order;
    private int index;

    private boolean isRoot;

    private String nodeName;



    /**
     * Getters & Setters
     */
    public Comparable<CustomPolygon>[] getKeys() {
        return keys;
    }

    public void setKeys(Comparable<CustomPolygon>[] keys) {
        this.keys = keys;
    }

    public int getOrder() {
        return order;
    }

    public void setOrder(int order) {
        this.order = order;
    }

    public int getIndex() {
        return index;
    }

    public void setIndex(int index) {
        this.index = index;
    }

    public static int getNextIndex() {
        return nextIndex;
    }

    public static void setNextIndex(int nextIndex) {
        RTreeNode.nextIndex = nextIndex;
    }

    public String getNodeName() {
        return nodeName;
    }

    public void setNodeName(String nodeName) {
        this.nodeName = nodeName;
    }

    public int getNumberOfKeys() {
        return numberOfKeys;
    }

    public void setNumberOfKeys(int numberOfKeys) {
        this.numberOfKeys = numberOfKeys;
    }


    public void setRoot(boolean isRoot)
    {
        this.isRoot = isRoot;
    }


    /**
     * Finds the key at the given index
     * @param index the index at which the key is located
     * @return the key which is located at the specified index
     */
    public Comparable<CustomPolygon> getKey(int index)
    {
        return keys[index];
    }

    /**
     * Sets given the key at the given index
     * @param index the index of the key to be set
     * @param key the new value for the key
     */
    public void setKey(int index, Comparable<CustomPolygon> key)
    {
        keys[index] = key;
    }


    /**
     * @return the last key in this node
     */
    public Comparable<CustomPolygon> getLastKey()
    {
        return keys[numberOfKeys-1];
    }


    /**
     * @return the first key in this node
     */
    public Comparable<CustomPolygon> getFirstKey()
    {
        return keys[0];
    }


    /**
     * @return the minimum number of keys this node should hold
     */
    public abstract int getMinKeys();



    /**
     * Constructor
     */

    public RTreeNode(int order) throws DBAppException
    {
        index = nextIndex++;
        numberOfKeys = 0;
        this.order = order;
        nodeName=getFromMetaDataTree();
    }


    public static Vector readFile(String path) throws DBAppException
    {
        try
        {
            String currentLine = "";
            FileReader fileReader = new FileReader(path);
            BufferedReader bufferedReader = new BufferedReader(fileReader);
            Vector metadata = new Vector();

            while ((currentLine = bufferedReader.readLine()) != null) {
                metadata.add(currentLine.split(","));
            }
            return metadata;
        }
        catch(IOException e) {
            e.printStackTrace();
            throw new DBAppException("IO Exception reading file from: "+path);
        }
    }


    protected String getFromMetaDataTree() throws DBAppException
    {
        try {

            String path = "data/metadata.csv" ;
            String lastIn = "";
            Vector meta;
            meta = readFile(path);
            int overrideLastin = 0;

            for (Object obj : meta) {
                String[] current = (String[]) obj;
                lastIn = current[0];
                overrideLastin = Integer.parseInt(current[0])+1;
                current[0] = overrideLastin + "";
                break;
            }

            String fileName = "data/genMetadata.csv";
            FileWriter csvWriter;
            csvWriter = new FileWriter(fileName);
            for (Object obj : meta)
            {
                String[] curr = (String[]) obj;
                csvWriter.append(curr[0]);
                break;
            }
            csvWriter.flush();
            csvWriter.close();
            return lastIn;
        }
        catch (IOException e) {
            e.printStackTrace();
            throw new DBAppException("IOException reading file data! ");
        }
    }


    public boolean isRoot()
    {
        return isRoot;
    }

    public boolean isFull()
    {
        return numberOfKeys == order;
    }



    /**
     * Inserts the given key with the given record reference
     * @param key the key to be inserted
     * @param recordReference a pointer to the record
     * @param parent the parent of the current node
     * @param ptr the index of the parent pointer that points to this node
     * @return a key and a new node in case of a node splitting and null otherwise
     */
    public abstract PushUpRTree<CustomPolygon> insert(CustomPolygon key, Ref recordReference, RTreeInnerNode<CustomPolygon> parent, int ptr) throws DBAppException;


    public abstract GeneralRef search(CustomPolygon key) throws DBAppException;


    public abstract ArrayList<GeneralRef> searchMT(CustomPolygon key)throws DBAppException;


    public abstract ArrayList<GeneralRef> searchMTE(CustomPolygon key)throws DBAppException;


    public abstract Ref searchForInsertion(CustomPolygon key,int tableLength)throws DBAppException;


    /**
     * Deletes the given key from the Rtree
     * @param key the key to be deleted from the R tree
     * @param parent the parent of the current node
     * @param ptr the index of the parent pointer that points to this node
     * @return true if the node was deleted or false otherwise
     */
    public abstract boolean delete(CustomPolygon key, RTreeInnerNode<CustomPolygon> parent, int ptr) throws DBAppException;

    public abstract boolean delete(CustomPolygon key, RTreeInnerNode<CustomPolygon> parent, int ptr,String pageName) throws DBAppException;


    public String toString()
    {
        StringBuilder stringBuilder = new StringBuilder("(" + index + ")");

        stringBuilder.append("[");
        for (int i = 0; i < order; i++)
        {
            String key = " ";
            if(i < numberOfKeys)
                key = keys[i].toString();

            stringBuilder.append(key);
            if(i < order - 1)
                stringBuilder.append("|");
        }
        stringBuilder.append("]");
        return stringBuilder.toString();
    }



    public void serializeNode() throws DBAppException
    {
        try
        {
            FileOutputStream fileOutputStream = new FileOutputStream("data: "+ this.nodeName);
            ObjectOutputStream objectOutputStream = new ObjectOutputStream(fileOutputStream);
            objectOutputStream.writeObject(this);
            objectOutputStream.close();
            fileOutputStream.close();
        }
        catch(IOException e)
        {
            e.printStackTrace();
            throw new DBAppException("IO Exception writing: " + this.nodeName );
        }
    }


    public RTreeNode<CustomPolygon> deserializeNode(String name) throws DBAppException {
        try {
            FileInputStream fileInputStream = new FileInputStream("data: "+ name);
            ObjectInputStream objectInputStream = new ObjectInputStream(fileInputStream);
            RTreeNode<CustomPolygon> RTreeNode =   (RTreeNode<CustomPolygon>) objectInputStream.readObject();
            objectInputStream.close();
            fileInputStream.close();
            return RTreeNode;
        }
        catch(IOException e) {
            throw new DBAppException("IO Exception loading node: " + name );
        }
        catch(ClassNotFoundException e) {
            throw new DBAppException("Class Not Found Exception");
        }
    }


    public abstract RTreeLeafNode searchForUpdateRef(CustomPolygon key) throws DBAppException;


    public boolean deleteFile() {
        File f = new File("data/"+nodeName+".class");
        return f.delete();
    }

}