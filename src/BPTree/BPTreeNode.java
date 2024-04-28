package src.BPTree;

import src.DBGeneralEngine.DBAppException;
import src.Ref.GeneralRef;
import src.Ref.Ref;

import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.Vector;


public abstract class BPTreeNode<T extends Comparable<T>> implements Serializable{

    /**
     * Abstract class that collects the common functionalities of the inner and leaf nodes
     */


    // attributes


    private int index;
    private static int nextIndex = 0;
    private Comparable<T>[] keys;
    private int numberOfKeys;
    private int order;
    private boolean isRoot;
    private String nodeName;



    public abstract PushUp<T> insert(T key, Ref ref, BPTreeInnerNode<T> parent, int i) throws DBAppException;
    public abstract GeneralRef search(T key) throws DBAppException;
    public abstract ArrayList<GeneralRef> searchMT(T key)throws DBAppException;
    public abstract ArrayList<GeneralRef> searchMTE(T key)throws DBAppException;
    public abstract Ref searchForInsertion(T key,int tableLength)throws DBAppException;


    public abstract boolean delete(T key, BPTreeInnerNode<T> parent, int i) throws DBAppException;
    public abstract boolean delete(T key, BPTreeInnerNode<T> parent, int i,String pageName) throws DBAppException;

    public abstract BPTreeLeafNode searchForUpdateRef(T key) throws DBAppException;


    // constructor

    public BPTreeNode(int order) throws DBAppException
    {
        this.order = order;
        numberOfKeys = 0;
        index = nextIndex++;
        nodeName = "Node" + getFromMetaDataTree();
    }


    // getters and setters

    public Comparable<T>[] getKeys() {
        return keys;
    }

    public void setKeys(Comparable<T>[] keys) {
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
        BPTreeNode.nextIndex = nextIndex;
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
            throw new DBAppException("IO Exception reading from path: "+path);
        }
    }

    protected String getFromMetaDataTree() throws DBAppException
    {
        try {

            String lastIn = "";
            Vector meta = readFile("data/metadata.csv");
            int newLastIn = 0;
            for (Object obj : meta) {
                String[] currentIn = (String[]) obj;
                lastIn = currentIn[0];
                newLastIn = Integer.parseInt(currentIn[0])+1;
                currentIn[0] = newLastIn + "";
                break;
            }
            FileWriter fileWriter = new FileWriter("data/metadata.csv");
            for (Object obj : meta)
            {
                String[] currentIn = (String[]) obj;
                fileWriter.append(currentIn[0]);
                break;
            }
            fileWriter.flush();
            fileWriter.close();
            return lastIn;
        }
        catch (IOException e) {
            e.printStackTrace();
            throw new DBAppException("IOException reading from metadata.csv ");
        }
    }




    /**
     * @return a boolean indicating whether this node is the root of the B+ tree
     */
    public boolean isRoot()
    {
        return isRoot;
    }

    /**
     * set this node to be a root or unset it if it is a root
     * @param isRoot the setting of the node
     */
    public void setRoot(boolean isRoot)
    {
        this.isRoot = isRoot;
    }

    /**
     * find the key at the specified index
     * @param index the index at which the key is located
     * @return the key which is located at the specified index
     */
    public Comparable<T> getKey(int index)
    {
        return keys[index];
    }

    /**
     * sets the value of the key at the specified index
     * @param index the index of the key to be set
     * @param key the new value for the key
     */
    public void setKey(int index, Comparable<T> key)
    {
        keys[index] = key;
    }

    /**
     * @return a boolean whether this node is full or not
     */
    public boolean isFull()
    {
        return numberOfKeys == order;
    }

    /**
     * @return the last key in this node
     */
    public Comparable<T> getLastKey()
    {
        return keys[numberOfKeys-1];
    }

    /**
     * @return the first key in this node
     */
    public Comparable<T> getFirstKey()
    {
        return keys[0];
    }

    /**
     * @return the minimum number of keys this node can hold
     */
    public abstract int minKeys();


    /**
     * delete a key from the B+ tree recursively
     * @param key the key to be deleted from the B+ tree
     * @param parent the parent of the current node
     * @param i the index of the parent pointer that points to this node
     * @return true if this node was successfully deleted and false otherwise
     * @throws DBAppException
     */


    /**
     * A string representation for the node
     */
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
            FileOutputStream fileOutputStream = new FileOutputStream("data: "+ this.nodeName+ ".class");
            ObjectOutputStream out = new ObjectOutputStream(fileOutputStream);
            out.writeObject(this);
            out.close();
            fileOutputStream.close();
        }
        catch(IOException e) {
            e.printStackTrace();
            throw new DBAppException("IO Exception writing to disk: " + this.nodeName );
        }

    }
    public BPTreeNode<T> deserializeNode(String name) throws DBAppException {
        try {
            FileInputStream fileInputStream = new FileInputStream("data: "+ name + ".class");
            ObjectInputStream objectInputStream = new ObjectInputStream(fileInputStream);
            BPTreeNode<T> BPTreeNode =   (BPTreeNode<T>) objectInputStream.readObject();
            objectInputStream.close();
            fileInputStream.close();
            return BPTreeNode;
        }
        catch(IOException e) {
            e.printStackTrace();
            throw new DBAppException("IO Exception reading from disk: " + name );
        }
        catch(ClassNotFoundException e) {
            e.printStackTrace();
            throw new DBAppException("Class Not Found Exception above");
        }

    }

}

