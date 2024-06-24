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


/**
 * Abstract class representing a node in a B+ Tree.
 * It collects the common functionalities of the inner and leaf nodes.
 *
 * @param <T> The type of elements maintained by this tree, which must be comparable.
 */
public abstract class BPTreeNode<T extends Comparable<T>> implements Serializable {


    /**
     * Attributes
     * <p>
     * index        -> The unique index of this node.
     * nextIndex    -> The next index to be assigned to a new node. This is a static field shared across all instances.
     * keys         -> Array to hold the keys in the node.
     * numberOfKeys -> The number of keys currently stored in the node.
     * order        -> The order of the B+ Tree, determining the maximum number of children each node can have.
     * isRoot       -> Indicates whether this node is the root of the tree.
     * nodeName     -> The name of this node, used for serialization and identification.
     */
    private int index;
    private static int nextIndex = 0;
    private Comparable<T>[] keys;
    private int numberOfKeys;
    private int order;
    private boolean isRoot;
    private String nodeName;


    /**
     * Constructor
     * Initializes a new B+Tree node with the given order.
     *
     * @param order The order of the B+Tree, which determines the maximum number of keys and child pointers the node can hold.
     * @throws DBAppException If there is an error during the initialization of the B+Tree node.
     */
    public BPTreeNode(int order) throws DBAppException {
        this.order = order;
        numberOfKeys = 0;
        index = nextIndex++;
        nodeName = "Node" + getFromMetaDataTree();
    }


    /**
     * Getters & Setters
     */
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

    public Comparable<T> getLastKey() {
        return keys[numberOfKeys - 1];
    }

    public Comparable<T> getFirstKey() {
        return keys[0];
    }

    public void setRoot(boolean isRoot) {
        this.isRoot = isRoot;
    }

    public Comparable<T> getKey(int index) {
        return keys[index];
    }

    public void setKey(int index, Comparable<T> key) {
        keys[index] = key;
    }


    /**
     * @return Boolean indicating whether this node is the root of the B+ tree
     */
    public boolean isRoot() {
        return isRoot;
    }

    /**
     * @return Boolean indicating whether this node is full or not
     */
    public boolean isFull() {
        return numberOfKeys == order;
    }


    /**
     * Reads the contents of a file at the specified path and returns a Vector of String[] representing the lines of the file.
     *
     * @param path the path to the file to be read
     * @return a Vector of String[] representing the lines of the file
     * @throws DBAppException with a custom error message & prints the stack trace if an IOException occurs during the file reading process.
     */
    public static Vector readFile(String path) throws DBAppException {
        try {
            String currentLine = "";
            FileReader fileReader = new FileReader(path);
            BufferedReader bufferedReader = new BufferedReader(fileReader);
            Vector metadata = new Vector();
            while ((currentLine = bufferedReader.readLine()) != null) {
                metadata.add(currentLine.split(","));
            }
            return metadata;
        } catch (IOException e) {
            e.printStackTrace();
            throw new DBAppException("IO Exception reading from path: " + path);
        }
    }

    /**
     * Retrieves the last value from the metadata file and updates it by incrementing it by 1.
     *
     * @return the last value from the metadata file
     * @throws DBAppException with a custom error message & prints the stack trace if an IOException occurs during the file reading or writing process
     */
    protected String getFromMetaDataTree() throws DBAppException {
        try {

            String lastIn = "";
            Vector meta = readFile("data/metadata.csv");
            int newLastIn = 0;
            for (Object obj : meta) {
                String[] currentIn = (String[]) obj;
                lastIn = currentIn[0];
                newLastIn = Integer.parseInt(currentIn[0]) + 1;
                currentIn[0] = newLastIn + "";
                break;
            }
            FileWriter fileWriter = new FileWriter("data/metadata.csv");
            for (Object obj : meta) {
                String[] currentIn = (String[]) obj;
                fileWriter.append(currentIn[0]);
                break;
            }
            fileWriter.flush();
            fileWriter.close();
            return lastIn;
        } catch (IOException e) {
            e.printStackTrace();
            throw new DBAppException("IOException reading from metadata.csv ");
        }
    }


    /**
     * Serializes the current BPTreeNode object to a file on disk.
     * The file is named "data: [nodeName].class", where [nodeName] is the name of the node.
     *
     * @throws DBAppException if an IOException occurs during the file operations
     */
    public void serializeNode() throws DBAppException {
        try {
            FileOutputStream fileOutputStream = new FileOutputStream("data: " + this.nodeName + ".class");
            ObjectOutputStream out = new ObjectOutputStream(fileOutputStream);
            out.writeObject(this);
            out.close();
            fileOutputStream.close();
        } catch (IOException e) {
            e.printStackTrace();
            throw new DBAppException("IO Exception writing to disk: " + this.nodeName);
        }
    }

    /**
     * Deserializes a BPTreeNode object from a file on disk.
     * The file is named "data: [name].class", where [name] is the name of the node.
     *
     * @param name the name of the node to be deserialized
     * @return the deserialized BPTreeNode object
     * @throws DBAppException if an IOException or ClassNotFoundException occurs during the file operations
     */
    public BPTreeNode<T> deserializeNode(String name) throws DBAppException {
        try {
            FileInputStream fileInputStream = new FileInputStream("data: " + name + ".class");
            ObjectInputStream objectInputStream = new ObjectInputStream(fileInputStream);
            BPTreeNode<T> BPTreeNode = (BPTreeNode<T>) objectInputStream.readObject();
            objectInputStream.close();
            fileInputStream.close();
            return BPTreeNode;
        } catch (IOException e) {
            e.printStackTrace();
            throw new DBAppException("IO Exception reading from disk: " + name);
        } catch (ClassNotFoundException e) {
            e.printStackTrace();
            throw new DBAppException("Class Not Found Exception above");
        }
    }


    /**
     * Abstract methods
     * <p>
     * <p>
     * Returns the minimum number of keys this node can hold.
     *
     * @return The minimum number of keys this node can hold.
     */
    public abstract int minKeys();

    /**
     * Inserts a key-value pair into the B+Tree.
     *
     * @param key    The key to be inserted.
     * @param ref    The reference to be associated with the key.
     * @param parent The parent node of the current node.
     * @param i      The index of the parent pointer that points to the current node.
     * @return The updated B+Tree root after the insertion.
     * @throws DBAppException If there is an error during the insertion.
     */
    public abstract PushUpBPTree<T> insert(T key, Ref ref, BPTreeInnerNode<T> parent, int i) throws DBAppException;

    /**
     * Searches for a key in the B+Tree.
     *
     * @param key The key to be searched for.
     * @return The reference associated with the key, or null if the key is not found.
     * @throws DBAppException If there is an error during the search.
     */
    public abstract GeneralRef search(T key) throws DBAppException;

    /**
     * Searches for a key in the metadata tree.
     *
     * @param key The key to be searched for.
     * @return An ArrayList of references associated with the key.
     * @throws DBAppException If there is an error during the search.
     */
    public abstract ArrayList<GeneralRef> searchMT(T key) throws DBAppException;

    /**
     * Searches for a key in the metadata tree with extended search.
     *
     * @param key The key to be searched for.
     * @return An ArrayList of references associated with the key.
     * @throws DBAppException If there is an error during the search.
     */
    public abstract ArrayList<GeneralRef> searchMTE(T key) throws DBAppException;

    /**
     * Searches for the appropriate reference for insertion.
     *
     * @param key         The key to be inserted.
     * @param tableLength The length of the table.
     * @return The appropriate reference for insertion.
     * @throws DBAppException If there is an error during the search.
     */
    public abstract Ref searchForInsertion(T key, int tableLength) throws DBAppException;

    /**
     * Deletes a key from the B+Tree recursively.
     *
     * @param key    The key to be deleted.
     * @param parent The parent of the current node.
     * @param i      The index of the parent pointer that points to the current node.
     * @return True if the key was successfully deleted, false otherwise.
     * @throws DBAppException If there is an error during the deletion.
     */
    public abstract boolean delete(T key, BPTreeInnerNode<T> parent, int i) throws DBAppException;

    /**
     * Deletes a key from the B+Tree recursively, with a specified page name.
     *
     * @param key      The key to be deleted.
     * @param parent   The parent of the current node.
     * @param i        The index of the parent pointer that points to the current node.
     * @param pageName The name of the page.
     * @return True if the key was successfully deleted, false otherwise.
     * @throws DBAppException If there is an error during the deletion.
     */
    public abstract boolean delete(T key, BPTreeInnerNode<T> parent, int i, String pageName) throws DBAppException;

    /**
     * Searches for a leaf node containing the specified key.
     *
     * @param key The key to be searched for.
     * @return The leaf node containing the specified key.
     * @throws DBAppException If there is an error during the search.
     */
    public abstract BPTreeLeafNode searchForUpdateRef(T key) throws DBAppException;


    /**
     * A string representation of the object in the following format:   (index)[key1|key2|...|keyN]
     * Which can be useful for debugging, logging, or displaying the object's state.
     *
     * @return the string representation of the object
     */
    public String toString() {
        StringBuilder stringBuilder = new StringBuilder("(" + index + ")");

        stringBuilder.append("[");
        for (int i = 0; i < order; i++) {
            String key = " ";
            if (i < numberOfKeys)
                key = keys[i].toString();

            stringBuilder.append(key);
            if (i < order - 1)
                stringBuilder.append("|");
        }
        stringBuilder.append("]");
        return stringBuilder.toString();
    }

}

