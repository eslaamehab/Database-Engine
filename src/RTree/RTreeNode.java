package src.RTree;

import src.DBGeneralEngine.DBAppException;
import src.Ref.GeneralRef;
import src.Ref.Ref;

import java.io.*;
import java.util.ArrayList;
import java.util.Vector;

/**
 * Abstract class to collect the functionalities of both inner and leaf nodes in an R-Tree.
 * This class provides the common properties and behaviors for all R-Tree nodes.
 *
 * @param <CustomPolygon> The type of custom polygon objects stored in the R-Tree nodes.
 *                        This type must implement the Comparable interface.
 */
public abstract class RTreeNode<CustomPolygon extends Comparable<CustomPolygon>> implements Serializable {


    /**
     * Attributes
     * <p>
     * <p>
     * keys         ->  Array to hold the "CustomPolygon" keys in the node, hence should implement the Comparable interface.
     * nextIndex    ->  Static field shared across all instances, which holds the next index to be assigned to a new node.
     * numberOfKeys ->  The number of keys currently stored in the node.
     * order        ->  The order of the R-Tree, indicating the maximum number of children each node can have.
     * index        ->  The unique index of this node.
     * isRoot       ->  Boolean indicating whether this node is the root of the tree.
     * nodeName     ->  The name of this node, used for serialization and identification.
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
     * <p>
     * <p>
     * <p>
     * <p>
     * Gets the array of keys stored in this node.
     *
     * @return the array of keys.
     */
    public Comparable<CustomPolygon>[] getKeys() {
        return keys;
    }

    /**
     * Sets the array of keys for this node.
     *
     * @param keys the array of keys to set.
     */
    public void setKeys(Comparable<CustomPolygon>[] keys) {
        this.keys = keys;
    }

    /**
     * Gets the order of the R-Tree.
     *
     * @return the order of the R-Tree.
     */
    public int getOrder() {
        return order;
    }

    /**
     * Sets the order of the R-Tree.
     *
     * @param order the order to set.
     */
    public void setOrder(int order) {
        this.order = order;
    }

    /**
     * Gets the unique index of this node.
     *
     * @return the index of this node.
     */
    public int getIndex() {
        return index;
    }

    /**
     * Sets the unique index of this node.
     *
     * @param index the index to set.
     */
    public void setIndex(int index) {
        this.index = index;
    }

    /**
     * Gets the next index to be assigned to a new node.
     * STATIC field shared across all instances.
     *
     * @return the next index to be assigned.
     */
    public static int getNextIndex() {
        return nextIndex;
    }

    /**
     * Sets the next index to be assigned to a new node.
     * STATIC field shared across all instances.
     *
     * @param nextIndex the next index to set.
     */
    public static void setNextIndex(int nextIndex) {
        RTreeNode.nextIndex = nextIndex;
    }

    /**
     * Gets the name of this node.
     *
     * @return the name of this node.
     */
    public String getNodeName() {
        return nodeName;
    }

    /**
     * Sets the name of this node.
     *
     * @param nodeName the name to set.
     */
    public void setNodeName(String nodeName) {
        this.nodeName = nodeName;
    }

    /**
     * Gets the number of keys currently stored in this node.
     *
     * @return the number of keys.
     */
    public int getNumberOfKeys() {
        return numberOfKeys;
    }

    /**
     * Sets the number of keys for this node.
     *
     * @param numberOfKeys the number of keys to set.
     */
    public void setNumberOfKeys(int numberOfKeys) {
        this.numberOfKeys = numberOfKeys;
    }

    /**
     * Sets whether this node is the root of the tree.
     *
     * @param isRoot true if this node is the root, false otherwise.
     */
    public void setRoot(boolean isRoot) {
        this.isRoot = isRoot;
    }


    /**
     * Finds the key at the given index.
     *
     * @param index the index at which the key is located.
     * @return the key which is located at the given index.
     */
    public Comparable<CustomPolygon> getKey(int index) {
        return keys[index];
    }

    /**
     * Sets the key at the given index.
     *
     * @param index the index of the key to be set.
     * @param key   the new value for the key.
     */
    public void setKey(int index, Comparable<CustomPolygon> key) {
        keys[index] = key;
    }

    /**
     * Gets the last key in this node.
     *
     * @return the last key in this node.
     */
    public Comparable<CustomPolygon> getLastKey() {
        return keys[numberOfKeys - 1];
    }


    /**
     * Gets the first key in this node.
     *
     * @return the first key in this node.
     */
    public Comparable<CustomPolygon> getFirstKey() {
        return keys[0];
    }


    /**
     * Gets the minimum number of keys this node should hold.
     *
     * @return the minimum number of keys.
     */
    public abstract int getMinKeys();


    /**
     * Constructs a new RTreeNode with the given order.
     * Initializes the node with an index, order, and a unique node name.
     *
     * @param order The order of the R-Tree.
     * @throws DBAppException if there is an error generating the node name.
     */
    public RTreeNode(int order) throws DBAppException {
        index = nextIndex++;
        numberOfKeys = 0;
        this.order = order;
        nodeName = getFromMetaDataTree();
    }


    /**
     * Reads the metadata file and returns its contents as a Vector.
     *
     * @param path The path to the metadata file.
     * @return A Vector containing the metadata.
     * @throws DBAppException if there is an IO error reading the file.
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
            throw new DBAppException("IO Exception reading file from: " + path);
        }
    }


    /**
     * Generates a unique node name from the metadata file and updates the metadata.
     *
     * @return The generated node name.
     * @throws DBAppException if there is an IO error reading or writing the metadata file.
     */
    public String getFromMetaDataTree() throws DBAppException {
        try {

            String path = "data/metadata.csv";
            String lastIn = "";
            Vector meta;
            meta = readFile(path);
            int overrideLastin = 0;

            for (Object obj : meta) {
                String[] current = (String[]) obj;
                lastIn = current[0];
                overrideLastin = Integer.parseInt(current[0]) + 1;
                current[0] = overrideLastin + "";
                break;
            }

            String fileName = "data/genMetadata.csv";
            FileWriter csvWriter;
            csvWriter = new FileWriter(fileName);
            for (Object obj : meta) {
                String[] curr = (String[]) obj;
                csvWriter.append(curr[0]);
                break;
            }
            csvWriter.flush();
            csvWriter.close();
            return lastIn;
        } catch (IOException e) {
            e.printStackTrace();
            throw new DBAppException("IOException reading file data! ");
        }
    }


    /**
     * Checks if this node is the root of the tree.
     *
     * @return true if this node is the root, false otherwise.
     */
    public boolean isRoot() {
        return isRoot;
    }


    /**
     * Checks if this node is full ( has the maximum number of keys ).
     *
     * @return true if this node is full, false otherwise.
     */
    public boolean isFull() {
        return numberOfKeys == order;
    }


    /**
     * Inserts the given key with the given record reference.
     *
     * @param key             the key to be inserted.
     * @param recordReference a pointer to the record.
     * @param parent          the parent of the current node.
     * @param ptr             the index of the parent pointer that points to this node.
     * @return a key and a new node in case of a node splitting and null otherwise.
     * @throws DBAppException if there is an error during insertion.
     */
    public abstract PushUpRTree<CustomPolygon> insert(CustomPolygon key, Ref recordReference, RTreeInnerNode<CustomPolygon> parent, int ptr) throws DBAppException;


    /**
     * Searches for the given key in the node.
     *
     * @param key the key to search for.
     * @return the reference to the record if found, null otherwise.
     * @throws DBAppException if there is an error during the search.
     */
    public abstract GeneralRef search(CustomPolygon key) throws DBAppException;


    /**
     * Searches for all keys that match the given key in the node.
     *
     * @param key the key to search for.
     * @return a list of references to the records that match the key.
     * @throws DBAppException if there is an error during the search.
     */
    public abstract ArrayList<GeneralRef> searchMT(CustomPolygon key) throws DBAppException;


    /**
     * Searches for all keys that match or exceed the given key in the node.
     *
     * @param key the key to search for.
     * @return a list of references to the records that match or exceed the key.
     * @throws DBAppException if there is an error during the search.
     */
    public abstract ArrayList<GeneralRef> searchMTE(CustomPolygon key) throws DBAppException;


    /**
     * Searches for the appropriate position to insert the given key in the node.
     *
     * @param key         the key to search for.
     * @param tableLength the length of the table where the key will be inserted.
     * @return the reference to the position where the key can be inserted.
     * @throws DBAppException if there is an error during the search.
     */
    public abstract Ref searchForInsertion(CustomPolygon key, int tableLength) throws DBAppException;


    /**
     * Searches for the leaf node that should be updated with the given key.
     *
     * @param key the key to search for.
     * @return the leaf node that contains or should contain the key.
     * @throws DBAppException if there is an error during the search.
     */
    public abstract RTreeLeafNode searchForUpdateRef(CustomPolygon key) throws DBAppException;


    /**
     * Deletes the given key from the R-Tree.
     * If the key is found in this node, it is removed.
     * If the node becomes underflowed after deletion, it attempts to borrow from siblings or merge nodes.
     *
     * @param key    the key to be deleted from the R-Tree.
     * @param parent the parent of the current node.
     * @param ptr    the index of the parent pointer that points to this node.
     * @return true if the key was successfully deleted, false otherwise.
     * @throws DBAppException if there is an error during deletion.
     */
    public abstract boolean delete(CustomPolygon key, RTreeInnerNode<CustomPolygon> parent, int ptr) throws DBAppException;


    /**
     * Deletes the given key from the R-Tree, considering the specific page name.
     * This method handles scenarios where keys are part of overflow pages.
     *
     * @param key      the key to be deleted from the R-Tree.
     * @param parent   the parent of the current node.
     * @param ptr      the index of the parent pointer that points to this node.
     * @param pageName the name of the page where the key is stored.
     * @return true if the key was successfully deleted, false otherwise.
     * @throws DBAppException if there is an error during deletion.
     */
    public abstract boolean delete(CustomPolygon key, RTreeInnerNode<CustomPolygon> parent, int ptr, String pageName) throws DBAppException;


    /**
     * Deletes the file associated with this node.
     * The file is identified by the node's unique name.
     *
     * @return true if the file was successfully deleted, false otherwise.
     */
    public boolean deleteFile() {
        File f = new File("data/" + nodeName + ".class");
        return f.delete();
    }


    /**
     * Serializes this node to a file.
     * The node is written to a file with a name based on its unique identifier.
     *
     * @throws DBAppException if there is an error during serialization.
     */
    public void serializeNode() throws DBAppException {
        try {
            FileOutputStream fileOutputStream = new FileOutputStream("data: " + this.nodeName);
            ObjectOutputStream objectOutputStream = new ObjectOutputStream(fileOutputStream);
            objectOutputStream.writeObject(this);
            objectOutputStream.close();
            fileOutputStream.close();
        } catch (IOException e) {
            e.printStackTrace();
            throw new DBAppException("IO Exception writing: " + this.nodeName);
        }
    }


    /**
     * Deserializes a node from a file.
     * The file is identified by the given name, and the node is reconstructed from the file.
     *
     * @param name the name of the file to deserialize the node from.
     * @return the deserialized R-Tree node.
     * @throws DBAppException if there is an error during deserialization.
     */
    public RTreeNode<CustomPolygon> deserializeNode(String name) throws DBAppException {
        try {
            FileInputStream fileInputStream = new FileInputStream("data: " + name);
            ObjectInputStream objectInputStream = new ObjectInputStream(fileInputStream);
            RTreeNode<CustomPolygon> RTreeNode = (RTreeNode<CustomPolygon>) objectInputStream.readObject();
            objectInputStream.close();
            fileInputStream.close();
            return RTreeNode;
        } catch (IOException e) {
            throw new DBAppException("IO Exception loading node: " + name);
        } catch (ClassNotFoundException e) {
            throw new DBAppException("Class Not Found Exception");
        }
    }


    /**
     * Provides a string representation of this node.
     * The string includes the node's index and its keys in order.
     *
     * @return a string representation of the node.
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