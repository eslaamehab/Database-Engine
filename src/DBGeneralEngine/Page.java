package src.DBGeneralEngine;

import src.Ref.GeneralRef;
import src.Ref.OverflowRef;

import java.io.*;
import java.sql.Ref;
import java.util.ArrayList;
import java.util.Hashtable;
import java.util.Set;
import java.util.Vector;

public class Page implements Serializable {

    /**
     * Attributes
     */
    private Vector vector = new Vector();
    private Vector<Tuple> tuples;
    private String pageName;


    /**
     * Constructor
     * Initializes a new Page object with the given page name.
     * It also initializes the tuples field as an empty Vector to hold the records; aka tuples associated with the page.
     *
     * @param pageName the name of the page to be created
     */
    public Page(String pageName) {
        tuples = new Vector<Tuple>();
        this.pageName = pageName;
    }


    /**
     * Getters & Setters
     * <p>
     * <p>
     *
     * Returns the vector associated with the page.
     *
     * @return the Vector object associated with the page
     */
    public Vector getVector() {
        return vector;
    }


    /**
     * Sets the vector associated with the page to the given Vector.
     *
     * @param vector the new Vector to be set for the page
     */
    public void setVector(Vector vector) {
        this.vector = vector;
    }


    /**
     * Returns the Vector of tuples (records) stored in the page.
     *
     * @return the Vector of Tuple objects associated with the page
     */
    public Vector<Tuple> getTuples() {
        return tuples;
    }


    /**
     * Sets the Vector of tuples (records) for the page to the specified Vector.
     *
     * @param tuples the new Vector of Tuple objects to be set for the page
     */
    public void setTuples(Vector<Tuple> tuples) {
        this.tuples = tuples;
    }


    /**
     * Returns the name of the page.
     *
     * @return the name of the page
     */
    public String getPageName() {
        return pageName;
    }


    /**
     * Sets the name of the page to the specified string.
     *
     * @param pageName the new name to be set for the page
     */
    public void setPageName(String pageName) {
        this.pageName = pageName;
    }


    /**
     * Returns the number of tuples (records) stored in the page.
     *
     * @return the size of the tuples Vector
     */
    public int size() {
        return tuples.size();
    }


    /**
     * Performs a binary search on the tuples to find the position of the specified key.
     * It first attempts to find the last occurrence of the key. If not found, it checks for the first greater key.
     *
     * @param key the Comparable key to search for
     * @param pos the position of the attribute in the tuple to compare
     *
     * @return the index of the last occurrence of the key, or the index of the first greater key, or the size of the tuples if none found
     */
    public int binarySearch(Comparable key, int pos) {
        int result = binarySearchLastOccurrence(key, pos);
        return (result == -1) ? ((binarySearchFirstGreater(key, pos) == -1) ? tuples.size() : binarySearchFirstGreater(key, pos)) : result;
    }


    /**
     * Performs a binary search to find the last occurrence of the specified key in the tuples.
     *
     * @param key the Comparable key to search for
     * @param pos the position of the attribute in the tuple to compare
     *
     * @return the index of the last occurrence of the key, or -1 if not found
     */
    public int binarySearchLastOccurrence(Comparable key, int pos) {
        int result = -1;
        int low = 0;
        int high = tuples.size() - 1;
        int mid;
        while (low <= high) {
            mid = low + (high - low + 1) / 2;
            Comparable currentValue = (Comparable) tuples.get(mid).getAttributes().get(pos);
            if (currentValue.compareTo(key) < 0) {
                low = mid + 1;
            } else if (currentValue.compareTo(key) == 0) {
                low = mid + 1;
                result = mid;
            } else if (currentValue.compareTo(mid) > 0) {
                high = mid - 1;
            }
        }
        return result;
    }


    /**
     * Performs a binary search to find the first occurrence of a key that is greater than the specified key.
     *
     * @param key the Comparable key to search for
     * @param pos the position of the attribute in the tuple to compare
     *
     * @return the index of the first key that is greater than the specified key, or -1 if not found
     */
    public int binarySearchFirstGreater(Comparable key, int pos) {
        int result = -1;
        int low = 0;
        int high = tuples.size() - 1;
        int mid;
        while (low <= high) {
            mid = low + (high - low + 1) / 2;
            Comparable currentValue = (Comparable) tuples.get(mid).getAttributes().get(pos);
            if (currentValue.compareTo(key) <= 0) {
                low = mid + 1;
            } else if (currentValue.compareTo(mid) > 0) {
                result = mid;
                high = mid - 1;
            }
        }
        return result;
    }


    /**
     * Serializes a given Page object to a specified file address.
     *
     * @param page the Page object to be serialized
     * @param address the file path where the serialized Page object will be stored
     * @throws DBAppException if an IOException occurs during the serialization process
     */
    public void serialize(Page page, String address) throws DBAppException {
        try {
            FileOutputStream fileOut = new FileOutputStream(address);
            ObjectOutputStream out = new ObjectOutputStream(fileOut);
            out.writeObject(page);
            out.close();
            fileOut.close();
        } catch (IOException e) {
            e.printStackTrace();
            throw new DBAppException("IO Exception in Page: " + pageName);
        }
    }


    /**
     * Deserializes a Page object from a specified file address.
     *
     * @param address the file path from which the Page object will be deserialized
     */
    public void deserialize(String address) {
        try {
            FileInputStream fileIn = new FileInputStream(address);
            ObjectInputStream stream = new ObjectInputStream(fileIn);
            Page page = (Page) stream.readObject();
            stream.close();
            fileIn.close();
        } catch (IOException i) {
            i.printStackTrace();
            System.out.println("IO Exception above");
        } catch (ClassNotFoundException c) {
            System.out.println("class not found below");
            c.printStackTrace();
        }
    }


    /**
     * Inserts a Tuple object into the page, maintaining the order based on the specified attribute.
     * The method compares the new tuple's key with existing tuples to find the correct insertion point.
     *
     * @param x the Tuple object to be inserted into the page
     * @param pos the position of the attribute used for comparison
     */
    public void insertIntoPage(Tuple x, int pos) {
        Comparable nKey = (Comparable) x.getAttributes().get(pos);

        for (int i = 0; i < tuples.size(); i++) {
            if (nKey.compareTo(tuples.get(i).getAttributes().get(pos)) < 0) {
                tuples.insertElementAt(x, i);
                return;
            }
        }
        tuples.insertElementAt(x, tuples.size());
    }


    /**
     * Deletes tuples from the page that match the specified criteria defined in the hashtable.
     * The method checks each tuple against the values in the hashtable and removes matching tuples.
     *
     * @param hashtableColumnNameValue a Hashtable containing column name-value pairs to match against
     * @param attributeIndex a Vector containing the indices of the attributes to check for matches
     */
    public void deleteInPage(Hashtable<String, Object> hashtableColumnNameValue, Vector<Integer> attributeIndex) {

        for (int i = 0; i < tuples.size(); i++) {
            Vector x = tuples.get(i).getAttributes();
            Set<String> keys = hashtableColumnNameValue.keySet();
            int j = 0;
            for (String key : keys) {
                if (j == attributeIndex.size()) {
                    break;
                }
                if (!x.get(attributeIndex.get(j)).equals(hashtableColumnNameValue.get(key))) {
                    break;
                }
                j++;
            }
            if (j == attributeIndex.size()) {
                tuples.remove(i);
                i--;
            }
        }
    }


    /**
     * Deletes tuples from the page based on the specified conditions and attributes.
     * The method checks for a specific clustering key and deletes references from associated TreeIndex objects.
     *
     * @param metaOfTable metadata of the table containing attributes
     * @param orgPos the original position of the clustering key
     * @param clusteringKey the name of the clustering key attribute
     * @param colNameTreeIndex a Hashtable mapping column names to their TreeIndex objects
     * @param hashtableColumnNameValue a Hashtable containing the values for the columns to be matched
     * @param allIndices a list of all indices associated with the table
     * @param isCluster boolean indicating if the operation is cluster-based
     * @throws DBAppException if an error occurs during the deletion process
     */
    public void deleteInPageForRef(Vector<String[]> metaOfTable,
                                   int orgPos,
                                   String clusteringKey,
                                   Hashtable<String, TreeIndex> colNameTreeIndex,
                                   Hashtable<String, Object> hashtableColumnNameValue,
                                   ArrayList<String> allIndices,
                                   boolean isCluster) throws DBAppException {
        int n = 0;
        int lastOccurrence = tuples.size();
        if (isCluster) {
            lastOccurrence = binarySearchLastOccurrence((Comparable) hashtableColumnNameValue.get(clusteringKey), orgPos) + 1;
            for (n = lastOccurrence - 1; n >= 0 && ((Comparable) tuples.get(n).getAttributes().get(orgPos))
                    .compareTo(hashtableColumnNameValue.get(clusteringKey)) == 0; n--)
                ;
            n++;
        }

        ArrayList<String> arrayList = new ArrayList<>();
        for (String[] strings : metaOfTable) {
            arrayList.add(strings[1]);
        }
        for (int k = n; k <= Math.min(tuples.size() - 1, lastOccurrence); k++) {
            Tuple tuple = tuples.get(k);
            if (validDelete(arrayList, hashtableColumnNameValue, tuple)) {
                for (int i = 0; i < tuples.get(k).getAttributes().size() - 2; i++) {
                    for (String allIndex : allIndices) {
                        if (allIndex.equals(arrayList.get(i))) {
                            TreeIndex tree = colNameTreeIndex.get(allIndex);
                            GeneralRef generalRef = tree.search((Comparable) tuple.getAttributes().get(i));
                            if (generalRef instanceof Ref) {
                                tree.delete((Comparable) tuples.get(k).getAttributes().get(i));
                            } else {
                                if (generalRef instanceof OverflowRef) {
                                    {
                                        tree.delete((Comparable) tuple.getAttributes().get(i), this.pageName);
                                    }
                                }
                            }
                        }
                    }
                }
                tuples.remove(k);
                k--;
            }
        }
    }


    /**
     * Deletes tuples from the page using binary search based on the specified clustering key.
     * The method searches for the last occurrence of the key and removes matching tuples.
     *
     * @param hashtableColumnNameValue a Hashtable containing the values for the columns to be matched
     * @param metaOfTable metadata of the table containing attributes
     * @param orgPos the original position of the clustering key
     * @param clusteringKey the name of the clustering key attribute
     */
    public void deleteInPageWithBinarySearch(Hashtable<String, Object> hashtableColumnNameValue,
                                             Vector<String[]> metaOfTable,
                                             int orgPos,
                                             String clusteringKey) {

        int n = binarySearchLastOccurrence((Comparable) hashtableColumnNameValue.get(clusteringKey), orgPos);
        ArrayList<String> arrayList = new ArrayList<>();
        for (String[] strings : metaOfTable) {
            arrayList.add(strings[1]);
        }
        for (int i = n; i >= 0; i--) {
            Tuple tuple = tuples.get(i);
            if (tuple.getAttributes().get(orgPos).equals(hashtableColumnNameValue.get(clusteringKey))) {
                if (validDelete(arrayList, hashtableColumnNameValue, tuple)) {
                    tuples.remove(i);
                    i++;
                }
            }
        }
    }


    /**
     * Validates if a tuple can be deleted based on the specified conditions in the hashtable.
     * The method checks if the attributes of the tuple match the values in the hashtable.
     *
     * @param x an ArrayList containing attribute names to check against
     * @param hashtableColumnNameValue a Hashtable containing the values for the columns to be matched
     * @param t the Tuple object to validate for deletion
     *
     * @return true if the tuple matches the criteria for deletion, false otherwise
     */
    public boolean validDelete(ArrayList<String> x, Hashtable<String, Object> hashtableColumnNameValue, Tuple t) {
        Set<String> keys = hashtableColumnNameValue.keySet();
        ArrayList<String> arrayList = new ArrayList<>(keys);
        for (String s : arrayList) {
            for (int j = 0; j < x.size(); j++) {
                if (s.equals(x.get(j))) {
                    if (!(hashtableColumnNameValue.get(s).equals(t.getAttributes().get(j)))) {
                        return false;
                    }
                }
            }
        }
        return true;
    }


    /**
     * Converts the tuples in the page to a string representation.
     * Each tuple's string representation is appended to a StringBuilder, separated by new lines.
     *
     * @return a string representation of all tuples in the page
     */
    public String toString() {
        StringBuilder stringBuilder = new StringBuilder();
        for (Tuple tuple : tuples) {
            stringBuilder.append(tuple.toString());
            stringBuilder.append("\n");
        }
        return stringBuilder + "\n";
    }

}