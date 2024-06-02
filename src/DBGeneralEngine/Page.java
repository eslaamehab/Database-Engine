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
     */
    public Page(String pageName) {
        tuples = new Vector<Tuple>();
        this.pageName = pageName;
    }


    /**
     * Getters & Setters
     */
    public Vector getVector() {
        return vector;
    }

    public void setVector(Vector vector) {
        this.vector = vector;
    }

    public Vector<Tuple> getTuples() {
        return tuples;
    }

    public void setTuples(Vector<Tuple> tuples) {
        this.tuples = tuples;
    }

    public String getPageName() {
        return pageName;
    }

    public void setPageName(String pageName) {
        this.pageName = pageName;
    }


    /**
     * Rest of the functions
     */
    public int size() {
        return tuples.size();
    }

    public int binarySearch(Comparable key, int pos) {
        int result = binarySearchLastOccurrence(key, pos);
        return (result == -1) ? ((binarySearchFirstGreater(key, pos) == -1) ? tuples.size() : binarySearchFirstGreater(key, pos)) : result;
    }

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


    public void serialize(Page page, String address) {
        try {
            FileOutputStream fileOut = new FileOutputStream(address);
            ObjectOutputStream out = new ObjectOutputStream(fileOut);
            out.writeObject(page);
            out.close();
            fileOut.close();
        } catch (IOException i) {
            i.printStackTrace();
            System.out.println("IO Exception above");
        }
    }

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

    public void serialize() throws DBAppException {
        try {
            FileOutputStream fileOut = new FileOutputStream("data/" + pageName);
            ObjectOutputStream out = new ObjectOutputStream(fileOut);
            out.writeObject(this);
            out.close();
            fileOut.close();
        } catch (IOException e) {
            e.printStackTrace();
            throw new DBAppException("IO Exception in Page: " + pageName);
        }
    }

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

    public String toString() {
        StringBuilder stringBuilder = new StringBuilder();
        for (Tuple tuple : tuples) {
            stringBuilder.append(tuple.toString());
            stringBuilder.append("\n");
        }
        return stringBuilder + "\n";
    }

}