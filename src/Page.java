package src;

import java.io.*;
import java.sql.Ref;
import java.util.*;

public class Page implements Serializable {

    private Vector v = new Vector(); //vector of hashtables
    private Vector<Tuple> tuples;
    private String pageName;


    public void serialize(Page p, String address) throws IOException {
        try {
            FileOutputStream fileOut = new FileOutputStream(address);
            ObjectOutputStream out = new ObjectOutputStream(fileOut);
            out.writeObject(p);
            out.close();
            fileOut.close();
            //System.out.printf("Serialized data is saved in /tmp/employee.ser");
        } catch (IOException i) {
            i.printStackTrace();
        }
    }

    public void deserialize(Page p, String address){
        try {
            FileInputStream fileIn = new FileInputStream(address);
            ObjectInputStream in = new ObjectInputStream(fileIn);
            p = (Page) in.readObject();
            in.close();
            fileIn.close();
        } catch (IOException i) {
            System.out.println("IO");
            i.printStackTrace();
        } catch (ClassNotFoundException c) {
            System.out.println("class not found");
            c.printStackTrace();
        }
    }

//    private static final long serialVersionUID = -6080083008142779648L;

    // constructor
    public Page(String pageName) {
        tuples = new Vector<Tuple>();
        this.pageName = pageName;
    }

    // getter and setters
    public Vector getV() {
        return v;
    }
    public void setV(Vector v) {
        this.v = v;
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
            } else if(currentValue.compareTo(mid) > 0 ){
                result = mid;
                high = mid - 1;
            }
        }
        return result;
    }

    public void insertIntoPage(Tuple x, int pos) {
        Comparable nKey = (Comparable) x.getAttributes().get(pos);

        for(int i=0;i<tuples.size();i++){
            if(nKey.compareTo(tuples.get(i).getAttributes().get(pos))<0){
                tuples.insertElementAt(x, i);
                return;
            }
        }
        tuples.insertElementAt(x, tuples.size());
    }

    public void serialize() throws DBAppException {
        try {
            FileOutputStream fileOut = new FileOutputStream("data/" + pageName + ".class");
            ObjectOutputStream out = new ObjectOutputStream(fileOut);
            out.writeObject(this);
            out.close();
            fileOut.close();
        } catch (IOException e) {
            throw new DBAppException("IO Exception in Page: "+pageName);
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

    public void deleteInPageforRef(Vector<String[]> metaOfTable, int orgPos, String clusteringKey,
                                   Hashtable<String, TreeIndex> colNameTreeIndex, Hashtable<String, Object> hashtableColumnNameValue,
                                   ArrayList<String> allIndices, boolean isCluster) throws DBAppException{
        int index = 0;
        int lastOccurrence = tuples.size();
        if (isCluster) {
            lastOccurrence = binarySearchLastOccurrence((Comparable) hashtableColumnNameValue.get(clusteringKey), orgPos) + 1;
            for (index = lastOccurrence - 1; index >= 0 && ((Comparable)tuples.get(index).getAttributes().get(orgPos))
//					.equals(hashtableColumnNameValue.get(clusteringKey)); index--)
                    .compareTo(hashtableColumnNameValue.get(clusteringKey))==0; index--);
            index++;
        }

        ArrayList<String> x = new ArrayList<>();
        for (int i = 0; i < metaOfTable.size(); i++) {
            x.add(metaOfTable.get(i)[1]);
        }
        for (int k = index; k <= Math.min(tuples.size()-1, lastOccurrence); k++) {
            Tuple t = tuples.get(k);
            if (validDelete(x, hashtableColumnNameValue, t)) {
                for (int i = 0; i < tuples.get(k).getAttributes().size() - 2; i++) {
                    for (String allIndex : allIndices) {
                        if (allIndex.equals(x.get(i))) {
                            TreeIndex tree = colNameTreeIndex.get(allIndex);
                            GeneralReference generalReference = tree.search((Comparable) t.getAttributes().get(i));
                            if (generalReference instanceof Ref) {
                                tree.delete((Comparable) tuples.get(k).getAttributes().get(i));
                            } else {
                                if (generalReference instanceof OverflowReference) {
                                    OverflowReference overflowReference = (OverflowReference) generalReference;
                                    {
                                        tree.delete((Comparable) t.getAttributes().get(i), this.pageName);
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

    public void deleteInPageWithBinarySearch(Hashtable<String, Object> hashtableColumnNameValue, Vector<String[]> metaOfTable,
                                   String clusteringKeyValue, int orgPos, String clusteringKey) {

        int index = binarySearchLastOccurrence((Comparable) hashtableColumnNameValue.get(clusteringKey), orgPos);
        ArrayList<String> x = new ArrayList<>();
        for (int i = 0; i < metaOfTable.size(); i++) {
            x.add(metaOfTable.get(i)[1]);
        }
        for (int i = index; i >= 0; i--) {
            Tuple t = tuples.get(i);
            if (t.getAttributes().get(orgPos).equals(hashtableColumnNameValue.get(clusteringKey))) {
                if (validDelete(x, hashtableColumnNameValue, t)) {
                    tuples.remove(i);
                    i++;
                }
            }
        }
    }

    public boolean validDelete(ArrayList<String> x, Hashtable<String, Object> hashtableColumnNameValue, Tuple t) {
        Set<String> keys = hashtableColumnNameValue.keySet();
        ArrayList<String> y = new ArrayList<>();
        for (String key : keys) {
            y.add(key);
        }
        for (int i = 0; i < y.size(); i++) {
            for (int j = 0; j < x.size(); j++) {
                if (y.get(i).equals(x.get(j))) {
                    if (!(hashtableColumnNameValue.get(y.get(i)).equals(t.getAttributes().get(j)))) {
                        return false;
                    }
                }
            }
        }
        return true;
    }

    public String toString() {
        StringBuilder stringBuilder = new StringBuilder();
        for (Tuple obj : tuples) {
            stringBuilder.append(obj.toString());
            stringBuilder.append("\n");
        }
        return stringBuilder + "\n";
    }
}