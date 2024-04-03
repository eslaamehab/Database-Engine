package src.APTree;


import java.awt.Polygon;
import java.io.*;
import java.util.*;

import src.DBGeneralEngine.DBAppException;
import src.Ref.Ref;
import src.Ref.GeneralRef;
import src.Ref.OverflowRef;
import src.DBGeneralEngine.SQLTerm;

//import BPTree.BPTree;
//import BPTree.BPTreeLeafNode;
//import General.GeneralReference;
//import General.LeafNode;
//import General.OverflowPage;
//import General.OverflowReference;
//import General.Ref;
//import General.TreeIndex;
//import RTree.RTree;


public class Table implements Serializable {

        private Vector<String> pages = new Vector<>();
        private int maxRowsInPage;
        private String tableName;
        private String clusteringKey;
        private int primaryPosition;
        private int lastId;
        private Hashtable<String, TreeIndex> colNameTreeIndex = new Hashtable<>();



    public int getMaxRowsInPage() {
        return maxRowsInPage;
    }

    public void setMaxRowsInPage(int maxRowsInPage) {
        this.maxRowsInPage = maxRowsInPage;
    }

    public void setPages(Vector<String> pages) {
        this.pages = pages;
    }

    public String getClusteringKey() {
        return clusteringKey;
    }

        public int getLastId(boolean b) {
            if (b)
                return lastId++;
            return lastId;
        }

        public Hashtable<String, TreeIndex> getColNameBTreeIndex() {
            return colNameTreeIndex;
        }


        public void printI() {
            for (String str : colNameTreeIndex.keySet()) {
                TreeIndex b = colNameTreeIndex.get(str);
            }
        }

        public Vector<String> getPages() {
            return pages;
        }

        public String getNewPageName() {
            return tableName + ((pages.size() == 0) ? 0
                    : Integer.parseInt((pages.get(pages.size() - 1)).substring(tableName.length())) + 1);

        }


        public Comparable getMax(int index) throws DBAppException
        {
            String pageName = pages.get(index);
            Page page = deserialize(pageName);
            Object obj = page.getTuples().get(page.size() - 1).getAttributes().get(primaryPosition);
            return (Comparable)obj;
        }

        public Comparable getMin(int index) throws DBAppException
        {
            String pageName = pages.get(index);
            Page page = deserialize(pageName);
            Comparable min = (Comparable)page.getTuples().get(0).getAttributes().get(primaryPosition);
            return min;
        }

        public void setClusteringKey(String clusteringKey) {
            this.clusteringKey = clusteringKey;
        }

        public Table() {

        }

        public void setTableName(String name) {
            tableName = name;
        }

        public void setPrimaryPosition(int pos) {
            primaryPosition = pos;
        }

        public int getPrimaryPosition() {
            return primaryPosition;
        }

        public String getTableName() {
            return tableName;
        }

        public static Page deserialize(String name) throws DBAppException {
            try {
                FileInputStream fileIn = new FileInputStream("data: " + name + ".class");
                ObjectInputStream in = new ObjectInputStream(fileIn);
                Page pageIn = (Page) in.readObject();
                in.close();
                fileIn.close();
                return pageIn;
            } catch (IOException e) {
                throw new DBAppException("IO Exception");
            } catch (ClassNotFoundException e) {
                throw new DBAppException("Class Not Found Exception");
            }
        }

        public Hashtable<Tuple, String> addInPage
                (int current, Tuple tuple, String keyType, String keyColName, int nodeSize,
                 boolean doInsert, Hashtable<Tuple, String> list) throws DBAppException{

            if (current < pages.size()) {
                String pageName = pages.get(current);

                Page p = deserialize(pageName);
                if (p.size() < maxRowsInPage) {
                    p.insertIntoPage(tuple, primaryPosition);
                    if (colNameTreeIndex.containsKey(keyColName) && doInsert) {
                        TreeIndex tree = colNameTreeIndex.get(keyColName);
                        Ref recordReference = new Ref(p.getPageName());
                        tree.insert((Comparable) tuple.getAttributes().get(primaryPosition), recordReference);
                    }
                    p.serialize();
                    return list;
                } else {
                    p.insertIntoPage(tuple, primaryPosition);
                    if (colNameTreeIndex.containsKey(keyColName) && doInsert) {
                        TreeIndex tree = colNameTreeIndex.get(keyColName);
                        Ref recordReference = new Ref(p.getPageName());
                        tree.insert((Comparable) tuple.getAttributes().get(primaryPosition), recordReference);
                    }
                    Tuple t = p.getTuples().remove(p.size() - 1);
                    if (colNameTreeIndex.containsKey(keyColName)) {

                        TreeIndex tree = colNameTreeIndex.get(keyColName);
                        String newPage = "";
                        if (current + 1 < pages.size()) {
                            newPage = pages.get(current + 1);
                        } else {
                            Page n = new Page(getNewPageName());
                            pages.addElement(n.getPageName());
                            newPage = n.getPageName();
                            Object keyValue = t.getAttributes().get(primaryPosition);
                            n.serialize();
                        }
                        tree.updateRef(p.getPageName(),newPage,(Comparable) t.getAttributes().get(primaryPosition));
                    }
                    list.put(t, p.getPageName());
                    Object min = p.getTuples().get(0).getAttributes().get(primaryPosition);
                    Object max = p.getTuples().get(p.size() - 1).getAttributes().get(primaryPosition);

                    p.serialize();
                    return addInPage(current + 1, t, keyType, keyColName, nodeSize, false, list);
                }
            } else {
                Page page = new Page(getNewPageName());
                page.insertIntoPage(tuple, primaryPosition);
                if (colNameTreeIndex.containsKey(keyColName)) {
                    TreeIndex tree = colNameTreeIndex.get(keyColName);
                    Ref recordReference = new Ref(page.getPageName());
                    tree.insert((Comparable) tuple.getAttributes().get(primaryPosition), recordReference);
                }
                Object keyValue = page.getTuples().get(0).getAttributes().get(primaryPosition);
                pages.addElement(page.getPageName());
                page.serialize();
                return list;
            }

        }


        public void insertSorted(Tuple tuple, Object keyV, String keyType, String keyColName, int nodeSize, ArrayList colNames)
                throws DBAppException{
            Hashtable<Tuple, String> list = new Hashtable<Tuple, String>();
            if (pages.size() == 0) {
                Page p = new Page(getNewPageName());
                p.insertIntoPage(tuple, primaryPosition);
                if (colNameTreeIndex.containsKey(keyColName)) {
                    TreeIndex tree = colNameTreeIndex.get(keyColName);
                    Ref recordReference = new Ref(p.getPageName());
                    tree.insert((Comparable) tuple.getAttributes().get(primaryPosition), recordReference);
                }
                pages.addElement(p.getPageName());
                p.serialize();

            } else {
                Comparable keyValue = (Comparable) keyV;
                if (colNameTreeIndex.containsKey(keyColName)) {
                    TreeIndex tree = colNameTreeIndex.get(keyColName);
                    Ref pageReference = tree.searchForInsertion(keyValue, tableName.length());
                    String pageName = "";
                    if (pageReference == null) {
                        pageName = pages.get(pages.size() - 1);
                    } else
                        pageName = pageReference.getPage();
                    int curr = pages.indexOf(pageName);
                    list = addInPage(curr, tuple, keyType, keyColName, nodeSize, true, list);
                } else {
                    int curr = 0;
                    for (curr = 0; curr < pages.size(); curr++) {
                        Object min = (getMin(curr));
                        Object max = getMax(curr);
                        if ((keyValue.compareTo(min) >= 0 && keyValue.compareTo(max) <= 0)
                                || (keyValue.compareTo(min) < 0) || curr == pages.size() - 1) {
                            list = addInPage(curr, tuple, keyType, keyColName, nodeSize, true, list);
                            break;
                        }
                    }
                }
            }
            Set<Tuple> st = list.keySet();
            Set<String> c = colNameTreeIndex.keySet();
            for (String s : c) {
                if (!keyColName.equals(s)) {
                    TreeIndex tree = colNameTreeIndex.get(s);
                    int index = 0;
                    for (; index < colNames.size(); index++) {
                        if (s.equals(colNames.get(index))) {
                            break;
                        }
                    }
                    Object keyValueOfNonCluster = tuple.getAttributes().get(index);
                    Ref pageReference = getClusterReference(keyV, keyColName);

                    tree.insert((Comparable) keyValueOfNonCluster, pageReference);

                    for (Tuple t : st) {
                        if (!t.equals(tuple)) {
                            Object keyValueOfNonClusterT = t.getAttributes().get(index);
                            tree.updateRef(list.get(t), pages.get(pages.indexOf(list.get(t))+1), (Comparable) keyValueOfNonClusterT);

                        }
                    }
                }
            }
        }


        private Ref getClusterReference(Object keyV, String keyColName) throws DBAppException {
            Comparable keyValue = (Comparable) keyV;
            Ref ref = null;
            if (colNameTreeIndex.contains(keyColName)) {
                TreeIndex tree = colNameTreeIndex.get(keyColName);
                GeneralRef generalRef = tree.search(keyValue);
                if (generalRef instanceof Ref) {
                    ref = (Ref) generalRef;
                } else {
                    OverflowRef overflowRef = (OverflowRef) generalRef;
                    ref = overflowRef.getLastRef();
                }
            } else {
                for (int i = pages.size() - 1; i >= 0; i--) {
                    if (keyValue.compareTo(getMin(i)) >= 0 && keyValue.compareTo(getMax(i)) <= 0) {
                        ref = new Ref(pages.get(i));
                        break;
                    }
                }
            }
            return ref;
        }

        @SuppressWarnings("unchecked")
        public void deleteInTable(Hashtable<String, Object> hashtableColumnNameValue, Vector<String[]> metaOfTable,
                                  String clusteringKey) throws DBAppException {

            ArrayList<String> indicesGiven = indicesIHave(hashtableColumnNameValue, colNameTreeIndex);
            ArrayList<String> allIndices = allTableIndices(colNameTreeIndex);

            if (!(indicesGiven.size() == 0)) {
                String selectedCol = (clusteringKey != null && clusteringKeyHasIndex(indicesGiven, clusteringKey))
                        ? clusteringKey
                        : indicesGiven.get(0);
                boolean isCluster = clusteringKey != null && selectedCol.equals(clusteringKey);

                TreeIndex tree = colNameTreeIndex.get(selectedCol);
                GeneralRef pageReference = tree.search((Comparable) hashtableColumnNameValue.get(selectedCol));
                if (pageReference == null) {
                    throw new DBAppException("Can not delete not found!");
                }
                if (pageReference instanceof Ref) {
                    Ref ref = (Ref) pageReference;
                    Page page = deserialize(ref.getPage() + "");
                    page.deleteInPageForRef(metaOfTable, primaryPosition, selectedCol, colNameTreeIndex, hashtableColumnNameValue,
                            allIndices, isCluster);
                    setMinMax(page);

                } else {
                    OverflowRef overflowRef = (OverflowRef) pageReference;
                    OverflowPage overflowPage = overflowRef.getFirstPage();
                    Set<Ref> allReferences = getRefFromBPTree(overflowPage);
                    for (Ref ref : allReferences) {
                        if (ref != null) {
                            Page page = deserialize(ref.getPage() + "");
                            page.deleteInPageForRef(metaOfTable, primaryPosition, selectedCol, colNameTreeIndex,
                                    hashtableColumnNameValue, allIndices, isCluster);
                            setMinMax(page);
                        }

                    }

                }

            } else if (clusteringKey != null) {

                for (int i = 0; i < pages.size(); i++) {
                    if (((Comparable) hashtableColumnNameValue.get(clusteringKey))
                            .compareTo(getMin(i)) < 0) {
                        break;

                    }
                    if (((Comparable) hashtableColumnNameValue.get(clusteringKey)).compareTo(((Comparable) getMin(i))) >= 0
                            && ((Comparable) hashtableColumnNameValue.get(clusteringKey))
                            .compareTo(((Comparable) getMax(i))) <= 0) {
                        Page page = deserialize(pages.get(i));
                        page.deleteInPageWithBinarySearch(hashtableColumnNameValue, metaOfTable, clusteringKey, primaryPosition,
                                clusteringKey);
                        if (page.getTuples().size() == 0) {
                            File file = new File("data: " + page.getPageName() + ".class");
                            file.delete();
                            pages.remove(i);
                            i--;

                        } else {
                            Object min = page.getTuples().get(0).getAttributes().get(primaryPosition);
                            Object max = page.getTuples().get(page.size() - 1).getAttributes().get(primaryPosition);
                            page.serialize();
                        }

                    }
                }
            } else {
                Vector<Integer> attributeIndex = getIntegers(hashtableColumnNameValue, metaOfTable);
                for (int i = 0; i < pages.size(); i++) {
                    String pageName = pages.get(i);
                    Page p = deserialize(pageName);
                    p.deleteInPage(hashtableColumnNameValue, attributeIndex);
                    if (p.getTuples().size() == 0) {
                        File f = new File("data: " + pageName + ".class");
                        f.delete();
                        pages.remove(i);
                        i--;

                    } else {
                        Object min = p.getTuples().get(0).getAttributes().get(primaryPosition);
                        Object max = p.getTuples().get(p.size() - 1).getAttributes().get(primaryPosition);
                        p.serialize();
                    }
                }
            }
        }

    private static Vector<Integer> getIntegers(Hashtable<String, Object> hashtableColumnNameValue, Vector<String[]> metaOfTable) {
        Vector<Integer> attributeIndex = new Vector<>();
        Set<String> keys = hashtableColumnNameValue.keySet();
        for (String key : keys) {
            int i;
            for (i = 0; i < metaOfTable.size(); i++) {
                if (metaOfTable.get(i)[1].equals(key)) {
                    break;
                }
            }
            attributeIndex.add(i);
        }
        return attributeIndex;
    }

    public Set<Ref> getRefFromBPTree(OverflowPage OFP) throws DBAppException {
            Set<Ref> allReferences = new HashSet<Ref>();
            Vector<Ref> xx = new Vector<Ref>();
            boolean notFound = true;
            boolean notFound1 = true;
            boolean first = true;
            for (int i = 0; i < OFP.getRefs().size(); i++) {
                if (first) {
                    xx.add(OFP.getRefs().get(0));
                    first = false;
                }
                for (int j = 0; j < xx.size(); j++) {
                    notFound = true;
                    if (OFP.getRefs().get(i).getPage().equals(xx.get(j).getPage())) {
                        notFound = false;
                        break;
                    } else {

                    }

                }
                if (notFound == true) {
                    xx.add(OFP.getRefs().get(i));
                }
            }
            OverflowPage nextOFP;
            boolean notNull = true;
            if (OFP.getNext() != null) {
                nextOFP = OFP.deserialize(OFP.getNext());
                while (notNull) {
                    for (int i = 0; i < nextOFP.getRefs().size(); i++) {
                        for (int j = 0; j < xx.size(); j++) {
                            notFound1 = true;
                            if (nextOFP.getRefs().get(i).getPage().equals(xx.get(j).getPage())) {
                                notFound1 = false;
                                break;
                            } else {

                            }

                        }
                        if (notFound1 == true) {
                            xx.add(nextOFP.getRefs().get(i));
                        }
                    }
                    if (nextOFP.getNext() != null) {
                        // nextOFP.serialize();
                        nextOFP = nextOFP.deserialize(nextOFP.getNext());
                    } else {
                        notNull = false;
                    }

                }
                // nextOFP.serialize();
            }
            allReferences.addAll(xx);
            // System.out.println(allReferences);
            return allReferences;

        }

        public boolean invalidDelete(Hashtable<String, Object> htblColNameValue, Vector<String[]> metaOfTable)
                throws DBAppException {
            Set<String> keys = htblColNameValue.keySet();
            for (String key : keys) {
                int i;
                for (i = 0; i < metaOfTable.size(); i++) {
                    if (metaOfTable.get(i)[1].equals(key)) {
                        try {
                            Class colType = Class.forName(metaOfTable.get(i)[2]);
                            Class parameterType = htblColNameValue.get(key).getClass();
                            Class polyOriginal = Class.forName("java.awt.Polygon");
                            if (colType == polyOriginal) {
                                Polygons p = new Polygons((Polygon) htblColNameValue.get(key));
                                htblColNameValue.put(key, p);
                            }
                            // System.out.println(colType + " " + parameterType);
                            if (!colType.equals(parameterType))
                                return true;
                            else
                                break;
                        } catch (ClassNotFoundException e) {
                            throw new DBAppException("Class Not Found Exception");
                        }
                    }
                }
                if (i == metaOfTable.size())
                    return true;
            }
            return false;
        }

        public void setMinMax(Page p) throws DBAppException {
            String pageName = p.getPageName();
            for (int i = 0; i < pages.size(); i++) {
                if (pages.get(i).equals(pageName)) {
                    if (p.getTuples().size() == 0) {
                        File f = new File("data/" + pageName + ".class");
//					System.out.println("/////||||\\\\\\\\\\\\\\\\\\deleting file " + pageName);
                        f.delete();
                        pages.remove(i);
                        //min.remove(i);
                        //max.remove(i);
                        i--;

                    } else {
                        Object minn = p.getTuples().get(0).getAttributes().get(primaryPos);
                        Object maxx = p.getTuples().get(p.size() - 1).getAttributes().get(primaryPos);
                        //min.setElementAt(minn, i);
                        //max.setElementAt(maxx, i);
                        p.serialize();
                    }
                }
            }
        }

        public ArrayList<String> indicesIHave(Hashtable<String, Object> htblColNameValue,
                                              Hashtable<String, TreeIndex> colNameTreeIndex) {
            ArrayList<String> columns = new ArrayList<String>();

            Set<String> keys = htblColNameValue.keySet();
            for (String key : keys) {
                columns.add(key);
            }

            ArrayList<String> indices = new ArrayList<String>();

            Set<String> keys1 = colNameTreeIndex.keySet();
            for (String key1 : keys1) {
                indices.add(key1);
            }
            ArrayList<String> indicesGiven = new ArrayList<String>();
            for (int i = 0; i < indices.size(); i++) {
                for (int j = 0; j < columns.size(); j++) {
                    if (indices.get(i).equals(columns.get(j))) {
                        indicesGiven.add(columns.get(j));

                    }
                }
            }
            /*
             * for (String s : htblColNameValue.keySet()) {if
             * (colNameTreeIndex.contains(s))indicesGiven.add(s); }
             */

            return indicesGiven;
        }

        public ArrayList<String> allTableIndices(Hashtable<String, TreeIndex> colNameBTreeIndex) {
            ArrayList<String> allTableIndices = new ArrayList<String>();
            Set<String> keys = colNameBTreeIndex.keySet();
            for (String key : keys) {
                allTableIndices.add(key);
            }

            return allTableIndices;
        }

        public boolean clusteringKeyHasIndex(ArrayList<String> indices, String clusteringKey) {
            if (clusteringKey != null) {
                for (int i = 0; i < indices.size(); i++) {
                    if (indices.get(i).equals(clusteringKey)) {
                        return true;
                    }
                }
                return false;
            } else
                return false;

        }

        public void createBTreeIndex(String strColName, BPTree bTree, int colPosition) throws DBAppException {
            if (colNameTreeIndex.containsKey(strColName)) {
                throw new DBAppException("BTree index already exists on this column");
            } else {
                colNameTreeIndex.put(strColName, bTree);
            }
            for (String str : pages) {
                Page p = deserialize(str);
                Ref recordReference = new Ref(p.getPageName());

                int i = 0;
                for (Tuple t : p.getTuples()) {
                    bTree.insert((Comparable) t.getAttributes().get(colPosition), recordReference);
                    i++;
                }
                p.serialize();
            }
        }

        public void createRTreeIndex(String strColName, RTree rTree, int colPosition) throws DBAppException{
            if (colNameTreeIndex.containsKey(strColName)) {
                // TODO: does this "STATEMENT" saying RTREE index here work correctly?
                throw new DBAppException("RTree index already exists on this column");
            } else {
                colNameTreeIndex.put(strColName, rTree);
            }
            for (String str : pages) {
                Page p = deserialize(str);

                int i = 0;
                for (Tuple t : p.getTuples()) {
                    Ref recordReference = new Ref(p.getPageName());
                    rTree.insert((Comparable) t.getAttributes().get(colPosition), recordReference);
                    i++;
                }
                p.serialize();
            }
        }

        public static int getIndexNumber(String pName, int s) {
            String num = pName.substring(s);
            return Integer.parseInt(num);
        }

        public static Comparable parseObject(String strTableName, Object strKey) throws DBAppException {
            try {
                Vector meta = DBApp.readFile("data/metadata.csv");
                Comparable key = null;
                for (Object O : meta) {
                    String[] curr = (String[]) O;
                    if (curr[0].equals(strTableName) && curr[3].equals("True")) // search in metadata for the table name and
                    // the
                    // key
                    {
                        if (curr[2].equals("java.lang.Integer"))
                            key = (Integer) (strKey);
                        else if (curr[2].equals("java.lang.Double"))
                            key = (Double) (strKey);
                        else if (curr[2].equals("java.util.Date"))
                            key = (Date) (strKey);
                        else if (curr[2].equals("java.lang.Boolean"))
                            key = (Boolean) (strKey);
                        else if (curr[2].equals("java.awt.Polygon"))
                            key = (Polygons) strKey;
                        else {
                            throw new DBAppException("Searching for a key of unknown type !");
                        }
                    }
                }
                return key;
            } catch (ClassCastException e) {
                throw new DBAppException("Class Cast Exception");
            }
        }

        public static Comparable parseString(String strTableName, String strKey) throws DBAppException {
            try {
                Vector meta = DBApp.readFile("data/metadata.csv");
                Comparable key = null;
                for (Object O : meta) {
                    String[] curr = (String[]) O;
                    if (curr[0].equals(strTableName) && curr[3].equals("True")) // search in metadata for the table name and
                    // the
                    // key
                    {
                        if (curr[2].equals("java.lang.Integer"))
                            key = Integer.parseInt(strKey);
                        else if (curr[2].equals("java.lang.Double"))
                            key = Double.parseDouble(strKey);
                        else if (curr[2].equals("java.util.Date"))
                            key = Date.parse(strKey);
                        else if (curr[2].equals("java.lang.Boolean"))
                            key = Boolean.parseBoolean(strKey);
                        else if (curr[2].equals("java.awt.Polygon"))
                            key = (Polygons) Polygons.parsePolygons(strKey);
                        else {
                            throw new DBAppException("Searching for a key of unknown type !");
                        }
                    }
                }
                return key;
            } catch (ClassCastException e) {
                throw new DBAppException("Class Cast Exception");
            }
        }

        /**
         * performs binary search
         *
         * @param strTableName
         * @param strKey
         *
         * @return
         * @throws DBAppException
         */
        public String SearchInTable(String strTableName, Object strKey) throws DBAppException {
            return SearchInTable(strTableName, parseObject(strTableName, strKey));
        }

        public String SearchInTable(String strTableName, String strKey) throws DBAppException {
            return SearchInTable(strTableName, parseString(strTableName, strKey));
        }

        public String SearchInTable(String strTableName, Comparable key) throws DBAppException {
            try {

                Table t = this;
                Vector<String> pages = t.getPages();
                // Vector<String> MinMax = t.getMin().toString() ;

                for (String s : pages) {
                    Page p = Table.deserialize(s);
                    int l = 0;
                    int r = p.getTuples().size() - 1;

                    while (l <= r) {
                        int m = l + (r - l) / 2;

                        // Check if x is present at mid
                        if (key.compareTo((p.getTuples().get(m)).getAttributes().get(t.getPrimaryPos())) == 0) {
                            while (m > 0 && key
                                    .compareTo((p.getTuples().get(m - 1)).getAttributes().get(t.getPrimaryPos())) == 0) {
                                m--;
                            }
                            return p.getPageName() + "#" + m;
                        }

                        // If x greater, ignore left half
                        if (key.compareTo((p.getTuples().get(m)).getAttributes().get(t.getPrimaryPos())) < 0)
                            r = m - 1;

                            // If x is smaller, ignore right half
                        else
                            l = m + 1;
                    }
                    // p.serialize(); // added by abdo
                }
                // serialize(t); // addd by abdo

                // return "-1";
                throw new DBAppException("Searched for a tuple that does not exist in the table");
            } catch (ClassCastException e) {
                throw new DBAppException("Class Cast Exception");
            }
        }

        public Iterator<Tuple> selectFromTable(SQLTerm[] arrSQLTerms, String[] strarrOperators,
                                               Vector<String[]> metaOfTable) throws DBAppException {

            // check columns names and types validity
            checkQueryValidity(arrSQLTerms, strarrOperators, metaOfTable);

            // this is for complete linear/binary search through the whole table
            // Iterator<Tuple>
            // s=inspectCols(arrSQLTerms,strarrOperators,metaOfTable);//method inspect cols
            // determines which way to search through pages
            // strarroperator operators there are checked in method setOperation and
            // getArrayOfTuples
            ArrayList<Tuple> current = new ArrayList(), next = new ArrayList();
            int i = 0, j = 0;
            for (; i < strarrOperators.length && strarrOperators[i] != null; i++)
                ;
            for (; j < arrSQLTerms.length && arrSQLTerms[i] != null; j++)
                ;
            if (j != i + 1)
                throw new DBAppException("Number of terms does not match number of operators");
            if (i == 0) { // only one attribute like , where id=5
                int pos = getColPositionWithinTuple(arrSQLTerms[0]._strColumnName, metaOfTable);
                current = getArrayOfTuples(arrSQLTerms[0]._strColumnName, arrSQLTerms[0]._objValue,
                        arrSQLTerms[0]._strOperator, pos);
            } else {
                // boolean[]chosen=new boolean[i];
                int linearScGu = linearScanGuaranteed(arrSQLTerms, strarrOperators);
                // System.out.println();
//			System.out.println((linearScGu == 1) ? "linear scan"
//					: (linearScGu == 2) ? "binary and indx only"
//							: "sweet lovely indices");

                if (linearScGu == 1) {
                    // at least non indexed column preceeded by or/xor , question if only the
                    // cluster is the non indexed
                    // preceeded by or/xor should we also do linear??
                    current = doLinearScan(arrSQLTerms, strarrOperators, metaOfTable);
                } else if (linearScGu == 2) {
                    // single clustering without index preceeded or sufficed with or/xor , no other
                    // non indexed appears in query

                    int pos = getColPositionWithinTuple(arrSQLTerms[0]._strColumnName, metaOfTable);
                    current = getArrayOfTuples(arrSQLTerms[0]._strColumnName, arrSQLTerms[0]._objValue,
                            arrSQLTerms[0]._strOperator, pos);
                    // System.out.println(Arrays.asList(current));
                    for (int k = 1; k < arrSQLTerms.length; k++) {
                        pos = getColPositionWithinTuple(arrSQLTerms[k]._strColumnName, metaOfTable);
                        if (strarrOperators[k - 1].toLowerCase().equals("and")) { // operation on the current
                            for (int z = 0; z < current.size(); z++) {
                                if (!checkTupleInCurrent(arrSQLTerms[k], current.get(z), pos)) {
                                    current.remove(z--);
                                }
                            }
                        } else {
                            // set operation between 2 indices
                            next = getArrayOfTuples(arrSQLTerms[k]._strColumnName, arrSQLTerms[k]._objValue,
                                    arrSQLTerms[k]._strOperator, pos);
                            current = setOperation(current, next, strarrOperators[k - 1]);
                        }
                    }

                } else {
                    int leadingIndexPosition = getFirstIndexPos(arrSQLTerms); // index to start with
                    // System.out.println(leadingIndexPosition);
                    if (leadingIndexPosition == -1) {
                        // no existing indices this case all the operators are ands , so will we go
                        // linear
                        // or if the cluster exists we search with it
                        int pos = getColPositionWithinTuple(strClusteringKey, metaOfTable);
                        current = (clusterExists(arrSQLTerms))
                                ? binaryWithCluster(arrSQLTerms, metaOfTable)
                                : doLinearScan(arrSQLTerms, strarrOperators, metaOfTable);
                    } else {// its guranteed inshallah that first index is either in first position
                        // or all its previous columns are anded to it(clustering key excluded)
                        String firstIndex = arrSQLTerms[leadingIndexPosition]._strColumnName;
                        // System.out.println(firstIndex);
                        Object value = arrSQLTerms[leadingIndexPosition]._objValue;
                        String op = arrSQLTerms[leadingIndexPosition]._strOperator;
                        int pos = getColPositionWithinTuple(firstIndex, metaOfTable);
                        current = getArrayOfTuples(firstIndex, value, op, pos);
                        // System.out.println(Arrays.asList(current));
                        // filters what come before index
                        for (int k = leadingIndexPosition - 1; k >= 0; k--) {
                            pos = getColPositionWithinTuple(arrSQLTerms[k]._strColumnName, metaOfTable);
                            // if(arrSQLTerms[k]._strColumnName.equals(strClusteringKey))
                            for (int z = 0; z < current.size(); z++) {

                                if (!checkTupleInCurrent(arrSQLTerms[k], current.get(z), pos)) {
                                    current.remove(z--);
                                }
                            }
                        } // proceed with terms after chosen column
                        for (int k = leadingIndexPosition + 1; k < arrSQLTerms.length; k++) {
                            pos = getColPositionWithinTuple(arrSQLTerms[k]._strColumnName, metaOfTable);
                            if (strarrOperators[k - 1].toLowerCase().equals("and")) { // operation on the current
                                for (int z = 0; z < current.size(); z++) {
                                    if (!checkTupleInCurrent(arrSQLTerms[k], current.get(z), pos)) {
                                        current.remove(z--);
                                    }
                                }
                            } else {
                                // set operation between 2 indices
                                next = getArrayOfTuples(arrSQLTerms[k]._strColumnName, arrSQLTerms[k]._objValue,
                                        arrSQLTerms[k]._strOperator, pos);
                                // g System.out.println(Arrays.asList(next));
                                current = setOperation(current, next, strarrOperators[k - 1]);
                            }
                        }
                    }

                }
            }

            return current.iterator();
        }

        private boolean checkTupleInCurrent(SQLTerm sqlTerm, Tuple t, int position) throws DBAppException {
            Comparable comparableOne = (Comparable) t.getAttributes().get(position);
            Comparable comparableTwo = (Comparable) sqlTerm.getObjValue();
            return switch (sqlTerm.getStrOperator()) {
                case "=" ->
                        (comparableOne instanceof CustomPolygon) ? comparableOne.equals(comparableTwo) : comparableOne.compareTo(comparableTwo) == 0;
                case "!=" ->
                        (comparableOne instanceof CustomPolygon) ? !comparableOne.equals(comparableTwo) : comparableOne.compareTo(comparableTwo) != 0;
                case ">" -> comparableOne.compareTo(comparableTwo) > 0;
                case ">=" -> comparableOne.compareTo(comparableTwo) >= 0;
                case "<" -> comparableOne.compareTo(comparableTwo) < 0;
                case "<=" -> comparableOne.compareTo(comparableTwo) <= 0;
                default -> throw new DBAppException("INVALID OPERATOR " + sqlTerm.getStrOperator());
            };
        }

        private boolean clusterExists(SQLTerm[] arrSQLTerms) {
            for (SQLTerm x : arrSQLTerms) {
                if (x.getStrColumnName().equals(clusteringKey)) {
                    return true;
                }
            }
            return false;
        }

        private ArrayList<Tuple> binaryWithCluster(SQLTerm[] arrSQLTerms,
                                                   Vector<String[]> metaOfTable) throws DBAppException {
            ArrayList<Tuple> tupAL = new ArrayList<>();

            for (SQLTerm sqlTerm : arrSQLTerms) {
                if (sqlTerm.getStrColumnName().equals(clusteringKey)) {
                    tupAL = getArrayOfTuples(clusteringKey, sqlTerm.getObjValue(), sqlTerm.getStrOperator(),
                            getPrimaryPosition());
                    break;

                }
            }
            for (SQLTerm arrSQLTerm : arrSQLTerms) {
                if (arrSQLTerm.getStrColumnName().equals(clusteringKey))
                    continue;
                for (int i = 0; i < tupAL.size(); i++) {
                    if (!checkTupleInCurrent(arrSQLTerm, tupAL.get(i),
                            getColPositionWithinTuple(arrSQLTerm.getStrColumnName(), metaOfTable))) {
                        tupAL.remove(i--);
                    }
                }
            }
            return tupAL;
        }

        private int linearScanGuaranteed(SQLTerm[] arrSQLTerms, String[] arrOperators) {

            boolean Grant = true;
            boolean isFound = false, clustNondIndex = false, nonClustNonIndex = false;

            if (arrSQLTerms[0].getStrOperator().equals("!="))
                return 1;

            if (!colNameTreeIndex.containsKey(arrSQLTerms[0].getStrColumnName())) {
                if (!arrSQLTerms[0].getStrColumnName().equals(clusteringKey)) {
                    if (!arrOperators[0].equalsIgnoreCase("and"))
                        return 1;
                    else
                        nonClustNonIndex = true;
                } else if (arrSQLTerms[0].getStrColumnName().equals(clusteringKey)
                        && !arrOperators[0].equalsIgnoreCase("and"))
                    clustNondIndex = true;
            } else {
                isFound = true;
            }

            for (int i = 1; i < arrSQLTerms.length; i++) {
                if (arrSQLTerms[i].getStrOperator().equals("!="))
                    return 1;
                if (colNameTreeIndex.containsKey(arrSQLTerms[i].getStrColumnName())) {
                    if (arrOperators[i - 1].equalsIgnoreCase("and"))
                        isFound = true;
                    else if (arrSQLTerms[i - 1].getStrColumnName().equals(clusteringKey))
                        isFound = true;
                    else if (!isFound)
                        Grant = false;
                }
                if (arrSQLTerms[i].getStrColumnName().equals(clusteringKey)
                        && !colNameTreeIndex.containsKey(clusteringKey)
                        && !arrOperators[i - 1].equalsIgnoreCase("and")) {
                    clustNondIndex = true;
                    if (nonClustNonIndex)
                        return 1;
                    continue;
                }

                if (!colNameTreeIndex.containsKey(arrSQLTerms[i].getStrColumnName())) {
                    if (!arrOperators[i - 1].equalsIgnoreCase("and"))
                        return 1;
                    else if (clustNondIndex)
                        return 1;
                    nonClustNonIndex = true;
                }

            }
            if (clustNondIndex && !nonClustNonIndex)
                return 2;
            if (!Grant)
                return 1;
            return 0;
        }

        private int getFirstIndexPos(SQLTerm[] arrSQLTerms) {
            for (int i = 0; i < arrSQLTerms.length; i++) {
                if (colNameTreeIndex.containsKey(arrSQLTerms[i].getStrColumnName()))
                    return i;
            }
            return -1;
        }

        private ArrayList<Tuple> doLinearScan(SQLTerm[] arrSQLTerms, String[] arrOperators, Vector<String[]> metaOfTable)
                throws DBAppException {
            ArrayList<Integer> intAL = new ArrayList<>();
            ArrayList<Tuple> tupAL = new ArrayList<>();
            for (SQLTerm sqlTerm : arrSQLTerms) {
                int i = 0;
                for (; i < metaOfTable.size(); i++) {
                    if (sqlTerm.getStrColumnName().equals(metaOfTable.get(i)[1]))
                        break;
                }
                intAL.add(i);
            }
            for (String page : pages) {
                Page p = deserialize(page);
                for (Tuple t : p.getTuples()) {
                    if (tupleMetConditions(arrSQLTerms, arrOperators, intAL, intAL.size() - 1, t))
                        tupAL.add(t);
                }
            }
            return tupAL;
        }

        private boolean tupleMetConditions(SQLTerm[] arrSQLTerms, String[] arrOperators, ArrayList<Integer> x, int i,
                                           Tuple t) throws DBAppException {
            return switch (arrOperators[i - 1].toLowerCase()) {
                case "or" -> (i == 1)
                        ? checkTupleInCurrent(arrSQLTerms[0], t, x.get(0))
                        || checkTupleInCurrent(arrSQLTerms[1], t, x.get(1))
                        : tupleMetConditions(arrSQLTerms, arrOperators, x, i - 1, t)
                        || checkTupleInCurrent(arrSQLTerms[i], t, x.get(i));
                case "and" -> (i == 1)
                        ? checkTupleInCurrent(arrSQLTerms[0], t, x.get(0))
                        && checkTupleInCurrent(arrSQLTerms[1], t, x.get(1))
                        : tupleMetConditions(arrSQLTerms, arrOperators, x, i - 1, t)
                        && checkTupleInCurrent(arrSQLTerms[i], t, x.get(i));
                case "xor" -> (i == 1)
                        ? checkTupleInCurrent(arrSQLTerms[0], t, x.get(0))
                        ^ checkTupleInCurrent(arrSQLTerms[1], t, x.get(1))
                        : tupleMetConditions(arrSQLTerms, arrOperators, x, i - 1, t)
                        ^ checkTupleInCurrent(arrSQLTerms[i], t, x.get(i));
                default -> false;
            };
        }

        private ArrayList<Tuple> setOperation(ArrayList<Tuple> current, ArrayList<Tuple> next, String str) {
            str = str.toLowerCase();
            return switch (str) {
                case "or" -> orSets(current, next);
                case "xor" -> xorSets(current, next);
                default -> new ArrayList<>();
            };
        }

        private ArrayList<Tuple> xorSets(ArrayList<Tuple> current, ArrayList<Tuple> next) {
            return differenceSets(orSets(current, next), andSets(current, next));
        }
        public ArrayList<Tuple> differenceSets(ArrayList<Tuple> A, ArrayList<Tuple> B) {
            ArrayList<Tuple> res = new ArrayList<>();

            HashSet<Tuple> hashsetOne = new HashSet<>();
            HashSet<Tuple> hashsetTwo = new HashSet<>();

            hashsetTwo.addAll(B);
            for (Tuple cur : A) {
                if (hashsetOne.contains(cur))
                    continue;
                hashsetOne.add(cur);
                if (!hashsetTwo.contains(cur)) {
                    res.add(cur);
                }
            }
            return res;
        }

        public ArrayList<Tuple> andSets(ArrayList<Tuple> current, ArrayList<Tuple> next) {
            ArrayList<Tuple> result = new ArrayList<>();

            HashSet<Tuple> hashsetOne = new HashSet<>(current);
            HashSet<Tuple> hashsetTwo = new HashSet<>();
            for (Tuple cur : next) {
                if (!hashsetTwo.contains(cur)) {
                    hashsetTwo.add(cur);
                    if (hashsetOne.contains(cur)) {
                        result.add(cur);
                    }
                }
            }
            return result;
        }

        private ArrayList<Tuple> orSets(ArrayList<Tuple> current, ArrayList<Tuple> next) {
            Set<Tuple> tupleSet = new HashSet<>();
            tupleSet.addAll(current);
            tupleSet.addAll(next);
            return new ArrayList<>(tupleSet);
        }

        private ArrayList<Tuple> getArrayOfTuples(String strColumnName, Object objValue, String strOperator, int position)
                throws DBAppException {
            if (strOperator.equals("!="))
                return goLinear(objValue, strOperator, position);

            return (colNameTreeIndex.containsKey(strColumnName))
                    ? goWithIndex(strColumnName, objValue, strOperator, position)
                    : (strColumnName.equals(clusteringKey)) ? goBinary(strColumnName, objValue, strOperator, position)
                    : goLinear(objValue, strOperator, position);
        }

        private int getColPositionWithinTuple(String strColumnName, Vector<String[]> metaOfTable) {
            for (int i = 0; i < metaOfTable.size(); i++) {
                if (metaOfTable.get(i)[1].equals(strColumnName))
                    return i;
            }
            return -1;
        }

        private boolean validOp(String strOperator) {
            return strOperator.equals("=") || strOperator.equals("!=") || strOperator.equals(">")
                    || strOperator.equals(">=") || strOperator.equals("<") || strOperator.equals("<=");
        }

        private ArrayList<Tuple> goLinear(Object _objValue, String strOperator, int position)
                throws DBAppException {
            return switch (strOperator) {
                case ">", ">=" -> mtOrMtlLinear(_objValue, strOperator, position);
                case "<", "<=" -> ltOrLtlLinear(_objValue, strOperator, position);
                case "=" -> equalsLinear(_objValue, position);
                case "!=" -> notEqualsLinear(_objValue, position);
                default -> new ArrayList<>();
            };
        }

        private ArrayList<Tuple> notEqualsLinear(Object objValue, int position)
                throws DBAppException {
            ArrayList<Tuple> result = new ArrayList<>();
            for (String s : pages) {
                Page page = deserialize(s);
                for (int j = 0; j < page.getTuples().size(); j++) {
                    Comparable grantKey = (Comparable) page.getTuples().get(j).getAttributes().get(position);
                    Comparable obj = (Comparable) objValue;
                    if (grantKey.compareTo(obj) != 0) {
                        result.add(page.getTuples().get(j));
                    } else if ((grantKey instanceof CustomPolygon) && !grantKey.equals(obj)) {
                        result.add(page.getTuples().get(j));
                    }

                }

            }
            return result;
        }

        private ArrayList<Tuple> equalsLinear(Object objValue, int position)
                throws DBAppException {
            ArrayList<Tuple> result = new ArrayList();
            for (String s : pages) {
                Page page = deserialize(s);
                for (int j = 0; j < page.getTuples().size(); j++) {
                    Comparable grantKey = (Comparable) page.getTuples().get(j).getAttributes().get(position);
                    Comparable obj = (Comparable) objValue;
                    if (grantKey.compareTo(obj) == 0) {
                        if (!(grantKey instanceof CustomPolygon) || grantKey.equals(obj))
                            result.add(page.getTuples().get(j));
                    }
                }
            }
            return result;
        }

        private ArrayList<Tuple> ltOrLtlLinear(Object objValue, String strOperator, int position)
                throws DBAppException {

            ArrayList<Tuple> result = new ArrayList<>();
            for (int i = 0; i < pages.size(); i++) {
                if (getMin(i).compareTo(objValue) > 0)
                    break;
                if (getMin(i).compareTo(objValue) == 0 && strOperator.length() == 1)
                    break;
                Page page = deserialize(pages.get(i));
                int j = 0;
                while (j < page.getTuples().size() && ((Comparable) page.getTuples().get(j).getAttributes().get(position))
                        .compareTo(objValue) < 0)
                    result.add(page.getTuples().get(j++));
                if (strOperator.length() == 2) {
                    while (j < page.getTuples().size() && ((Comparable) page.getTuples().get(j).getAttributes().get(position))
                            .compareTo(objValue) == 0)
                        result.add(page.getTuples().get(j++));
                }

            }
            return result;

        }

        private ArrayList<Tuple> mtOrMtlLinear(Object objValue, String strOperator, int position)
                throws DBAppException {
            ArrayList<Tuple> result = new ArrayList();
            for (int i = pages.size() - 1; i >= 0; i--) {
                if (getMax(i).compareTo(objValue) < 0)
                    break;
                if (getMax(i).compareTo(objValue) == 0 && strOperator.length() == 1)
                    break;
                Page x = deserialize(pages.get(i));
                int j = x.getTuples().size() - 1;
                while (j >= 0 && ((Comparable) x.getTuples().get(j).getAttributes().get(position))
                        .compareTo(objValue) > 0) {
                    result.add(0, x.getTuples().get(j));
                    j--;
                }
                if (strOperator.length() == 2) {
                    while (j >= 0 && ((Comparable) x.getTuples().get(j).getAttributes().get(position))
                            .compareTo(objValue) == 0) {
                        result.add(0, x.getTuples().get(j));
                        j--;
                    }
                }
            }
            return result;
        }

        private ArrayList<Tuple> goBinary(String strColumnName, Object objValue, String strOperator, int position)
                throws DBAppException {
            return switch (strOperator) {
                case ">", ">=" -> mtOrMtlBinary(objValue, strOperator, position);
                case "<", "<=" -> ltOrLtlBinary(objValue, strOperator, position);
                case "=" -> equalsBinary(objValue, position);
                default -> new ArrayList<>();
            };

        }

        private ArrayList<Tuple> equalsBinary(Object objValue, int position)
                throws DBAppException {
            String[] searchResult = SearchInTable(tableName, objValue).split("#");
            String startPage = searchResult[0];
            int startPageIndex = getPageIndex(startPage);
            int startTupleIndex = Integer.parseInt(searchResult[1]);
            ArrayList<Tuple> res = new ArrayList<>();
            for (int pageIndex = startPageIndex, tupleIndex = startTupleIndex; pageIndex < pages
                    .size(); pageIndex++, tupleIndex = 0) {
                if (getMin(pageIndex).compareTo(objValue) > 0)
                    break;
                Page currentPage = deserialize(pages.get(pageIndex));
                Comparable comparable;
                while (tupleIndex < currentPage.getTuples().size()
                        && (comparable = (Comparable) currentPage.getTuples().get(tupleIndex).getAttributes().get(position))
                        .compareTo(objValue) == 0)
                    if (!(comparable instanceof CustomPolygon) || comparable.equals(objValue))
                        res.add(currentPage.getTuples().get(tupleIndex++));
            }

            return res;
        }

        private ArrayList<Tuple> ltOrLtlBinary(Object objValue, String strOperator, int position)
                throws DBAppException {
            return ltOrLtlLinear(objValue, strOperator, position);
        }

        private ArrayList<Tuple> mtOrMtlBinary(Object objValue, String strOperator, int position)
                throws DBAppException {
            return mtOrMtlLinear(objValue, strOperator, position);

        }

        private ArrayList<Tuple> goWithIndex(String strColumnName, Object objValue, String strOperator, int position)
                throws DBAppException {
            return switch (strOperator) {
                case ">", ">=" -> mtOrMtlIndex(strColumnName, objValue, strOperator, position);
                case "<", "<=" -> ltOrLtlIndex(strColumnName, objValue, strOperator, position);
                case "=" -> equalsIndex(strColumnName, objValue, strOperator, position);
                default -> new ArrayList();
            };
        }

        private ArrayList<Tuple> equalsIndex(String strColumnName, Object objValue, String strOperator, int position)
                throws DBAppException {
            ArrayList<Tuple> result = new ArrayList<>();
            String lastPage = pages.get(pages.size() - 1);
            int lastPageMaxNum = Integer.parseInt(lastPage.substring(tableName.length()));
            boolean[] visited = new boolean[lastPageMaxNum + 1];
            TreeIndex b = colNameTreeIndex.get(strColumnName);
            GeneralRef resultReference = b.search((Comparable) objValue);
            if (resultReference == null)
                return result;
            ArrayList<Ref> referenceList = resultReference.getAllRef();
            for (Ref currentReference : referenceList) {
                String pageName = currentReference.getPage();
                int curPageNum = Integer.parseInt(pageName.substring(tableName.length()));
                if (visited[curPageNum])
                    continue;
                addToResultSet(result, pageName, position, objValue, strOperator);
                visited[curPageNum] = true;
            }
            return result;
        }

        private ArrayList<Tuple> ltOrLtlIndex(String strColumnName, Object objValue, String strOperator, int position)
                throws DBAppException {
            if (strColumnName.equals(clusteringKey))
                return ltOrLtlLinear(objValue, strOperator, position);
            ArrayList<Tuple> res = new ArrayList();
            String lastPage = pages.get(pages.size() - 1);
            int lastPageMaxNum = Integer.parseInt(lastPage.substring(tableName.length()));
            boolean[] visited = new boolean[lastPageMaxNum + 1];
            TreeIndex treeIndex = colNameTreeIndex.get(strColumnName);
            LeafNode leafNode = treeIndex.getLeftmostLeaf();
            while (leafNode != null) {
                int i;
                for (i = 0; i < leafNode.getNumberOfKeys(); i++) {
                    GeneralRef gr = leafNode.getRecord(i);
                    if (leafNode.getKey(i).compareTo(objValue) > 0)
                        break;
                    if (leafNode.getKey(i).compareTo(objValue) == 0 && strOperator.length() == 1)
                        break;
                    Set<Ref> ref = fillInRef(gr);
                    for (Ref r : ref) {
                        String pageName = r.getPage();
                        int curPageNum = Integer.parseInt(pageName.substring(tableName.length()));
                        if (visited[curPageNum])
                            continue;
                        addToResultSet(res, pageName, position, objValue, strOperator);
                        visited[curPageNum] = true;
                    }

                }
                if (i < leafNode.getNumberOfKeys())
                    break;
                leafNode = leafNode.getNext();
            }
            return res;
        }

        private void addToResultSet(ArrayList<Tuple> res, String pageName, int position, Object objValue, String strOperator)
                throws DBAppException {
            switch (strOperator) {
                case ("<") -> addToResultSetLESS(res, pageName, position, objValue);
                case ("<=") -> addToResultSetLESSorEQUAL(res, pageName, position, objValue);
                case ("=") -> addToResultSetEQUAL(res, pageName, position, objValue);
                case (">") -> addToResultSetMORE(res, pageName, position, objValue);
                case (">=") -> addToResultSetMOREorEQUAL(res, pageName, position, objValue);
                default -> throw new DBAppException("DEFAULT OPERATOR");
            }
        }

        private void addToResultSetLESS(ArrayList<Tuple> res, String pageName, int position, Object objValue)
                throws DBAppException {
            Page x = deserialize(pageName);
            for (int i = 0; i < x.getTuples().size(); i++) {
                if (((Comparable) objValue).compareTo(x.getTuples().get(i).getAttributes().get(position)) > 0)
                    res.add(x.getTuples().get(i));
            }
        }

        private void addToResultSetLESSorEQUAL(ArrayList<Tuple> res, String pageName, int position, Object objValue)
                throws DBAppException {
            Page x = deserialize(pageName);
            for (int i = 0; i < x.getTuples().size(); i++) {
                if (((Comparable) objValue).compareTo(x.getTuples().get(i).getAttributes().get(position)) >= 0)
                    res.add(x.getTuples().get(i));
            }
        }

        private void addToResultSetEQUAL(ArrayList<Tuple> res, String pageName, int position, Object objValue)
                throws DBAppException {
            Page page = deserialize(pageName);
            for (int i = 0; i < page.getTuples().size(); i++) {
                Comparable objectValue = (Comparable) objValue;
                Tuple currentTuple = page.getTuples().get(i);
                Comparable currentKey = (Comparable) currentTuple.getAttributes().get(position);
                if (objectValue.compareTo(currentKey) == 0) {
                    if (! (objValue instanceof CustomPolygon) || objValue.equals(currentKey))
                        res.add(page.getTuples().get(i));
                }
            }
        }

        private void addToResultSetMORE(ArrayList<Tuple> res, String pagename, int pos, Object _objValue)
                throws DBAppException {
            Page x = deserialize(pagename);
            for (int i = 0; i < x.getTuples().size(); i++) {
                if (((Comparable) _objValue).compareTo(x.getTuples().get(i).getAttributes().get(pos)) < 0)
                    res.add(x.getTuples().get(i));
            }
        }

        private void addToResultSetMOREorEQUAL(ArrayList<Tuple> res, String pageName, int position, Object objValue)
                throws DBAppException {
            Page x = deserialize(pageName);
            for (int i = 0; i < x.getTuples().size(); i++) {
                if (((Comparable) objValue).compareTo(x.getTuples().get(i).getAttributes().get(position)) <= 0)
                    res.add(x.getTuples().get(i));
            }
        }

        private Set<Ref> fillInRef(GeneralRef generalRef) throws DBAppException {
            Set<Ref> ref = new HashSet<>();
            if (generalRef instanceof Ref)
                ref.add((Ref) generalRef);
            else {
                OverflowRef overflowRef = (OverflowRef) generalRef;
                OverflowPage overflowPage = overflowRef.getFirstPage();
                while (overflowPage != null) {
                    ref.addAll(overflowPage.getRefs());
                    overflowPage = overflowPage.getNext1();

                }
            }
            return ref;
        }

        private ArrayList<Tuple> mtOrMtlIndex(String strColumnName, Object objValue, String strOperator, int position)
                throws DBAppException {
            if (strColumnName.equals(clusteringKey))
                return mtOrMtlLinear(objValue, strOperator, position);
            else {
                ArrayList<Tuple> results = new ArrayList<>();
                String lastPage = pages.get(pages.size() - 1);
                int lastPageMaxNum = Integer.parseInt(lastPage.substring(tableName.length()));
                boolean[] visited = new boolean[lastPageMaxNum + 1];
                TreeIndex b = colNameTreeIndex.get(strColumnName);
                ArrayList referenceList = strOperator.equals(">") ? b.searchMT((Comparable) objValue)
                        : b.searchMTE((Comparable) objValue);
                for (Object o : referenceList) {
                    GeneralRef currentGR = (GeneralRef) o;
                    ArrayList<Ref> currentRefsForOneKey = currentGR.getAllRef();
                    for (Ref currentReference : currentRefsForOneKey) {
                        String pageName = currentReference.getPage();
                        int curPageNum = Integer.parseInt(pageName.substring(tableName.length()));
                        if (visited[curPageNum])
                            continue;
                        addToResultSet(results, pageName, position, objValue, strOperator);
                        visited[curPageNum] = true;
                    }
                }
                return results;
            }
        }

        public void checkQueryValidity(SQLTerm[] arrSQLTerms, String[] operators, Vector<String[]> metaOfTable)
                throws DBAppException {
            for (SQLTerm x : arrSQLTerms) {
                if (x==null) {
                    throw new DBAppException("INVALID SQL Term");
                }
                int i;
                if (!validOp(x.getStrOperator()))
                    throw new DBAppException("INVALID OPERATION\" " + x.getStrOperator());
                for (i = 0; i < metaOfTable.size(); i++) {
                    if (!x.getStrTableName().equals(arrSQLTerms[0].getStrTableName()))
                        throw new DBAppException("INVALID table name " + x.getStrColumnName() + "INVALID OPERATION");
                    if (metaOfTable.get(i)[1].equals(x.getStrColumnName())) {
                        try {
                            Class<?> colType = Class.forName(metaOfTable.get(i)[2]);

                            Class<?> parameterType = x.getObjValue().getClass();
                            Class<?> polyOriginal = Class.forName("java.awt.Polygon");
                            if (colType == polyOriginal) {
                                x.setObjValue(new CustomPolygon((Polygon) x.getObjValue()));
                            }
                            if (!colType.equals(parameterType)) {
                                throw new DBAppException("INVALID Datatype");
                            } else {
                                break;
                            }

                        } catch (ClassNotFoundException e) {
                            throw new DBAppException("Class Not Found Exception");
                        }
                    }
                }
                if (i == metaOfTable.size())
                    throw new DBAppException("Column " + x.getStrColumnName() + " doesn't exist");
            }
            for (String str : operators) {
                str = str.toLowerCase();
                if (!(str.equals("and") || str.equals("xor") || str.equals("or")))
                    throw new DBAppException("INVALID OPERATION " + str);
            }
        }

        public void drop() throws DBAppException {
            for (String str : pages) {
                File fileIn = new File("data: " + str + ".class");
                fileIn.delete();
            }
        }

        public int getPageIndex(String pageName) {
            return getSuffix(pageName);
        }

        public int getSuffix(String pageName) {
            return Integer.parseInt(pageName.substring(tableName.length()));
        }

        public String toString() {
            StringBuilder stringBuilder = new StringBuilder();

            stringBuilder.append("Name: ").append(tableName).append("\n");

            stringBuilder.append("Clustering key: ").append(clusteringKey).append(" at pos=").append(primaryPosition).append("\n");

            stringBuilder.append("Pages:\n{");
            for (int i = 0; i < pages.size() - 1; i++) {
                stringBuilder.append(pages.get(i)).append(", ");
            }
            if (!pages.isEmpty())
                stringBuilder.append(pages.get(pages.size() - 1)).append("}\n");
            stringBuilder.append("Indexed Columns: \n");
            for (String col : colNameTreeIndex.keySet()) {
                stringBuilder.append(col).append("\t");
            }
            stringBuilder.append("Indexes: \n");
            for (String col : colNameTreeIndex.keySet()) {
                stringBuilder.append(col).append("\n");
                stringBuilder.append(colNameTreeIndex.get(col)).append("\n");
            }
            return stringBuilder.toString();
        }

        static String show(ArrayList<Tuple> arr) {
            StringBuilder sb = new StringBuilder();
            sb.append("{");
            for (int i = 0; i < arr.size() - 1; i++) {
                sb.append(arr.get(i).getAttributes().get(0)).append(", ");
            }
            if (!arr.isEmpty())
                sb.append(arr.get(arr.size() - 1).getAttributes().get(0));
            sb.append("}");
            return sb.toString();
        }

        public static void main(String[] args) {

            // initialize here

        }


}
