package src.DBGeneralEngine;

import java.awt.*;
import java.io.*;
import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.*;

/**
 * DBApp class represents the main database application.
 * This class manages a collection of tables and configurations related to the database.
 */
public class DBApp implements Serializable {


    /**
     * Attributes
     * <p>
     *
     * tables   ->    A Vector to hold the tables in the database
     * MaximumRowsCountInPage   ->  Maximum number of rows allowed per page
     * nodeSide ->  Size of nodes in the database structure
     */
    Vector<Table> tables = new Vector<>();
    private int MaximumRowsCountInPage;
    private int nodeSize;


    /**
     * Getters & Setters


     * Returns the Vector of tables in the database.
     *
     * @return a Vector containing the tables
     */
    public Vector<Table> getTables() {
        return tables;
    }

    /**
     * Sets the Vector of tables for the database to the specified Vector.
     *
     * @param tables the new Vector of Table objects to be set for the database
     */
    public void setTables(Vector<Table> tables) {
        this.tables = tables;
    }

    /**
     * Returns the maximum number of rows allowed per page.
     *
     * @return the maximum rows count in a page
     */
    public int getMaximumRowsCountInPage() {
        return MaximumRowsCountInPage;
    }

    /**
     * Sets the maximum number of rows allowed per page.
     *
     * @param maximumRowsCountInPage the new maximum rows count to be set
     */
    public void setMaximumRowsCountInPage(int maximumRowsCountInPage) {
        MaximumRowsCountInPage = maximumRowsCountInPage;
    }

    /**
     * Returns the size of nodes in the database structure.
     *
     * @return the node size
     */
    public int getNodeSize() {
        return nodeSize;
    }

    /**
     * Sets the size of nodes in the database structure.
     *
     * @param nodeSize the new size of nodes to be set
     */
    public void setNodeSize(int nodeSize) {
        this.nodeSize = nodeSize;
    }


    /**
     * Inserts a new entry into the metadata CSV file for a specified table.
     * This includes the table name, column names, types, and whether each column is a clustering key.
     *
     * @param strTableName the name of the table
     * @param strClusteringKeyColumn the name of the clustering key column
     * @param htblColNameType a Hashtable mapping column names to their types
     * @throws IOException if an error occurs while writing to the file
     */
    public static void insertIntoMetadata(String strTableName, String strClusteringKeyColumn, Hashtable<String, String> htblColNameType) throws IOException {

        FileWriter csvWriter = new FileWriter("metadata.csv", true);
        csvWriter.write("\n");

        // Loop through each column in the hashtable
        for (int i = 0; i < htblColNameType.size(); i++) {

            // Obtaining the keys from the hashtable
            Enumeration<String> e = htblColNameType.keys();
            String ColumnName = (String) e.nextElement();
            String ColumnType = (String) e.nextElement();

            // Write the table name, column name, and the column type, and clustering key status to the CSV
            csvWriter.append(strTableName).append(',')
                    .append(ColumnName).append(',')
                    .append(ColumnType).append(',');

            boolean cluster;
            String clusterstring;
            cluster = strClusteringKeyColumn.contentEquals(ColumnName);
            clusterstring = Boolean.toString(cluster);
            csvWriter.append(clusterstring);
        }
        csvWriter.flush();
        csvWriter.close();
    }


    /**
     * Initializes the metadata CSV file with header columns.
     * This should be called to set up the structure of the metadata file before any entries are added.
     *
     * @throws IOException if an error occurs while writing to the file
     */
    public static void insertIntoMetadataBase() throws IOException {
        FileWriter csvWriter = new FileWriter("metadata.csv");
        csvWriter.append("TableName,ColumnName,ColumnType,ClusteringKey");
        csvWriter.flush();
        csvWriter.close();
    }


    /**
     * HELPER methods listed below
     * This does whatever initialization you would like
     * Or leave it empty if there is no code you want to
     * Execute at application startup
     */
    public void init() throws IOException, DBAppException {

        try {
            InputStream inStream = new FileInputStream("config/DBApp.properties");
            Properties bal = new Properties();
            bal.load(inStream);
            MaximumRowsCountInPage = Integer.parseInt(bal.getProperty("MaximumRowsCountInPage"));
            nodeSize = Integer.parseInt(bal.getProperty("NodeSize"));

            File data = new File("data");
            data.mkdir();
            File metadata = new File("data/metadata.csv");
            metadata.createNewFile();
            File metaBPtree = new File("data/metaBPtree.csv");
            if (metaBPtree.createNewFile()) {
                FileWriter csvWriter = new FileWriter("data/metaBPtree.csv");
                csvWriter.append("0");
                csvWriter.flush();
                csvWriter.close();
            }
        } catch (IOException e) {
            System.out.println(e.getStackTrace());
            throw new DBAppException("IO Exception Initializing the file");
        }

    }

    public static void clear() {
        File metadata = new File("data/metadata.csv");
        metadata.delete();
        File file = new File("data/");
        String[] pages = file.list();
        if (pages == null) return;
        for (String p : pages) {
            File pageToDelete = new File("data/" + p);
            pageToDelete.delete();
        }
    }


    public static String[][] readCSV(String path) throws FileNotFoundException, IOException {
        try (FileReader fr = new FileReader(path); BufferedReader br = new BufferedReader(fr);) {
            Collection<String[]> lines = new ArrayList<>();
            for (String line = br.readLine(); line != null; line = br.readLine())
                lines.add(line.split(";"));
            return lines.toArray(new String[lines.size()][]);
        }
    }

    public static String[][] make2d() throws IOException {
        String[][] x = readCSV("metadata.csv");
        String[] elements = x[0][0].split(",");
        int second = elements.length;
        String[][] y = new String[x.length][second];
        for (int i = 0; i < x.length; i++) {
            elements = x[i][0].split(",");
            System.arraycopy(elements, 0, y[i], 0, elements.length);
        }
        return y;
    }

    public static int occurrenceCounter(String[][] arr, String s) {
        int x = 0;
        for (String[] strings : arr)
            for (String string : strings) if (string.equals(s)) x++;
        return x;
    }

    public static String[][] findRecordByTableName(String s) throws IOException {
        String[][] y = make2d();
        String[][] arr = new String[occurrenceCounter(y, s)][y.length];

        int b = 0;
        for (String[] strings : y)
            if (strings[0].equals(s)) {
                System.arraycopy(strings, 0, arr[b], 0, y.length);
                b++;
            }
        return arr;
    }

    public Table findTable(String s) {
        for (Table table : tables) if (((Table) table).getTableName().equals(s)) return (Table) table;
        return new Table();
    }

    public static void print2d(String[][] arr) {
        for (String[] strings : arr) {
            for (String string : strings) System.out.print(string);
            System.out.println();
        }
    }

    public static String getType(String strTableName, String s) throws FileNotFoundException, IOException {
        String[][] arr = findRecordByTableName(strTableName);
        for (String[] strings : arr)
            for (int j = 0; j < strings.length; j++)
                if (strings[j].equals(s))
                    return strings[j + 1];
        return "";
    }

    public static String getTypeOfKey(String strTableName) throws FileNotFoundException, IOException {
        String[][] arr = findRecordByTableName(strTableName);
        for (String[] strings : arr)
            if (strings[3].equals("True"))
                return strings[2];
        return "";
    }


    public static boolean equalPolygons(final Polygon p1, final Polygon p2) {
        if (p1 == null) return (p2 == null);
        if (p2 == null) return false;
        if (p1.npoints != p2.npoints) return false;
        if (!Arrays.equals(p1.xpoints, p2.xpoints)) return false;
        return Arrays.equals(p1.ypoints, p2.ypoints);
    }


    /**
     * Methods
     */
    public void createTable(
            String strTableName,
            String strClusteringKeyColumn,
            Hashtable<String, Object> htblColNameType)
            throws DBAppException, IOException {
        htblColNameType.put("TouchDate", "java.util.Date");
        Table t = new Table();
        tables.add(t);
        Page firstPage = new Page("");
        t.setTableName(strTableName);
        t.setClusteringKey(strClusteringKeyColumn);
//        firstPage.hashtables.add(htblColNameType);
        t.getPages().add(String.valueOf(firstPage));
        firstPage.serialize(firstPage, "firstPage.ser");

        Enumeration<String> e = htblColNameType.keys();
        String in1 = (String) e.nextElement();
        String in2 = (String) e.nextElement();

        if (
                !in2.equals("java.lang.Integer") ||
                        !in2.equals("java.lang.String") ||
                        !in2.equals("java.lang.Double") ||
                        !in2.equals("java.lang.Boolean") ||
                        !in2.equals("java.util.Date") ||
                        !in2.equals("java.awt.Polygon")
        ) throw (new DBAppException("Data type not supported"));
    }


    /**
     * Inserts one row at a time
     */
    public void insertIntoTable(
            String strTableName,
            Hashtable<String, Object> hashtableColumnNameValue)
            throws DBAppException, FileNotFoundException, IOException {

        DateFormat dateformat = new SimpleDateFormat("yyyy/MM/dd HH:mm:ss");
        Date date = new Date();
        hashtableColumnNameValue.put("TouchDate", dateformat.format(date));
        Table t = new Table();
        // Serialize pages OR serialize table with transients

        // page p = new page();
        if (occurrenceCounter(DBApp.findRecordByTableName(strTableName), strTableName) == 0) return;
        else {
            // Check column type with column type in metadata

            Enumeration<String> e = hashtableColumnNameValue.keys();
            String in1 = (String) e.nextElement();
            // Column name
            Object in2 = hashtableColumnNameValue.get(in1);
            // Value

            for (int i = 0; i < findRecordByTableName(strTableName).length; i++)
                for (int j = 0; j < findRecordByTableName(strTableName)[i].length; j++)
                    if (!findRecordByTableName(strTableName)[i][j].equals(in2.getClass()))
                        throw (new DBAppException("Classes don't match"));

            t = findTable(strTableName);

//            Page p = (Page) t.getPages().get(t.getPages().size()-1);
//
//            if(p.hashtables.size()<200)
//                p.hashtables.add(hashtableColumnNameValue);
//            else {
//                t.pages.add(p);
//                page p2 = new page();
//                p2.hashtables.add(hashtableColumnNameValue);
//                p=p2;
//            }
        }
    }


    /**
     * hashtableColumnNameValue holds the key and new value
     * hashtableColumnNameValue will not include clustering key as column name
     * hashtableColumnNameValue entries are ANDED together
     */
    @SuppressWarnings("unchecked")
    public void updateTable(
            String strTableName,
            String strClusteringKey,
            Hashtable<String, Object> hashtableColumnNameValue)
            throws DBAppException, NumberFormatException, FileNotFoundException, IOException, ParseException {

        Object key;

        switch (getTypeOfKey(strTableName)) {
            case "java.lang.Integer" -> {
                // PARSING
                key = Integer.parseInt(strClusteringKey);

                // COMPARING


//                for (int i = 0; i < tables.size(); i++)
//                    if (tables.elementAt(i).getTableName().equals(strTableName))
//                        for (int j = 0; j < tables.elementAt(i).getPages().size(); j++)
//                            for (int k = 0; k < tables.elementAt(i).getPages().elementAt(j).hashtables.size(); k++) {
//                                Hashtable<String, Object> tempHash = tables.elementAt(i).getPages().elementAt(j).hashtables.elementAt(k);
//                                String newName = hashtableColumnNameValue.keys().nextElement();
//                                Object newValue = hashtableColumnNameValue.get(newName);
//                                String oldName = tempHash.keys().nextElement();
//                                Object oldValue = tempHash.get(oldName);
//                                if (newName.equals(oldName) && (int) key == (int) oldValue)
//                                    tables.elementAt(i).getPages().elementAt(j).hashtables.elementAt(k).replace(oldName, oldValue, newValue);
//                            }

            }
            case "java.lang.String" -> {
                // PARSING
                key = strClusteringKey;
                // No need to parse


                // COMPARING

//                for (int i = 0; i < tables.size(); i++)
//                    if (tables.elementAt(i).getTableName().equals(strTableName))
//                        for (int j = 0; j < tables.elementAt(i).getPages().size(); j++)
//                            for (int k = 0; k < tables.elementAt(i).getPages().elementAt(j).hashtables.size(); k++) {
//                                Hashtable<String, Object> tempHash = tables.elementAt(i).getPages().elementAt(j).hashtables.elementAt(k);
//                                String newName = hashtableColumnNameValue.keys().nextElement();
//                                Object newValue = hashtableColumnNameValue.get(newName);
//                                String oldName = tempHash.keys().nextElement();
//                                Object oldValue = tempHash.get(oldName);
//                                if (newName.equals(oldName) && ((String) key).equals((String) oldValue))
//                                    tables.elementAt(i).getPages().elementAt(j).hashtables.elementAt(k).replace(oldName, oldValue, newValue);
//                            }
            }
            case "java.lang.Double" -> {
                // PARSING
                key = Double.parseDouble(strClusteringKey);

                // COMPARING


//                for (int i = 0; i < tables.size(); i++)
//                    if (tables.elementAt(i).getTableName().equals(strTableName))
//                        for (int j = 0; j < tables.elementAt(i).getPages().size(); j++)
//                            for (int k = 0; k < tables.elementAt(i).getPages().elementAt(j).hashtables.size(); k++) {
//                                Hashtable<String, Object> tempHash = tables.elementAt(i).getPages().elementAt(j).hashtables.elementAt(k);
//                                String newName = hashtableColumnNameValue.keys().nextElement();
//                                Object newValue = hashtableColumnNameValue.get(newName);
//                                String oldName = tempHash.keys().nextElement();
//                                Object oldValue = tempHash.get(oldName);
//                                if (newName.equals(oldName) && ((Double) key).equals((Double) oldValue))
//                                    tables.elementAt(i).getPages().elementAt(j).hashtables.elementAt(k).replace(oldName, oldValue, newValue);
//                            }

            }
            case "java.lang.Boolean" -> {
                // PARSING
                if (strClusteringKey.equals("True") || strClusteringKey.equals("true"))
                    key = true;
                else key = false;

                //COMPARING

//                for (int i = 0; i < tables.size(); i++)
//                    if (tables.elementAt(i).getTableName().equals(strTableName))
//                        for (int j = 0; j < tables.elementAt(i).getPages().size(); j++)
//                            for (int k = 0; k < tables.elementAt(i).getPages().elementAt(j).hashtables.size(); k++) {
//                                Hashtable<String, Object> tempHash = tables.elementAt(i).getPages().elementAt(j).hashtables.elementAt(k);
//                                String newName = hashtableColumnNameValue.keys().nextElement();
//                                Object newValue = hashtableColumnNameValue.get(newName);
//                                String oldName = tempHash.keys().nextElement();
//                                Object oldValue = tempHash.get(oldName);
//                                if (newName.equals(oldName) && (boolean) key == (boolean) oldValue)
//                                    tables.elementAt(i).getPages().elementAt(j).hashtables.elementAt(k).replace(oldName, oldValue, newValue);
//                            }

            }
            case "java.util.Date" -> {
                // PARSING
                key = new SimpleDateFormat("dd/MM/yyyy").parseObject(strClusteringKey);

                // COMPARING


//                for (int i = 0; i < tables.size(); i++)
//                    if (tables.elementAt(i).getTableName().equals(strTableName))
//                        for (int j = 0; j < tables.elementAt(i).getPages().size(); j++)
//                            for (int k = 0; k < tables.elementAt(i).getPages().elementAt(j).hashtables.size(); k++) {
//                                Hashtable<String, Object> tempHash = tables.elementAt(i).getPages().elementAt(j).hashtables.elementAt(k);
//                                String newName = hashtableColumnNameValue.keys().nextElement();
//                                Object newValue = hashtableColumnNameValue.get(newName);
//                                String oldName = tempHash.keys().nextElement();
//                                Object oldValue = tempHash.get(oldName);
//                                if (newName.equals(oldName) && ((Date) key).compareTo((Date) oldValue) == 0)
//                                    tables.elementAt(i).getPages().elementAt(j).hashtables.elementAt(k).replace(oldName, oldValue, newValue);
//                            }

            }
            case "java.awt.Polygon" -> {

                // PARSING
                String[] s = strClusteringKey.replace(",(", "#(").replace("(", "").replace(")", ".").split("#");
                int[] x = new int[s.length];
                int[] y = new int[s.length];
                for (int i = 0; i < s.length; i++) {
                    int xend = s[i].indexOf(",");
                    int yend = s[i].indexOf(".");
                    if (xend != -1) x[i] = Integer.parseInt(s[i].substring(0, xend));
                    if (yend != -1) y[i] = Integer.parseInt(s[i].substring(xend + 1, yend));
                }
                key = new Polygon(x, y, s.length);

                // COMPARING

                for (int i = 0; i < tables.size(); i++)
                    if (tables.elementAt(i).getTableName().equals(strTableName)) {
                        // DESERIALIZE

//                        tables.elementAt(i);
//                        tables.elementAt(i).setPages((Vector<Page>) Table.deserialize(tables.elementAt(i).getTableName()));
//                        for (int j = 0; j < tables.elementAt(i).getPages().size(); j++)
//                            for (int k = 0; k < tables.elementAt(i).getPages().elementAt(j).hashtables.size(); k++) {
//                                Hashtable<String, Object> tempHash = tables.elementAt(i).getPages().elementAt(j).hashtables.elementAt(k);
//                                String newName = hashtableColumnNameValue.keys().nextElement();
//                                Object newValue = hashtableColumnNameValue.get(newName);
//                                String oldName = tempHash.keys().nextElement();
//                                Object oldValue = tempHash.get(oldName);
//                                if (newName.equals(oldName) && equalPolygons((Polygon) key, (Polygon) oldValue))
//                                    tables.elementAt(i).getPages().elementAt(j).hashtables.elementAt(k).replace(oldName, oldValue, newValue);
//                            }

                        // SERIALIZE
                        tables.elementAt(i).serialize(tables.elementAt(i).getTableName());
                    }
            }
            default -> throw new DBAppException("This type is not supported!");
        }
    }


    /**
     * hashtableColumnNameValue holds the key and value.
     * This will be used in search to identify which rows/ tuples to delete.
     * hashtableColumnNameValue entries are ANDED together
     */
    @SuppressWarnings("unchecked")
    public void deleteFromTable(
            String strTableName,
            Hashtable<String, Object> hashtableColumnNameValue)
            throws DBAppException, IOException {

		/*

		table t = findTable(strTableName);
		Vector x = new Vector();//pages
		Hashtable zero = new Hashtable();
		//deserialize
		x.add(t.pages);

		page p=new page();
		for(int i = 0;i<x.size();i++) {
			p = (page) x.elementAt(i);
			p.deserialize(p, "Untitled.ser");
			for(int j=0;j<p.hashtables.size();j++)
				if(p.hashtables.elementAt(j).equals(hashtableColumnNameValue))
					p.hashtables.remove(j);
		}
		p.serialize(p, "Untitled.ser");

		 */
        for (int i = 0; i < tables.size(); i++)
            if (tables.elementAt(i).getTableName().equals(strTableName)) {
                // DESERIALIZE

//                tables.elementAt(i);
//                tables.elementAt(i).setPages((Vector<Page>) Table.deserialize(tables.elementAt(i).getTableName()));
//                for(int j = 0; j< tables.elementAt(i).getPages().size(); j++)
//                    for(int k = 0; k< tables.elementAt(i).getPages().elementAt(j).hashtables.size(); k++){
//                        Hashtable<String, Object> tempHash = tables.elementAt(i).getPages().elementAt(j).hashtables.elementAt(k);
//                        String name1 = hashtableColumnNameValue.keys().nextElement();
//                        Object val1 = hashtableColumnNameValue.get(name1);
//                        String name2 = tempHash.keys().nextElement();
//                        Object val2 = tempHash.get(val1);
//                        if(name1.equals(name2) && val1.equals(val2))
//                            tables.elementAt(i).getPages().elementAt(j).hashtables.remove(
//                                    tables.elementAt(i).getPages().elementAt(j).hashtables.elementAt(k));
//                    }

                // SERIALIZE
                tables.elementAt(i).serialize(tables.elementAt(i).getTableName());
            }
    }

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
            throw new DBAppException("IO Exception reading the file");
        }
    }

    public static void main(String[] args) throws IOException {

        String strClusteringKey = "(10,20),(30,30),(40,40),(50,60)";
        String s1 = strClusteringKey.replace(",(", "#(").replace("(", "").replace(")", ".");//10,20.#30,30.#40,40.#50,60.
        String[] s = s1.split("#");
        int[] x = new int[s.length];
        int[] y = new int[s.length];
        for (int i = 0; i < s.length; i++) {
            int xend = s[i].indexOf(",");
            int yend = s[i].indexOf(".");
            if (xend != -1) x[i] = Integer.parseInt(s[i].substring(0, xend));
            if (yend != -1) y[i] = Integer.parseInt(s[i].substring(xend + 1, yend));
        }
        Polygon temp = new Polygon(x, y, s.length);


        int[] i1 = new int[4];
        int[] i2 = new int[4];
        i1[0] = 10;
        i1[1] = 30;
        i1[2] = 40;
        i1[3] = 50;
        i2[0] = 20;
        i2[1] = 30;
        i2[2] = 40;
        i2[3] = 60;
        Polygon p = new Polygon(i1, i2, 4);

        System.out.println(equalPolygons(p, temp));

        Hashtable<String, String> h = new Hashtable<String, String>();
        h.put("abc", "123");
        String ColumnName = (String) h.keys().nextElement();
        String ColumnType = (String) h.get(ColumnName);
        System.out.println(ColumnName);
        System.out.println(ColumnType);
        System.out.println(h);

    }


}
