package src.DBGeneralEngine;


import java.io.*;
import java.util.*;

import src.Ref.Ref;
import src.Ref.GeneralRef;
import src.Ref.OverflowRef;


/**
 * The Table class represents a table structure within a database system.
 * It is serializable, allowing it to be stored and retrieved from persistent storage.
 * It holds information about the table's pages, maximum number of rows per page, primary key position, last assigned ID, table name, clustering key, and any associated tree indexes.
 */
public class Table implements Serializable {

    /**
     * Attributes
     * <p>
     *
     * pages                    ->  A Vector containing the table's data pages.
     * maxRowsInPage            ->  The maximum number of rows allowed on a single page within the table.
     * primaryPosition          ->  The position of the primary key column within the table schema. (zero-based indexing)
     * lastId                   ->  The last identifier value assigned to a record within the table. (used for auto-increment)
     * tableName                ->  The name of the table as defined in the database schema.
     * clusteringKey            ->  The name of the column used for clustering data within the table.
     * treeIndexColumnName      -> A Hashtable that maps column names to their corresponding TreeIndex instances.
     *                                  ( used for efficient data retrieval )
     */
    private Vector<String> pages = new Vector<>();
    private int maxRowsInPage;
    private int primaryPosition;
    private int lastId;
    private String tableName;
    private String clusteringKey;
    private final Hashtable<String, TreeIndex> treeIndexColumnName = new Hashtable<>();


    /**
     * Constructors
     * <p>
     *
     * Default constructor for the Table class. Initializes an empty Table object.
     */
    public Table() {

    }

    /**
     * Constructor that takes arguments to define all aspects of the table structure.
     *
     * @param pages A Vector containing the table's data pages.
     * @param maxRowsInPage The maximum number of rows allowed on a single page.
     * @param primaryPosition The position of the primary key column (zero-based indexing).
     * @param lastId The last identifier value assigned to a record.
     * @param tableName The name of the table.
     * @param clusteringKey The name of the column used for clustering data.
     */
    public Table(Vector<String> pages, int maxRowsInPage, int primaryPosition, int lastId, String tableName, String clusteringKey) {
        this.pages = pages;
        this.maxRowsInPage = maxRowsInPage;
        this.primaryPosition = primaryPosition;
        this.lastId = lastId;
        this.tableName = tableName;
        this.clusteringKey = clusteringKey;
    }


    /**
     * Getters & Setters
     * <p>
     *
     *
     * Retrieves the maximum number of rows allowed on a single page within the table.
     *
     * @return The maximum number of rows per page.
     */
    public int getMaxRowsInPage() {
        return maxRowsInPage;
    }

    /**
     * Retrieves the last identifier value assigned to a record within the table. (used for auto-increment)
     *
     * @return The last assigned identifier value.
     */
    public int getLastId() {
        return lastId;
    }

    /**
     * Retrieves the name of the column used for clustering data within the table.
     *
     * @return The name of the clustering key column.
     */
    public String getClusteringKey() {
        return clusteringKey;
    }

    /**
     * Retrieves the last identifier value assigned to a record within the table.
     * It allows for optional auto-increment.
     *
     * @param increment If true, increments the lastId value by 1 and returns the new value.
     *                  If false, simply returns the current lastId value.
     * @return The last assigned identifier value, potentially incremented.
     */
    public int getLastId(boolean increment) {
        if (increment)
            return lastId++;
        return lastId;
    }

    /**
     * Retrieves a reference to the internal Hashtable that maps column names to their corresponding TreeIndex instances.
     *
     * @return An unmodifiable reference to the Hashtable containing column name to TreeIndex mappings.
     */
    public Hashtable<String, TreeIndex> getColNameBTreeIndex() {
        return treeIndexColumnName;
    }

    /**
     * Retrieves a reference to the internal Vector containing the table's data pages.
     *
     * @return An unmodifiable reference to the Vector containing table page names.
     */
    public Vector<String> getPages() {
        return pages;
    }

    /**
     * Generates a new page name based on the table name and a sequential numbering scheme.
     *
     * @return A string representing the name of a new page for the table.
     */
    public String getNewPageName() {
        return tableName + ((pages.size() == 0) ? 0
                : Integer.parseInt((pages.get(pages.size() - 1)).substring(tableName.length())) + 1);
    }

    /**
     * Retrieves a reference to the internal Hashtable that maps column names to their corresponding TreeIndex instances.
     *
     * @return An unmodifiable reference to the Hashtable containing column name to TreeIndex mappings.
     */
    public Hashtable<String, TreeIndex> getTreeIndexColumnName() {
        return treeIndexColumnName;
    }

    /**
     * Retrieves the maximum value from the page at the given index.
     *
     * @param index The index of the page from which to retrieve the maximum value.
     * @return      The maximum value from the specified page.
     * @throws DBAppException If an error occurs during the process of deserializing the page.
     */
    public Comparable getMax(int index) throws DBAppException {
        // Get the page name from the pages list using the specified index
        String pageName = pages.get(index);
        // Deserialize the page using its name
        Page page = deserialize(pageName);
        // Get the last tuple from the page
        Tuple lastTuple = page.getTuples().get(page.size() - 1);
        // Get the value of the primary key attribute from the last tuple
        Object obj = lastTuple.getAttributes().get(primaryPosition);
        // Convert the value to Comparable and return it
        return (Comparable) obj;
    }


    /**
     * Retrieves the minimum value from the page at the specified index.
     *
     * @param index The index of the page from which to retrieve the minimum value.
     * @return The minimum value from the specified page.
     * @throws DBAppException If an error occurs during the process of deserializing the page.
     */
    public Comparable getMin(int index) throws DBAppException {
        // Get the page name from the pages list using the specified index
        String pageName = pages.get(index);
        // Deserialize the page using its name
        Page page = deserialize(pageName);
        // Get the minimum value from the first tuple in the page
        Comparable min = (Comparable) page.getTuples().get(0).getAttributes().get(primaryPosition);
        // Return the minimum value
        return min;
    }

    /**
     * Retrieves the zero-based index of the attribute that acts as the primary key within the table.
     *
     * @return  The index of the primary key attribute.
     */
    public int getPrimaryPosition() {
        return primaryPosition;
    }

    /**
     * Retrieves the name assigned to this table.
     *
     * @return  The name of the table.
     */
    public String getTableName() {
        return tableName;
    }

    /**
     * Retrieves the index of a specific data page within the table.
     * This method delegates the task of extracting the numeric suffix from the page name to the getSuffix method and uses that value as the index.
     *
     * @param pageName The name of the data page to locate.
     * @return  The index of the data page within the table, or -1 if not found.
     */
    public int getPageIndex(String pageName) {
        return getSuffix(pageName);
    }

    /**
     * Extracts the numeric suffix from a data page name.
     *
     * @param pageName The name of the data page to extract the suffix from.
     * @return The numeric suffix extracted from the page name.
     */
    public int getSuffix(String pageName) {
        return Integer.parseInt(pageName.substring(tableName.length()));
    }


    /**
     * Updates the value used for auto-incrementing record identifiers within the table.
     *
     * @param lastId The new value to be used for the next auto-incremented identifier.
     */
    public void setLastId(int lastId) {
        this.lastId = lastId;
    }

    /**
     * Sets the maximum number of rows allowed on a single data page within the table.
     *
     * @param maxRowsInPage The new maximum number of rows allowed per page.
     */
    public void setMaxRowsInPage(int maxRowsInPage) {
        this.maxRowsInPage = maxRowsInPage;
    }

    /**
     * Updates the internal collection of data page names associated with this table.
     *
     * @param pages A Vector containing the new set of data page names.
     */
    public void setPages(Vector<String> pages) {
        this.pages = pages;
    }

    /**
     * Sets the name of the attribute used for clustering data within the table.
     *
     * @param clusteringKey The name of the attribute to be used for clustering.
     */
    public void setClusteringKey(String clusteringKey) {
        this.clusteringKey = clusteringKey;
    }

    /**
     * Updates the name assigned to this table.
     *
     * @param name The new name for the table.
     */
    public void setTableName(String name) {
        tableName = name;
    }

    /**
     * Sets the zero-based index of the attribute that acts as the primary key within the table.
     *
     * @param pos The new index of the primary key attribute.
     */
    public void setPrimaryPosition(int pos) {
        primaryPosition = pos;
    }


    /**
     * above done
     * below still
     * now  resume
     * breakpoint
     */

    /**
     * Serialize the current state of the pages to the specified file address.
     *
     * @param address The address of the file to serialize the pages to.
     */
    public void serialize(String address) {
        try {
            // Create a FileOutputStream for the specified file address
            FileOutputStream fileOut = new FileOutputStream(address);
            // Create an ObjectOutputStream to write objects to the FileOutputStream
            ObjectOutputStream out = new ObjectOutputStream(fileOut);
            // Write the pages object to the ObjectOutputStream
            out.writeObject(pages);
            // Close the ObjectOutputStream
            out.close();
            // Close the FileOutputStream
            fileOut.close();
        } catch (IOException i) { // Catch IO Exception
            i.printStackTrace();
        }
    }


    /**
     * Deserialize a page object from disk with the given name.
     *
     * @param name The name of the page to deserialize.
     * @return The deserialized Page object.
     * @throws DBAppException If an error occurs during deserialization.
     */
    public static Page deserialize(String name) throws DBAppException {
        try {
            // Open the file input stream for the page file
            FileInputStream fileIn = new FileInputStream("data: " + name + ".class");
            // Initialize an object input stream to read from the file input stream
            ObjectInputStream in = new ObjectInputStream(fileIn);
            // Read the Page object from the object input stream
            Page pageIn = (Page) in.readObject();
            // Close the object input stream
            in.close();
            // Close the file input stream
            fileIn.close();
            // Return the deserialized Page object
            return pageIn;
        } catch (IOException e) { // Catch IO Exception
            throw new DBAppException("IO Exception");
        } catch (ClassNotFoundException e) { // Catch Class Not Found Exception
            throw new DBAppException("Class Not Found Exception");
        }
    }



    /**
     * Parses the given object into a comparable value based on the metadata of the specified table.
     *
     * @param tableName the name of the table
     * @param strKey the object to parse
     * @return a comparable value parsed from the object
     * @throws DBAppException if the object cannot be parsed or if an error occurs during parsing
     */
    public static Comparable parseObject(String tableName, Object strKey) throws DBAppException {
        try {
            Vector meta = DBApp.readFile("data/metadata.csv");
            Comparable key = null;
            for (Object obj : meta) {
                String[] current = (String[]) obj;
                if (current[0].equals(tableName) && current[3].equals("True")) {
                    key = switch (current[2]) {
                        case "java.lang.Integer" -> {
                            assert (strKey) instanceof Integer;
                            yield (Integer) (strKey);
                        }
                        case "java.lang.Double" -> {
                            assert (strKey) instanceof Double;
                            yield (Double) (strKey);
                        }
                        case "java.util.Date" -> {
                            assert (strKey) instanceof Date;
                            yield (Date) (strKey);
                        }
                        case "java.lang.Boolean" -> {
                            assert (strKey) instanceof Boolean;
                            yield (Boolean) (strKey);
                        }
                        case "java.awt.Polygon" -> {
                            assert strKey instanceof CustomPolygon;
                            yield (CustomPolygon) strKey;
                        }
                        default -> throw new DBAppException("Invalid key");
                    };
                }
            }
            return key;
        } catch (ClassCastException e) {
            throw new DBAppException("Class Cast Exception");
        }
    }


    /**
     * Searches for a key in the specified table and returns its page name and tuple index.
     *
     * @param tableName the name of the table to search in
     * @param strKey the key to search for
     * @return a string representing the page name and tuple index of the key
     * @throws DBAppException if the key does not exist in the table or if an error occurs during parsing
     */
    public String SearchInTable(String tableName, Object strKey) throws DBAppException {
        return SearchInTable(parseObject(tableName, strKey));
    }


    /**
     * Performs a binary search to find the page and tuple index of a given key in the table.
     *
     * @param key the key to search for
     * @return a string representing the page name and tuple index of the key
     * @throws DBAppException if the key does not exist in the table or if a ClassCastException occurs
     */
    public String SearchInTable(Comparable key) throws DBAppException {
        try {

            Table table = this;
            Vector<String> pages = table.getPages();

            for (String str : pages) {
                Page page = Table.deserialize(str);
                int initialLength = 0;
                int finalLength = page.getTuples().size() - 1;

                while (initialLength <= finalLength) {
                    int midLength = initialLength + (finalLength - initialLength) / 2;

                    if (key.compareTo((page.getTuples().get(midLength)).getAttributes().get(table.getPrimaryPosition())) == 0) {
                        while (midLength > 0 && key
                                .compareTo((page.getTuples().get(midLength - 1)).getAttributes().get(table.getPrimaryPosition())) == 0) {
                            midLength--;
                        }
                        return page.getPageName() + "#" + midLength;
                    }

                    if (key.compareTo((page.getTuples().get(midLength)).getAttributes().get(table.getPrimaryPosition())) < 0)
                        finalLength = midLength - 1;

                    else
                        initialLength = midLength + 1;
                }
            }

            throw new DBAppException("Does not exist in table");
        } catch (ClassCastException e) {
            throw new DBAppException("Class Cast Exception");
        }
    }


    /**
     * Checks if a tuple satisfies the given SQL term.
     *
     * @param sqlTerm the SQL term representing the condition to check
     * @param tuple the tuple to be checked
     * @param position the position of the attribute in the tuple
     * @return true if the tuple satisfies the condition, false otherwise
     * @throws DBAppException if an invalid operator is encountered
     */
    private boolean checkTupleInCurrent(SQLTerm sqlTerm, Tuple tuple, int position) throws DBAppException {
        Comparable comparableOne = (Comparable) tuple.getAttributes().get(position);
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


    /**
     * Checks if a tuple satisfies the specified SQL terms and operators recursively.
     *
     * @param arrSQLTerms an array of SQLTerm objects representing the search conditions
     * @param arrOperators an array of strings representing the logical operators between conditions
     * @param integerArrayList an ArrayList of integers representing the positions of attributes in the table
     * @param index the index used to traverse the arrays of SQL terms and operators
     * @param tuple the tuple to be checked against the conditions
     * @return true if the tuple satisfies the conditions, false otherwise
     * @throws DBAppException if an error occurs during the operation
     */
    private boolean tupleMetConditions(
            SQLTerm[] arrSQLTerms,
            String[] arrOperators,
            ArrayList<Integer> integerArrayList,
            int index,
            Tuple tuple) throws DBAppException {
        return switch (arrOperators[index - 1].toLowerCase()) {
            case "or" -> (index == 1)
                    ? checkTupleInCurrent(arrSQLTerms[0], tuple, integerArrayList.get(0))
                    || checkTupleInCurrent(arrSQLTerms[1], tuple, integerArrayList.get(1))
                    : tupleMetConditions(arrSQLTerms, arrOperators, integerArrayList, index - 1, tuple)
                    || checkTupleInCurrent(arrSQLTerms[index], tuple, integerArrayList.get(index));
            case "and" -> (index == 1)
                    ? checkTupleInCurrent(arrSQLTerms[0], tuple, integerArrayList.get(0))
                    && checkTupleInCurrent(arrSQLTerms[1], tuple, integerArrayList.get(1))
                    : tupleMetConditions(arrSQLTerms, arrOperators, integerArrayList, index - 1, tuple)
                    && checkTupleInCurrent(arrSQLTerms[index], tuple, integerArrayList.get(index));
            case "xor" -> (index == 1)
                    ? checkTupleInCurrent(arrSQLTerms[0], tuple, integerArrayList.get(0))
                    ^ checkTupleInCurrent(arrSQLTerms[1], tuple, integerArrayList.get(1))
                    : tupleMetConditions(arrSQLTerms, arrOperators, integerArrayList, index - 1, tuple)
                    ^ checkTupleInCurrent(arrSQLTerms[index], tuple, integerArrayList.get(index));
            default -> false;
        };
    }


    /**
     * Finds the exclusive OR (XOR) of two ArrayLists of tuples, i.e., tuples that appear in either list but not in both.
     *
     * @param tupleArrayList1 the first ArrayList of tuples
     * @param tupleArrayList2 the second ArrayList of tuples
     * @return an ArrayList containing tuples that appear in either list but not in both
     */
    private ArrayList<Tuple> xorSets(ArrayList<Tuple> tupleArrayList1, ArrayList<Tuple> tupleArrayList2) {
        return differenceSets(orSets(tupleArrayList1, tupleArrayList2), andSets(tupleArrayList1, tupleArrayList2));
    }


    /**
     * Finds the difference between two ArrayLists of tuples, i.e., tuples that appear in the first list but not in the second.
     *
     * @param tupleArrayList1 the first ArrayList of tuples
     * @param tupleArrayList2 the second ArrayList of tuples
     * @return an ArrayList containing tuples that appear in the first list but not in the second
     */
    public ArrayList<Tuple> differenceSets(ArrayList<Tuple> tupleArrayList1, ArrayList<Tuple> tupleArrayList2) {
        ArrayList<Tuple> result = new ArrayList<>();

        HashSet<Tuple> hashsetOne = new HashSet<>(tupleArrayList1);
        HashSet<Tuple> hashsetTwo = new HashSet<>(tupleArrayList2);

        for (Tuple cur : tupleArrayList1) {
            if (hashsetOne.contains(cur))
                continue;
            hashsetOne.add(cur);
            if (!hashsetTwo.contains(cur)) {
                result.add(cur);
            }
        }
        return result;
    }


    /**
     * Finds the intersection of two ArrayLists of tuples, i.e., tuples that appear in both lists.
     *
     * @param tupleArrayList1 the first ArrayList of tuples
     * @param tupleArrayList2 the second ArrayList of tuples
     * @return an ArrayList containing tuples that appear in both input ArrayLists
     */
    public ArrayList<Tuple> andSets(ArrayList<Tuple> tupleArrayList1, ArrayList<Tuple> tupleArrayList2) {
        ArrayList<Tuple> result = new ArrayList<>();

        HashSet<Tuple> hashsetOne = new HashSet<>(tupleArrayList1);
        HashSet<Tuple> hashsetTwo = new HashSet<>();
        for (Tuple cur : tupleArrayList2) {
            if (!hashsetTwo.contains(cur)) {
                hashsetTwo.add(cur);
                if (hashsetOne.contains(cur)) {
                    result.add(cur);
                }
            }
        }
        return result;
    }


    /**
     * Combines two ArrayLists of tuples into one, removing any duplicate tuples.
     *
     * @param tupleArrayList1 the first ArrayList of tuples
     * @param tupleArrayList2 the second ArrayList of tuples
     * @return a new ArrayList containing all unique tuples from both input ArrayLists
     */
    private ArrayList<Tuple> orSets(ArrayList<Tuple> tupleArrayList1, ArrayList<Tuple> tupleArrayList2) {
        Set<Tuple> tupleSet = new HashSet<>();
        tupleSet.addAll(tupleArrayList1);
        tupleSet.addAll(tupleArrayList2);
        return new ArrayList<>(tupleSet);
    }


    /**
     * Retrieves an array of tuples based on the specified column name, value, and operator.
     *
     * @param strColumnName the name of the column to perform the search on
     * @param objValue the value to search for
     * @param strOperator the operator to use in the search
     * @param position the position of the column in the table
     * @return an ArrayList of tuples matching the search criteria
     * @throws DBAppException if an error occurs during the search operation
     */
    private ArrayList<Tuple> getArrayOfTuples(
            String strColumnName,
            Object objValue,
            String strOperator,
            int position) throws DBAppException {

        if (strOperator.equals("!="))
            return goLinear(objValue, strOperator, position);

        return (treeIndexColumnName.containsKey(strColumnName))
                ? goWithIndex(strColumnName, objValue, strOperator, position)
                : (strColumnName.equals(clusteringKey)) ? goBinary(objValue, strOperator, position)
                : goLinear(objValue, strOperator, position);
    }


    /**
     * Retrieves the position (index) of a column in a tuple based on its name.
     *
     * @param strColumnName the name of the column to search for
     * @param metaOfTable the metadata of the table containing column information
     * @return the position of the column in the tuple, or -1 if the column is not found
     */
    private int getColumnPositionInTuple(String strColumnName, Vector<String[]> metaOfTable) {

        for (int i = 0; i < metaOfTable.size(); i++) {
            if (metaOfTable.get(i)[1].equals(strColumnName))
                return i;
        }
        return -1;
    }


    /**
     * Checks if the given string represents a valid comparison operator.
     * Valid operators include "=", "!=", ">", ">=", "<", and "<=".
     *
     * @param strOperator the string representing the operator to check
     * @return true if the operator is valid, false otherwise
     */
    private boolean validOperator(String strOperator) {
        return strOperator.equals("=") || strOperator.equals("!=") || strOperator.equals(">")
                || strOperator.equals(">=") || strOperator.equals("<") || strOperator.equals("<=");
    }


    /**
     * Determines the appropriate linear search method based on the operator,
     * and performs the search operation accordingly.
     *
     * @param objValue the value to search for
     * @param strOperator the operator to use in the search (">", ">=", "<", "<=", "=", "!=")
     * @param position the position of the attribute in the table
     * @return an ArrayList of tuples matching the search criteria
     * @throws DBAppException if an error occurs during the search operation
     */
    private ArrayList<Tuple> goLinear(
            Object objValue,
            String strOperator,
            int position) throws DBAppException {

        return switch (strOperator) {
            case ">", ">=" -> mtOrMtlLinear(objValue, strOperator, position);
            case "<", "<=" -> ltOrLtlLinear(objValue, strOperator, position);
            case "=" -> equalsLinear(objValue, position);
            case "!=" -> notEqualsLinear(objValue, position);
            default -> new ArrayList<>();
        };
    }


    /**
     * Performs a linear search operation on the table for values not equal to the given value,
     * based on the given value and the position of the attribute in the table.
     *
     * @param objValue the value to compare against
     * @param position the position of the attribute in the table
     * @return an ArrayList of tuples not matching the given value
     * @throws DBAppException if an error occurs during the search operation
     */
    private ArrayList<Tuple> notEqualsLinear(Object objValue, int position) throws DBAppException {

        ArrayList<Tuple> result = new ArrayList<>();
        for (String s : pages) {
            Page page = deserialize(s);
            for (int j = 0; j < page.getTuples().size(); j++) {
                Comparable grantKey = (Comparable) page.getTuples().get(j).getAttributes().get(position);
                Comparable obj = (Comparable) objValue;
                if (grantKey.compareTo(obj) != 0) {
                    result.add(page.getTuples().get(j));
                }
                else if ((grantKey instanceof CustomPolygon) && !grantKey.equals(obj)) {
                    result.add(page.getTuples().get(j));
                }

            }

        }
        return result;
    }


    /**
     * Performs a linear search operation on the table for exact matches,
     * based on the given value and the position of the attribute in the table.
     *
     * @param objValue the value to search for
     * @param position the position of the attribute in the table
     * @return an ArrayList of tuples matching the search criteria
     * @throws DBAppException if an error occurs during the search operation
     */
    private ArrayList<Tuple> equalsLinear(Object objValue, int position) throws DBAppException {

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


    /**
     * Performs a linear search operation on the leftmost tree index leaf or leftmost tree index leaf with linear probing,
     * based on the given value and operator.
     *
     * @param objValue the value to search for
     * @param strOperator the operator to use in the search
     * @param position the position of the attribute in the table
     * @return an ArrayList of tuples matching the search criteria
     * @throws DBAppException if an error occurs during the search operation
     */
    private ArrayList<Tuple> ltOrLtlLinear(
            Object objValue,
            String strOperator,
            int position) throws DBAppException {

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


    /**
     * Performs a linear search operation on the main tree or main tree with linear probing,
     * based on the given value and operator.
     *
     * @param objValue the value to search for
     * @param strOperator the operator to use in the search
     * @param position the position of the attribute in the table
     * @return an ArrayList of tuples matching the search criteria
     * @throws DBAppException if an error occurs during the search operation
     */
    private ArrayList<Tuple> mtOrMtlLinear(
            Object objValue,
            String strOperator,
            int position) throws DBAppException {

        ArrayList<Tuple> result = new ArrayList<>();
        for (int i = pages.size() - 1; i >= 0; i--) {
            if (getMax(i).compareTo(objValue) < 0)
                break;
            if (getMax(i).compareTo(objValue) == 0 && strOperator.length() == 1)
                break;
            Page page = deserialize(pages.get(i));
            int pageMaxSize = page.getTuples().size() - 1;
            while (pageMaxSize >= 0 && ((Comparable) page.getTuples().get(pageMaxSize).getAttributes().get(position))
                    .compareTo(objValue) > 0) {
                result.add(0, page.getTuples().get(pageMaxSize));
                pageMaxSize--;
            }
            if (strOperator.length() == 2) {
                while (pageMaxSize >= 0 && ((Comparable) page.getTuples().get(pageMaxSize).getAttributes().get(position))
                        .compareTo(objValue) == 0) {
                    result.add(0, page.getTuples().get(pageMaxSize));
                    pageMaxSize--;
                }
            }
        }
        return result;
    }


    /**
     * Determines the appropriate binary search method based on the operator,
     * and performs the search operation accordingly.
     *
     * @param objValue the value to search for
     * @param strOperator the operator to use in the search (">", ">=", "<", "<=", "=")
     * @param position the position of the attribute in the table
     * @return an ArrayList of tuples matching the search criteria
     * @throws DBAppException if an error occurs during the search operation
     */
    private ArrayList<Tuple> goBinary(
            Object objValue,
            String strOperator,
            int position) throws DBAppException {
        return switch (strOperator) {
            case ">", ">=" -> mtOrMtlBinary(objValue, strOperator, position);
            case "<", "<=" -> ltOrLtlBinary(objValue, strOperator, position);
            case "=" -> equalsBinary(objValue, position);
            default -> new ArrayList<>();
        };
    }


    /**
     * Performs a search operation using binary search on the table for exact matches,
     * based on the given value and the position of the attribute in the table.
     *
     * @param objValue the value to search for
     * @param position the position of the attribute in the table
     * @return an ArrayList of tuples matching the search criteria
     * @throws DBAppException if an error occurs during the search operation
     */
    private ArrayList<Tuple> equalsBinary(Object objValue, int position) throws DBAppException {

        String[] searchResult = SearchInTable(tableName, objValue).split("#");
        String startPage = searchResult[0];
        int startPageIndex = getPageIndex(startPage);
        int startTupleIndex = Integer.parseInt(searchResult[1]);
        ArrayList<Tuple> tupleArrayList = new ArrayList<>();

        for (int pageIndex = startPageIndex, tupleIndex = startTupleIndex; pageIndex < pages.size(); pageIndex++, tupleIndex = 0) {
            if (getMin(pageIndex).compareTo(objValue) > 0)
                break;
            Page currentPage = deserialize(pages.get(pageIndex));
            Comparable comparable;
            while (tupleIndex < currentPage.getTuples().size()
                    && (comparable = (Comparable) currentPage.getTuples().get(tupleIndex).getAttributes().get(position))
                    .compareTo(objValue) == 0)
                if (!(comparable instanceof CustomPolygon) || comparable.equals(objValue))
                    tupleArrayList.add(currentPage.getTuples().get(tupleIndex++));
        }

        return tupleArrayList;
    }


    /**
     * Performs a search operation using binary search on the leftmost tree index leaf or linear probing if binary search is not available,
     * based on the given value and operator.
     *
     * @param objValue the value to search for
     * @param strOperator the operator to use in the search
     * @param position the position of the column in the table
     * @return an ArrayList of tuples matching the search criteria
     * @throws DBAppException if an error occurs during the search operation
     */
    private ArrayList<Tuple> ltOrLtlBinary(
            Object objValue,
            String strOperator,
            int position) throws DBAppException {
        return ltOrLtlLinear(objValue, strOperator, position);
    }


    /**
     * Performs a search operation using binary search on the main tree or linear probing if binary search is not available,
     * based on the given value and operator.
     *
     * @param objValue the value to search for
     * @param strOperator the operator to use in the search
     * @param position the position of the column in the table
     * @return an ArrayList of tuples matching the search criteria
     * @throws DBAppException if an error occurs during the search operation
     */
    private ArrayList<Tuple> mtOrMtlBinary(
            Object objValue,
            String strOperator,
            int position) throws DBAppException {
        return mtOrMtlLinear(objValue, strOperator, position);

    }


    /**
     * Determines the appropriate index-based search method based on the operator,
     * and performs the search operation accordingly.
     *
     * @param strColumnName the name of the column to perform the search on
     * @param objValue the value to search for
     * @param strOperator the operator to use in the search (">", ">=", "<", "<=", "=")
     * @param position the position of the column in the table
     * @return an ArrayList of tuples matching the search criteria
     * @throws DBAppException if an error occurs during the search operation
     */
    private ArrayList<Tuple> goWithIndex(
            String strColumnName,
            Object objValue,
            String strOperator,
            int position) throws DBAppException {
        return switch (strOperator) {
            case ">", ">=" -> mtOrMtlIndex(strColumnName, objValue, strOperator, position);
            case "<", "<=" -> ltOrLtlIndex(strColumnName, objValue, strOperator, position);
            case "=" -> equalsIndex(strColumnName, objValue, strOperator, position);
            default -> new ArrayList<>();
        };
    }


    /**
     * Performs a search operation on the tree index for exact matches,
     * based on the given column name and value.
     *
     * @param strColumnName the name of the column to perform the search on
     * @param objValue the value to search for
     * @param strOperator the operator to use in the search (unused in this context)
     * @param position the position of the column in the table
     * @return an ArrayList of tuples matching the search criteria
     * @throws DBAppException if an error occurs during the search operation
     */
    private ArrayList<Tuple> equalsIndex(
            String strColumnName,
            Object objValue,
            String strOperator,
            int position) throws DBAppException {
        ArrayList<Tuple> result = new ArrayList<>();
        String lastPage = pages.get(pages.size() - 1);
        int lastPageMaxNum = Integer.parseInt(lastPage.substring(tableName.length()));
        boolean[] visited = new boolean[lastPageMaxNum + 1];
        TreeIndex treeIndex = treeIndexColumnName.get(strColumnName);
        GeneralRef resultReference = treeIndex.search((Comparable) objValue);
        if (resultReference == null)
            return result;
        ArrayList<Ref> referenceList = resultReference.getAllRef();
        for (Ref currentReference : referenceList) {
            String pageName = currentReference.getPage();
            int currentPageNo = Integer.parseInt(pageName.substring(tableName.length()));
            if (visited[currentPageNo])
                continue;
            addToResultSet(result, pageName, position, objValue, strOperator);
            visited[currentPageNo] = true;
        }
        return result;
    }


    /**
     * Performs a search operation on either the leftmost tree index leaf or linear probing,
     * based on the given column name, value, and operator.
     *
     * @param strColumnName the name of the column to perform the search on
     * @param objValue the value to search for
     * @param strOperator the operator to use in the search ("<" or "<=")
     * @param position the position of the column in the table
     * @return an ArrayList of tuples matching the search criteria
     * @throws DBAppException if an error occurs during the search operation
     */
    private ArrayList<Tuple> ltOrLtlIndex(
            String strColumnName,
            Object objValue,
            String strOperator,
            int position) throws DBAppException {

        if (strColumnName.equals(clusteringKey))
            return ltOrLtlLinear(objValue, strOperator, position);

        ArrayList<Tuple> result = new ArrayList<>();
        String lastPage = pages.get(pages.size() - 1);
        int lastPageMaxNum = Integer.parseInt(lastPage.substring(tableName.length()));
        boolean[] visited = new boolean[lastPageMaxNum + 1];
        TreeIndex treeIndex = treeIndexColumnName.get(strColumnName);
        LeafNode LeafNode = treeIndex.getLeftmostLeaf();
        while (LeafNode != null) {
            int i;
            for (i = 0; i < LeafNode.getNumberOfKeys(); i++) {
                GeneralRef generalRef = LeafNode.getRecord(i);
                if (LeafNode.getKey(i).compareTo(objValue) > 0)
                    break;
                if (LeafNode.getKey(i).compareTo(objValue) == 0 && strOperator.length() == 1)
                    break;
                Set<Ref> ref = fillInRef(generalRef);
                for (Ref r : ref) {
                    String pageName = r.getPage();
                    int currentPageNo = Integer.parseInt(pageName.substring(tableName.length()));
                    if (visited[currentPageNo])
                        continue;
                    addToResultSet(result, pageName, position, objValue, strOperator);
                    visited[currentPageNo] = true;
                }

            }
            if (i < LeafNode.getNumberOfKeys())
                break;
        }
        return result;
    }


    /**
     * Adds tuples from a page to the result list based on the specified operator.
     *
     * @param result the list to add tuples to
     * @param pageName the name of the page to extract tuples from
     * @param position the position of the attribute to compare
     * @param objValue the value to compare against
     * @param strOperator the operator to use for comparison ("<", "<=", "=", ">", ">=")
     * @throws DBAppException if an error occurs during the process or if the operator is invalid
     */
    private void addToResultSet(
            ArrayList<Tuple> result,
            String pageName, int position,
            Object objValue,
            String strOperator)
            throws DBAppException {
        switch (strOperator) {
            case ("<") -> addToResultSetLESS(result, pageName, position, objValue);
            case ("<=") -> addToResultSetLESSorEQUAL(result, pageName, position, objValue);
            case ("=") -> addToResultSetEQUAL(result, pageName, position, objValue);
            case (">") -> addToResultSetMORE(result, pageName, position, objValue);
            case (">=") -> addToResultSetMOREorEQUAL(result, pageName, position, objValue);
            default -> throw new DBAppException("DEFAULT OPERATOR");
        }
    }


    /**
     * Adds tuples from a page to the result list if their attribute at the given position
     * is strictly less than the specified value.
     *
     * @param result the list to add tuples to
     * @param pageName the name of the page to extract tuples from
     * @param position the position of the attribute to compare
     * @param objValue the value to compare against
     * @throws DBAppException if an error occurs during the process
     */
    private void addToResultSetLESS(
            ArrayList<Tuple> result,
            String pageName,
            int position,
            Object objValue)
            throws DBAppException {
        Page page = deserialize(pageName);
        for (int i = 0; i < page.getTuples().size(); i++) {
            if (((Comparable) objValue).compareTo(page.getTuples().get(i).getAttributes().get(position)) > 0)
                result.add(page.getTuples().get(i));
        }
    }


    /**
     * Adds tuples from a page to the result list if their attribute at the given position
     * is less than or equal to the specified value.
     *
     * @param result the list to add tuples to
     * @param pageName the name of the page to extract tuples from
     * @param position the position of the attribute to compare
     * @param objValue the value to compare against
     * @throws DBAppException if an error occurs during the process
     */
    private void addToResultSetLESSorEQUAL(
            ArrayList<Tuple> result,
            String pageName,
            int position,
            Object objValue) throws DBAppException {
        Page page = deserialize(pageName);
        for (int i = 0; i < page.getTuples().size(); i++) {
            if (((Comparable) objValue).compareTo(page.getTuples().get(i).getAttributes().get(position)) >= 0)
                result.add(page.getTuples().get(i));
        }
    }


    /**
     * Adds tuples from a page to the result list if their attribute at the given position
     * is equal to the specified value.
     *
     * @param result the list to add tuples to
     * @param pageName the name of the page to extract tuples from
     * @param position the position of the attribute to compare
     * @param objValue the value to compare against
     * @throws DBAppException if an error occurs during the process
     */
    private void addToResultSetEQUAL(
            ArrayList<Tuple> result,
            String pageName,
            int position,
            Object objValue) throws DBAppException {
        Page page = deserialize(pageName);
        for (int i = 0; i < page.getTuples().size(); i++) {
            Comparable objectValue = (Comparable) objValue;
            Tuple currentTuple = page.getTuples().get(i);
            Comparable currentKey = (Comparable) currentTuple.getAttributes().get(position);
            if (objectValue.compareTo(currentKey) == 0) {
                if (!(objValue instanceof CustomPolygon) || objValue.equals(currentKey))
                    result.add(page.getTuples().get(i));
            }
        }
    }


    /**
     * Adds tuples from a page to the result list if their attribute at the given position
     * is strictly greater than the specified value.
     *
     * @param result the list to add tuples to
     * @param pageName the name of the page to extract tuples from
     * @param pos the position of the attribute to compare
     * @param objValue the value to compare against
     * @throws DBAppException if an error occurs during the process
     */
    private void addToResultSetMORE(
            ArrayList<Tuple> result,
            String pageName,
            int pos,
            Object objValue) throws DBAppException {
        Page page = deserialize(pageName);
        for (int i = 0; i < page.getTuples().size(); i++) {
            if (((Comparable) objValue).compareTo(page.getTuples().get(i).getAttributes().get(pos)) < 0)
                result.add(page.getTuples().get(i));
        }
    }


    /**
     * Adds tuples from a page to the result list if their attribute at the given position
     * is greater than or equal to the specified value.
     *
     * @param result the list to add tuples to
     * @param pageName the name of the page to extract tuples from
     * @param position the position of the attribute to compare
     * @param objValue the value to compare against
     * @throws DBAppException if an error occurs during the process
     */
    private void addToResultSetMOREorEQUAL(
            ArrayList<Tuple> result,
            String pageName,
            int position,
            Object objValue) throws DBAppException {
        Page page = deserialize(pageName);
        for (int i = 0; i < page.getTuples().size(); i++) {
            if (((Comparable) objValue).compareTo(page.getTuples().get(i).getAttributes().get(position)) <= 0)
                result.add(page.getTuples().get(i));
        }
    }


    /**
     * Fills in the set of references based on the given general reference.
     *
     * @param generalRef the general reference to fill in the references from
     * @return a set of references
     * @throws DBAppException if an error occurs during the process
     */
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


    /**
     * Performs a search operation on either the main tree or the main tree with linear probing,
     * based on the given column name, value, and operator.
     *
     * @param strColumnName the name of the column to perform the search on
     * @param objValue the value to search for
     * @param strOperator the operator to use in the search ("<" or "<=")
     * @param position the position of the column in the table
     * @return an ArrayList of tuples matching the search criteria
     * @throws DBAppException if an error occurs during the search operation
     */
    private ArrayList<Tuple> mtOrMtlIndex(
            String strColumnName,
            Object objValue,
            String strOperator,
            int position) throws DBAppException {

        if (strColumnName.equals(clusteringKey))
            return mtOrMtlLinear(objValue, strOperator, position);
        else {
            ArrayList<Tuple> results = new ArrayList<>();
            String lastPage = pages.get(pages.size() - 1);
            int lastPageMaxNum = Integer.parseInt(lastPage.substring(tableName.length()));
            boolean[] visited = new boolean[lastPageMaxNum + 1];
            TreeIndex treeIndex = treeIndexColumnName.get(strColumnName);
            ArrayList referenceList = strOperator.equals(">") ? treeIndex.searchMT((Comparable) objValue)
                    : treeIndex.searchMTE((Comparable) objValue);
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


    /**
     * Returns a string representation of the table, including:
     * its name, clustering key, pages, indexed columns, and indexes.
     *
     * @return a string representation of the table
     */
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
        for (String col : treeIndexColumnName.keySet()) {
            stringBuilder.append(col).append("\t");
        }
        stringBuilder.append("Indexes: \n");
        for (String col : treeIndexColumnName.keySet()) {
            stringBuilder.append(col).append("\n");
            stringBuilder.append(treeIndexColumnName.get(col)).append("\n");
        }
        return stringBuilder.toString();
    }

    public static void main(String[] args) throws IOException {

        // initialize here

        Hashtable<String, String> hashtable = new Hashtable<>();
        String line;
        ArrayList<String> tableNames = new ArrayList<>();
        BufferedReader bufferedReader = new BufferedReader(new FileReader("datafile.csv"));

        while ((line = bufferedReader.readLine()) != null) {
            // use comma as separator
            String[] data = line.split(",");
            hashtable.put(data[4], data[0]);
        }
        System.out.println(hashtable);

    }


}
