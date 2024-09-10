package src.DBGeneralEngine;

import java.io.*;

import src.Ref.Ref;

import java.util.ArrayList;
import java.util.Vector;

public class OverflowPage implements Serializable {

    /**
     * Attributes
     *
     * nextRef -> The reference to the next overflow page in the linked list of overflow pages.
     * refs  -> A Vector that holds references (Ref objects) contained in the overflow page.
     * maxNodeSize -> The maximum number of references that this overflow page can hold.
     * pageName -> The name of the overflow page.
     */
    private String nextRef;
    private Vector<Ref> refs;
    private final int maxNodeSize;
    private String pageName;


    /**
     * Constructor
     * Initializes the page with a maximum node size
     * Creates a Vector to hold references
     * Generates a unique page name based on metadata.
     *
     * @param maxNodeSize the maximum number of references that this overflow page can hold
     * @throws DBAppException if an error occurs during initialization
     */
    public OverflowPage(int maxNodeSize) throws DBAppException {
        this.maxNodeSize = maxNodeSize;
        refs = new Vector<>(maxNodeSize);
        nextRef = null;
        String lastRef = getFromMetaDataTree();
        pageName = "OverflowPage" + lastRef;
    }


    /**
     * Getters & Setters
     * <p>
     *
     * Returns the Vector of references stored in the overflow page.
     *
     * @return a Vector of Ref objects associated with the overflow page
     */
    public Vector<Ref> getRefs() {
        return refs;
    }

    /**
     * Sets the Vector of references for the overflow page to the specified Vector.
     *
     * @param refs the new Vector of Ref objects to be set for the overflow page
     */
    public void setRefs(Vector<Ref> refs) {
        this.refs = refs;
    }

    /**
     * Returns the name of the overflow page.
     *
     * @return the name of the overflow page
     */
    public String getPageName() {
        return pageName;
    }

    /**
     * Sets the name of the overflow page to the specified string.
     *
     * @param pageName the new name to be set for the overflow page
     */
    public void setPageName(String pageName) {
        this.pageName = pageName;
    }


    /**
     * Returns the reference to the next overflow page, if it exists.
     *
     * @return the name of the next overflow page, or null if there is no next page
     */
    public String getNext() {
        return nextRef;
    }


    /**
     * Retrieves the next overflow page object if it exists.
     *
     * @return the next OverflowPage object, or null if there is no next page
     * @throws DBAppException if an error occurs during deserialization
     */
    public OverflowPage getNext1() throws DBAppException {

        if (nextRef == null)
            return null;

        return deserialize(nextRef);
    }


    /**
     * Sets the reference to the next overflow page.
     *
     * @param next the name of the next overflow page to be set
     */
    public void setNext(String next) {
        this.nextRef = next;
    }


    /**
     * Returns the total size of all references in the current and any subsequent overflow pages.
     *
     * @return the total number of references across all linked overflow pages
     * @throws DBAppException if an error occurs during the total size calculation
     */
    public int getTotalSize() throws DBAppException {
        if (nextRef == null)
            return refs.size();

        OverflowPage overflowPage = deserialize(nextRef);
        return refs.size() + overflowPage.getTotalSize();
    }


    /**
     * Retrieves all references from the current overflow page and any subsequent pages.
     *
     * @return an ArrayList containing all Ref objects from this and any linked overflow pages
     * @throws DBAppException if an error occurs during retrieval
     */
    public ArrayList<Ref> getAllRefs() throws DBAppException {
        ArrayList<Ref> refRes = new ArrayList<>(refs);
        if (nextRef != null) {
            try {
                refRes.addAll(deserialize(nextRef).getAllRefs());
            } catch (DBAppException e) {
                e.printStackTrace();
                throw new DBAppException("Exception above while getting overflow page");
            }
        }
        return refRes;
    }


    /**
     * Retrieves the last reference from the current overflow page or the next linked page.
     *
     * @return the last Ref object from this or the next overflow page
     * @throws DBAppException if an error occurs during retrieval
     */
    public Ref getLastRef() throws DBAppException {
        if (nextRef != null) {
            OverflowPage nextPage = deserialize(nextRef);
            Ref ref = nextPage.getLastRef();
            nextPage.serialize();
            return ref;
        } else {
            return refs.get(refs.size() - 1);
        }
    }


    /**
     * Retrieves the maximum reference from the current overflow page based on a specified table length.
     *
     * @param tableLength the length of the table used for reference comparison
     *
     * @return the maximum Ref object from this overflow page
     * @throws DBAppException if an error occurs during retrieval
     */
    public Ref getMaxRefPage(int tableLength) throws DBAppException {
        return getMaxRefPage(tableLength, refs.get(0));
    }


    /**
     * Helper method to find the maximum reference in the current overflow page or any linked pages.
     *
     * @param tableLength the length of the table used for reference comparison
     * @param ref the current reference being compared
     *
     * @return the maximum Ref object found
     * @throws DBAppException if an error occurs during retrieval
     */
    private Ref getMaxRefPage(int tableLength, Ref ref) throws DBAppException {
        for (Ref value : refs) {

            if (getIntInRefPage(ref, tableLength) < getIntInRefPage(value, tableLength)) {
                ref = value;
            }
        }
        if (nextRef == null) {
            return ref;
        } else {
            OverflowPage nextPage = deserialize(nextRef);
            Ref ref2 = nextPage.getMaxRefPage(tableLength, ref);
            nextPage.serialize();
            return ref2;
        }
    }


    /**
     * Helper method to extract an integer value from a reference page based on the table length.
     *
     * @param ref the Ref object from which to extract the integer
     * @param tableLength the length of the table used for extraction
     *
     * @return the extracted integer value
     */
    private static int getIntInRefPage(Ref ref, int tableLength) {
        return Integer.parseInt(ref.getPage().substring(tableLength));
    }



    /**
     * Adds a record reference to the overflow page.
     * If the page is full, a new OverflowPage is created or the next page is used for insertion.
     *
     * @param recordRef the Ref object representing the record to be added
     * @throws DBAppException if an error occurs during the addition process
     */
    public void addRecord(Ref recordRef) throws DBAppException {
        if (refs.size() < maxNodeSize) {
            refs.add(recordRef);
        } else {
            OverflowPage nextPage;
            if (nextRef == null) {
                nextPage = new OverflowPage(maxNodeSize);
                nextRef = nextPage.getPageName();
            } else {
                nextPage = deserialize(nextRef);
            }

            nextPage.addRecord(recordRef);
            nextPage.serialize();
        }
    }


    /**
     * Deletes a record reference from the overflow page based on the specified page name.
     * If the record is not found in the current page, it will search in the next overflow page.
     * If the next page becomes empty after deletion, it will be removed.
     *
     * @param page_name the name of the page to be deleted
     * @throws DBAppException if an error occurs during the deletion process
     */
    public void deleteRecord(String page_name) throws DBAppException {
        boolean isDeleted = false;

        for (Ref r : refs) {
            if (r.getPage().equals(page_name)) {
                refs.remove(r);
                isDeleted = true;
                break;
            }
        }

        if (!isDeleted) {
            if (nextRef == null)
                throw new DBAppException("Ref not found");

            OverflowPage overflowPage = deserialize(nextRef);
            overflowPage.deleteRecord(page_name);
            if (overflowPage.refs.isEmpty()) {
                this.nextRef = overflowPage.nextRef;
                File f = new File("data: " + overflowPage.getPageName() + ".class");
                f.delete();
                return;
            }
            overflowPage.serialize();
        }

        this.serialize();
    }


    /**
     * Updates the reference of a specified old page to a new page name.
     * If the old page is not found in the current overflow page, it searches in the next linked page.
     *
     * @param oldPage the old page name to be updated
     * @param newPage the new page name to set
     * @throws DBAppException if an error occurs during the update process
     */
    public void updateRef(String oldPage, String newPage) throws DBAppException {
        int i = 0;
        for (; i < refs.size(); i++) {
            if (oldPage.equals(refs.get(i).getPage())) {
                refs.get(i).setPage(newPage);
                return;
            }
        }
        if (i == refs.size()) {
            OverflowPage nextPage;
            if (newPage != null) {
                (nextPage = deserialize(nextRef)).updateRef(oldPage, newPage);
                nextPage.serialize();
            }
        }
    }


    /**
     * Serializes the current overflow page to a file.
     * The method writes the page object to a file using an ObjectOutputStream.
     *
     * @throws DBAppException if an IO exception occurs during serialization
     */
    public void serialize() throws DBAppException {
        try {
            FileOutputStream fileOut = new FileOutputStream("data: " + this.getPageName() + ".class");
            ObjectOutputStream stream = new ObjectOutputStream(fileOut);
            stream.writeObject(this);
            stream.close();
            fileOut.close();
        } catch (IOException e) {
            throw new DBAppException("IO Exception in " + this.getPageName());
        }
    }


    /**
     * Deserializes an overflow page from a specified file.
     * The method reads the overflow page object from the file using an ObjectInputStream.
     *
     * @param name the name of the file from which to deserialize the overflow page
     * @return the deserialized OverflowPage object
     * @throws DBAppException if an error occurs during deserialization
     */
    public OverflowPage deserialize(String name) throws DBAppException {
        try {
            FileInputStream fileIn = new FileInputStream("data: " + name + ".class");
            ObjectInputStream stream = new ObjectInputStream(fileIn);
            OverflowPage overflowPage = (OverflowPage) stream.readObject();
            stream.close();
            fileIn.close();
            return overflowPage;
        } catch (IOException e) {
            e.printStackTrace();
            throw new DBAppException("IO Exception in " + name);
        } catch (ClassNotFoundException e) {
            e.printStackTrace();
            throw new DBAppException("Class Not Found Exception in " + name + ".class");
        }

    }


    /**
     * Reads a file, splits it by commas, and returns its content as a Vector of String arrays.
     *
     * @param path the path to the file to be read
     * @return a Vector containing the lines of the file as String arrays
     * @throws DBAppException if an error occurs during file reading
     */
    public static Vector readFile(String path) throws DBAppException {
        try {
            String currentLine;
            FileReader fileReader = new FileReader(path);
            BufferedReader bufferedReader = new BufferedReader(fileReader);
            Vector metadata = new Vector();
            while ((currentLine = bufferedReader.readLine()) != null) {
                metadata.add(currentLine.split(","));
            }
            return metadata;
        } catch (IOException e) {
            e.printStackTrace();
            throw new DBAppException("IO Exception above");
        }
    }


    /**
     * Retrieves the last fetched reference from the metadata tree and updates the metadata file.
     * This method increments the last fetched value for future reference tracking.
     *
     * @return the last fetched reference from the metadata
     * @throws DBAppException if an error occurs while accessing the metadata
     */
    protected String getFromMetaDataTree() throws DBAppException {
        try {

            String lastFetched = "";
            Vector meta = readFile("data/metadata.csv");
            int overrideLastFetched = 0;
            for (Object obj : meta) {
                String[] curr = (String[]) obj;
                lastFetched = curr[0];
                overrideLastFetched = Integer.parseInt(curr[0]) + 1;
                curr[0] = overrideLastFetched + "";
                break;
            }
            FileWriter fileWriter = new FileWriter("data/metadata.csv");
            for (Object obj : meta) {
                String[] curr = (String[]) obj;
                fileWriter.append(curr[0]);
                break;
            }
            fileWriter.flush();
            fileWriter.close();
            return lastFetched;
        } catch (IOException e) {
            e.printStackTrace();
            throw new DBAppException("IOException");
        }
    }


    /**
     * Returns a string representation of the overflow page, including its name and all contained references.
     * If there is a next overflow page, its content is also included in the string.
     *
     * @return a string representation of the overflow page
     */
    public String toString() {
        StringBuilder stringBuilder = new StringBuilder();
        stringBuilder.append("The overflow page: ").append(pageName).append("\n");
        for (Ref ref : refs) {
            stringBuilder.append(ref).append(" , ");
        }
        stringBuilder.append("\n");
        if (this.nextRef == null)
            return stringBuilder.toString();
        try {
            stringBuilder.append(deserialize(nextRef).toString());
        } catch (DBAppException e) {
            e.printStackTrace();
            System.out.println("Exception above");
        }
        return stringBuilder.toString();

    }

}