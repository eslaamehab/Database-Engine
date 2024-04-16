package src.APTree;

import java.io.*;
import src.DBGeneralEngine.DBAppException;
import src.Ref.Ref;

import java.util.ArrayList;
import java.util.Vector;

public class OverflowPage implements Serializable {

    /*      Attributes     */
    private String nextRef;
    private Vector<Ref> refs;
    private final int maxNodeSize;
    private String pageName;


    /*      Constructor     */
    public OverflowPage(int maxNodeSize) throws DBAppException {
        this.maxNodeSize=maxNodeSize;
        refs = new Vector<>(maxNodeSize);
        nextRef = null;
        String lastRef = getFromMetaDataTree();
        pageName = "OverflowPage" + lastRef;
    }


    /*      Getters & Setters     */
    public Vector<Ref> getRefs() {
        return refs;
    }
    public void setRefs(Vector<Ref> refs) {
        this.refs = refs;
    }
    public String getPageName() {
        return pageName;
    }
    public void setPageName(String pageName) {
        this.pageName = pageName;
    }
    public String getNext() {
        return nextRef;
    }
    public OverflowPage getNext1() throws DBAppException {
        if(nextRef==null)

            return null;
        return deserialize(nextRef);
    }
    public void setNext(String next) {
        this.nextRef = next;
    }
    public int getTotalSize() throws DBAppException
    {
        if(nextRef == null)
            return refs.size();

        OverflowPage overflowPage = deserialize(nextRef);
        return refs.size() + overflowPage.getTotalSize();
    }
    public ArrayList<Ref> getAllRefs() throws DBAppException {
        ArrayList<Ref> refRes = new ArrayList<>(refs);
        if(nextRef != null)
        {
            try
            {
                refRes.addAll(deserialize(nextRef).getAllRefs());
            }
            catch(DBAppException e)
            {
                e.printStackTrace();
                throw new DBAppException("Exception above while getting overflow page");
            }
        }
        return refRes;
    }
    public Ref getLastRef() throws DBAppException {
        if(nextRef!=null){
            OverflowPage nextPage = deserialize(nextRef);
            Ref ref = nextPage.getLastRef();
            nextPage.serialize();
            return ref;
        }
        else
        {
            return refs.get(refs.size()-1);
        }
    }
    public Ref getMaxRefPage(int tableLength) throws DBAppException {
        return getMaxRefPage(tableLength,refs.get(0));
    }
    private Ref getMaxRefPage(int tableLength, Ref ref) throws DBAppException {
        for (Ref value : refs) {

            if (getIntInRefPage(ref, tableLength) < getIntInRefPage(value, tableLength)) {
                ref = value;
            }
        }
        if(nextRef == null)
        {
            return ref;
        }
        else
        {
            OverflowPage nextPage = deserialize(nextRef);
            Ref ref2 = nextPage.getMaxRefPage(tableLength, ref);
            nextPage.serialize();
            return ref2;
        }
    }
    private static int getIntInRefPage(Ref ref,int tableLength){
        return Integer.parseInt(ref.getPage().substring(tableLength));
    }


    /*      Methods     */
    public void addRecord(Ref recordRef) throws DBAppException{
        if (refs.size()<maxNodeSize)
        {
            refs.add(recordRef);
        }
        else {
            OverflowPage nextPage;
            if (nextRef==null)
            {
                nextPage = new OverflowPage(maxNodeSize);
                nextRef = nextPage.getPageName();
            }
            else
            {
                nextPage = deserialize(nextRef);
            }

            nextPage.addRecord(recordRef);
            nextPage.serialize();
        }
    }

    public void deleteRecord(String page_name) throws DBAppException {
        boolean isDeleted = false;

        for(Ref r: refs)
        {
            if(r.getPage().equals(page_name))
            {
                refs.remove(r);
                isDeleted = true;
                break;
            }
        }

        if(!isDeleted)
        {
            if(nextRef == null)
                throw new DBAppException("Ref not found");

            OverflowPage overflowPage = deserialize(nextRef);
            overflowPage.deleteRecord(page_name);
            if(overflowPage.refs.isEmpty())
            {
                this.nextRef = overflowPage.nextRef;
                File f = new File("data: " + overflowPage.getPageName()+".class");
                f.delete();
                return;
            }
            overflowPage.serialize();
        }

        this.serialize();
    }

    public void updateRef(String oldPage, String newPage) throws DBAppException  {
        int i =0;
        for(; i<refs.size(); i++){
            if(oldPage.equals(refs.get(i).getPage())){
                refs.get(i).setPage(newPage);
                return;
            }
        }
        if(i==refs.size()){
            OverflowPage nextPage;
            if (newPage!=null ) {
                (nextPage=deserialize(nextRef)).updateRef(oldPage, newPage);
                nextPage.serialize();
            }
        }
    }

    public void serialize() throws DBAppException{
        try {
            FileOutputStream fileOut = new FileOutputStream("data: "+ this.getPageName() + ".class");
            ObjectOutputStream stream = new ObjectOutputStream(fileOut);
            stream.writeObject(this);
            stream.close();
            fileOut.close();
        }
        catch(IOException e) {
            throw new DBAppException("IO Exception in "+ this.getPageName());
        }
    }

    public OverflowPage deserialize(String name) throws DBAppException{
        try {
            FileInputStream fileIn = new FileInputStream("data: "+ name + ".class");
            ObjectInputStream stream = new ObjectInputStream(fileIn);
            OverflowPage overflowPage = (OverflowPage) stream.readObject();
            stream.close();
            fileIn.close();
            return overflowPage;
        }
        catch(IOException e) {
            e.printStackTrace();
            throw new DBAppException("IO Exception in "+ name);
        }
        catch(ClassNotFoundException e) {
            e.printStackTrace();
            throw new DBAppException("Class Not Found Exception in " + name + ".class");
        }

    }

    public static Vector readFile(String path) throws DBAppException {
        try {
            String currentLine;
//            modify below path
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
            throw new DBAppException("IO Exception");
        }
    }

    protected String getFromMetaDataTree() throws DBAppException
    {
        try {

            String lastFetched = "";
            Vector meta = readFile("data/metadata.csv");
            int overrideLastFetched = 0;
            for (Object obj : meta) {
                String[] curr = (String[]) obj;
                lastFetched = curr[0];
                overrideLastFetched = Integer.parseInt(curr[0])+1;
                curr[0] = overrideLastFetched + "";
                break;
            }
            FileWriter fileWriter = new FileWriter("data/metadata.csv");
            for (Object obj : meta)
            {
                String[] curr = (String[]) obj;
                fileWriter.append(curr[0]);
                break;
            }
            fileWriter.flush();
            fileWriter.close();
            return lastFetched;
        }
        catch (IOException e) {
            e.printStackTrace();
            throw new DBAppException("IOException");
        }
    }

    public String toString()
    {
        StringBuilder stringBuilder = new StringBuilder();
        stringBuilder.append("The overflow page: ").append(pageName).append("\n");
        for(Ref ref : refs)
        {
            stringBuilder.append(ref).append(" , ");
        }
        stringBuilder.append("\n");
        if(this.nextRef == null)
            return stringBuilder.toString();
        try
        {
            stringBuilder.append(deserialize(nextRef).toString());
        }
        catch(DBAppException e)
        {
            e.printStackTrace();
            System.out.println("Exception above");
        }
        return stringBuilder.toString();

    }

}