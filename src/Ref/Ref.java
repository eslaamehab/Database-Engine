package src.Ref;

import java.io.Serializable;



/**
 * This class represents a reference object that contains a page number.
 * It inherits the basic functionality of a general reference object, such as the ability to store and retrieve a value.
 * It implements 'Serializable' interface to allow instances of `OverflowRef` to be easily serialized and deserialized
 * In order to enable persistence and transfer of the object's state.
 *
 * Additional notes:-
 * This class overrides the `equals` and `hashCode` methods to provide custom implementations for comparing and hashing `Ref` objects based on their page number.
 * This class also overrides the `isOverflow` method from the `GeneralRef` class, indicating that this type of reference does not have any overflow conditions.
 */
public class Ref extends GeneralRef implements Serializable
{

    /**
     * Attributes
     * <p>
     * pageNo   -> A String representing the page number associated with this reference
     */
    private String pageNo;


    /**
     * Constructor
     * Creates a reference object by initializing the `pageNo` attribute with the provided page number.
     *
     * @param pageNo the page number to be associated with this reference
     */
    public Ref(String pageNo)
    {
        this.pageNo = pageNo;
    }


    /**
     * Getters & Setters
     */
    public String getPageNo() {
        return pageNo;
    }

    public void setPageNo(String pageNo) {
        this.pageNo = pageNo;
    }

    public String getPage()
    {
        return pageNo;
    }

    public void setPage(String pageNo) {
        this.pageNo=pageNo;
    }


    /**
     * Override of the `equals` method
     * Compares the page numbers of two `Ref` objects for equality
     *
     * @param obj the object to compare with
     * @return `true` if the page numbers are equal or `false` otherwise
     */
    public boolean equals(Object obj) {
        Ref x= (Ref) obj;
        String firstNo = x.pageNo;
        Ref y= this;
        String secondNo = y.pageNo;
        return firstNo.equals(secondNo);
    }

    /**
     * Override of the `hashCode` method
     * Generates a hash code for a `Ref` object based on its page number
     *
     * @return the hash code for this `Ref` object
     */
    public int hashCode() {
        char[] charArray = pageNo.toCharArray();
        StringBuilder stringBuilder = new StringBuilder();
        for (int i=charArray.length-1; i>=0; i--) {
            char c = charArray[i];
            int cInt = c-'0';
            if ( cInt<0 || cInt>9 ) break;
            stringBuilder.insert(0, cInt);
        }
        return Integer.parseInt(stringBuilder.toString());
    }

    /**
     * Updates the page number associated with this reference
     *
     * @param oldPage the old page number
     * @param newPage the new page number
     */
    public void updateRef(String oldPage, String newPage) {
        pageNo = newPage;
    }

    /**
     * Override of the `isOverflow` method from the `GeneralRef` class
     * Indicates that this type of reference does not have any overflow conditions
     *
     * @return `false` to indicate no overflow condition
     */
    @Override
    public boolean isOverflow() {
        return false;
    }

}

