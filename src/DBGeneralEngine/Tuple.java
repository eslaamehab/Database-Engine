package src.DBGeneralEngine;

import java.io.Serializable;
import java.util.Vector;


/**
 * The Tuple class represents a data record in a database table.
 * A tuple is a collection of attributes or field values that describe a single row in the table.
 * The Tuple class is serializable, allowing it to be stored and retrieved from disk.
 */
public class Tuple implements Serializable {


    /**
     *
     * attributes   -> Vector that stores the individual attribute values of the tuple.
     */
    private Vector attributes;


    /**
     * Constructor
     * Takes a Vector of attribute values and initializes the attributes field.
     *
     * @param attributes a Vector containing the attribute values for the tuple
     */
    public Tuple(Vector attributes) {
        this.attributes = attributes;
    }


    /**
     * Getters & Setters
     * <p>
     * The getAttributes() and setAttributes() methods allow both access and modification of the attributes field respectively.
     *
     *
     * @return the Vector of attribute values for the tuple
     */
    public Vector getAttributes() {
        return attributes;
    }

    /**
     *
     @param attributes the new Vector of attribute values to set for the tuple
     */
    public void setAttributes(Vector attributes) {
        this.attributes = attributes;
    }


    /**
     * Compares two Tuple objects based on the value of the last attribute in the attributes Vector.
     * Two Tuple objects are considered equal if the values of their last attributes are equal.
     *
     * @param obj the Object to compare with the current Tuple object
     * @return true if the two Tuple objects have equal last attribute values, false otherwise
     */
    public boolean equals(Object obj) {
        Tuple x = (Tuple) obj;
        Tuple y = this;
        Object compare1 = x.attributes.get(attributes.size() - 1);
        Object compare2 = y.attributes.get(attributes.size() - 1);

        return compare1.equals(compare2);
    }


    /**
     * Returns a hash code value for the Tuple object based on the value of the last attribute in the attributes Vector.
     *
     * @return the hash code value for the Tuple object
     */
    public int hashCode() {
        return (int) attributes.get(attributes.size() - 1);
    }


    /**
     * Adds a new attribute value to the end of the attributes Vector.
     *
     * @param obj   the new attribute value to add to the tuple.
     */
    public void addAttribute(Object obj) {
        attributes.add(obj);
    }


    /**
     * Returns a string representation of the Tuple object, with each attribute value separated by a tab character.
     * If an attribute value is null, it is represented as "null".
     *
     * @return a string representation of the Tuple object
     */
    public String toString() {
        StringBuilder stringBuilder = new StringBuilder();

        for (int i = 0; i < attributes.size() - 1; i++) {
            Object x = attributes.get(i);
            stringBuilder.append((x != null) ? (x + "\t") : ("null\n"));
        }
        return stringBuilder.toString();
    }

}