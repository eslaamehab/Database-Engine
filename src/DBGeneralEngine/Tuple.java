package src.DBGeneralEngine;

import java.io.Serializable;
import java.util.Vector;

public class Tuple implements Serializable {


    /**
     * Attributes
     */
    private Vector attributes;


    /**
     * Constructor
     */
    public Tuple(Vector attributes) {
        this.attributes = attributes;
    }


    /**
     * Getters & Setters
     */
    public Vector getAttributes() {
        return attributes;
    }

    public void setAttributes(Vector attributes) {
        this.attributes = attributes;
    }


    /**
     * Rest of the functions
     */
    public boolean equals(Object obj) {
        Tuple x = (Tuple) obj;
        Tuple y = this;
        Object compare1 = x.attributes.get(attributes.size() - 1);
        Object compare2 = y.attributes.get(attributes.size() - 1);

        return compare1.equals(compare2);
    }

    public int hashCode() {
        return (int) attributes.get(attributes.size() - 1);
    }

    public void addAttribute(Object obj) {
        attributes.add(obj);
    }

    public String toString() {
        StringBuilder stringBuilder = new StringBuilder();

        for (int i = 0; i < attributes.size() - 1; i++) {
            Object x = attributes.get(i);
            stringBuilder.append((x != null) ? (x + "\t") : ("null\n"));
        }
        return stringBuilder.toString();
    }

}