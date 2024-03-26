package src.APTree;

import java.io.Serializable;
import java.util.Vector;

public class Tuple implements Serializable {

//    private static final long serialVersionUID = -1512597374643196276L;
//    to be removed
    private Vector attributes = new Vector();
    private Object compare1, compare2;

    // compare was previously equals
    public boolean equals(Object obj) {
        Tuple x=(Tuple)obj;
        Tuple y= this;
        compare1 = x.attributes.get(attributes.size()-1) ;
        compare2 = y.attributes.get(attributes.size()-1) ;

        return compare1.equals(compare2);
    }

    public int hashCode() {
        return (int)attributes.get(attributes.size()-1);
    }

    public void addAttribute(Object obj) {
        attributes.add(obj);
    }

    public String toString() {
        String str = "";

        for (int i=0;i<attributes.size()-1;i++) {
            Object x = attributes.get(i);
            str += (x != null) ? (x + "\t") : ("null\n");
        }
        return str;
    }

    public Vector getAttributes() {
        return attributes;
    }

    public void setAttributes(Vector attributes) {
        this.attributes = attributes;
    }


}