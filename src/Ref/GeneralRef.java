package src.Ref;

import src.DBGeneralEngine.DBAppException;

//import java.sql.Ref;
import java.util.ArrayList;

public abstract class GeneralRef {

    public abstract boolean isOverflow();
    public abstract boolean isRecord();
    public abstract void updateRef(String oldPage, String newPage) throws DBAppException;

    public ArrayList<Ref> getAllRef() throws DBAppException
    {
        ArrayList<Ref> results = new ArrayList<Ref>();
        if(this instanceof Ref)
        {
            results.add((Ref)this);
        }
        else
        {
            OverflowRef overflowRef = (OverflowRef) this;
            results.addAll(overflowRef.getAllRef());
        }

        return results;

    }

}
