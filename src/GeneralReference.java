package src;

import java.sql.Ref;
import java.util.ArrayList;

public abstract class GeneralReference {

    public abstract boolean isOverflow();
    public abstract boolean isRecord();
    public abstract void updateRef(String oldPage, String newPage) throws DBAppException;

    public ArrayList<Ref> getALLRef() throws DBAppException
    {
        ArrayList<Ref> results = new ArrayList<Ref>();
        if(this instanceof Ref)
        {
            results.add((Ref)this);
        }
        else
        {
            OverflowReference or = (OverflowReference) this;
            results.addAll(or.ALLgetRef());
        }

        return results;

    }

}
