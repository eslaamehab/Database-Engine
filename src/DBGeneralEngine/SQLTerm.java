package src.DBGeneralEngine;

import src.APTree.CustomPolygon;

import java.awt.*;

public class SQLTerm {

    private String strTableName;
    private String strColumnName;
    private String strOperator;
    private Object objValue;

    public SQLTerm(String strTableName, String strColumnName, String strOperator, Object objValue) {
        this.strTableName = strTableName;
        this.strColumnName = strColumnName;
        this.strOperator = strOperator;

        if(objValue instanceof Polygon){
            objValue=new CustomPolygon((Polygon)objValue);
        }
        this.objValue = objValue;
    }

    public String getStrTableName() {
        return strTableName;
    }

    public String getStrColumnName() {
        return strColumnName;
    }

    public String getStrOperator() {
        return strOperator;
    }

    public Object getObjValue() {
        return objValue;
    }

    public void setStrTableName(String strTableName) {
        this.strTableName = strTableName;
    }

    public void setStrColumnName(String strColumnName) {
        this.strColumnName = strColumnName;
    }

    public void setStrOperator(String strOperator) {
        this.strOperator = strOperator;
    }

    public void setObjValue(Object objValue) {
        this.objValue = objValue;
    }

    public static void main(String[] args) {

    }
}
