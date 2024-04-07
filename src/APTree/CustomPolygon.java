package src.APTree;

import java.awt.*;
import java.io.Serializable;

public class CustomPolygon implements Comparable<CustomPolygon>, Serializable {


public Polygon polygon;

public CustomPolygon(Polygon polygon){
        this.polygon=polygon;
        }

@Override
public int compareTo(CustomPolygon customPolygon){
        Dimension dimension = polygon.getBounds().getSize();
        int polygonArea = dimension.width * dimension.height;

        dimension = customPolygon.polygon.getBounds().getSize();
        int customPolygonArea = dimension.width * dimension.height;

        int AreaDifference = polygonArea - customPolygonArea;

        return AreaDifference;
        }

@Override
public boolean equals(Object obj){

        CustomPolygon customPolygon = (CustomPolygon) obj;
        return this.compareTo(customPolygon) == 0;
        }

@Override
public String toString(){

        StringBuilder stringBuilder = new StringBuilder("Points: ");
        int[] x = polygon.xpoints;
        int[] y = polygon.ypoints;

        for(int i=0; i<y.length; i++){

        stringBuilder.append("("+x[i]+", "+y[i]+") ");
        }
        Dimension dimension = polygon.getBounds().getSize();
        int polygonArea = dimension.width * dimension.height;
        stringBuilder.append("Area: "+polygonArea);

        return stringBuilder.toString();
        }

}
