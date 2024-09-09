package src.DBGeneralEngine;

import java.awt.*;
import java.io.Serializable;


/**
 * CustomPolygon class represents a polygon with additional functionalities.
 * This class implements Comparable to allow sorting based on the polygon's area.
 */
public class CustomPolygon implements Comparable<CustomPolygon>, Serializable {

    /**
     * Attributes
     *
     * polygon -> The underlying Polygon object representing the shape.
     */
    public Polygon polygon;


    /**
     * Constructor
     * Initializes a CustomPolygon instance with the specified Polygon.
     *
     * @param polygon the Polygon to be encapsulated in this CustomPolygon
     */
    public CustomPolygon(Polygon polygon) {
        this.polygon = polygon;
    }


    /**
     * Compares this CustomPolygon with another CustomPolygon based on their areas.
     *
     * @param customPolygon the CustomPolygon to compare against
     *
     * @return a negative integer, zero, or a positive integer.
     * When this polygon is less than, equal to, or greater than the specified polygon
     */
    @Override
    public int compareTo(CustomPolygon customPolygon) {
        Dimension dimension = polygon.getBounds().getSize();
        int polygonArea = dimension.width * dimension.height;

        dimension = customPolygon.polygon.getBounds().getSize();
        int customPolygonArea = dimension.width * dimension.height;

        return polygonArea - customPolygonArea;
    }


    /**
     * Checks if this CustomPolygon is equal to another object.
     * Two CustomPolygons are considered equal if they have the same area.
     *
     * @param obj the object to compare with
     * @return true if the polygons are equal, false otherwise
     */
    @Override
    public boolean equals(Object obj) {

        CustomPolygon customPolygon = (CustomPolygon) obj;
        return this.compareTo(customPolygon) == 0;
    }


    /**
     * Returns a string representation of the CustomPolygon.
     * This includes the points of the polygon and its area.
     *
     * @return a string describing the points and area of the polygon
     */
    @Override
    public String toString() {

        StringBuilder stringBuilder = new StringBuilder("Points: ");
        int[] x = polygon.xpoints;
        int[] y = polygon.ypoints;

        for (int i = 0; i < y.length; i++) {

            stringBuilder.append("(").append(x[i]).append(", ").append(y[i]).append(") ");
        }
        Dimension dimension = polygon.getBounds().getSize();
        int polygonArea = dimension.width * dimension.height;
        stringBuilder.append("Area: ").append(polygonArea);

        return stringBuilder.toString();
    }

}


