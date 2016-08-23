
package com.esri.jts_extras;

import com.vividsolutions.jts.geom.Point;
import com.vividsolutions.jts.geom.Polygon;

/**
 */

//a class that helps us to read point and polygons from the same shape file
public class PointorPolygon {
    
    private static Polygon PolygonPnorPl = null;
    private static Point PointPnorPl = null;
    private static int PolygonorPoint_Shapetype;
    

    public static Polygon getPolygonPnorPl() {
        return PolygonPnorPl;
    }

    public static void setPolygonPnorPl(Polygon PolygonPnorPl) {
        PointorPolygon.PolygonPnorPl = PolygonPnorPl;
    }

    public static Point getPointPnorPl() {
        return PointPnorPl;
    }

    public static void setPointPnorPl(Point PointPnorPl) {
        PointorPolygon.PointPnorPl = PointPnorPl;
    }

    public static int getPolygonorPoint_Shapetype() {
        return PolygonorPoint_Shapetype;
    }

    public static void setPolygonorPoint_Shapetype(int PolygonorPoint_Shapetype) {
        PointorPolygon.PolygonorPoint_Shapetype = PolygonorPoint_Shapetype;
    }
        
    
    
}
