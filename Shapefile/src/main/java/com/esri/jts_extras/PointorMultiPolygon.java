package com.esri.jts_extras;

import com.vividsolutions.jts.geom.MultiPolygon;
import com.vividsolutions.jts.geom.Point;

/**
 * Created by bard on 9/3/16.
 */
public class PointorMultiPolygon {

    private static MultiPolygon MultiPolygonPnorMlPl = null;
    private static Point PointPnorMlPl = null;
    private static int MultiPolygonorPoint_Shapetype;

    public static MultiPolygon getMultiPolygonPnorMlPl() {
        return MultiPolygonPnorMlPl;
    }

    public static void setMultiPolygonPnorMlPl(MultiPolygon multiPolygonPnorMlPl) {
        MultiPolygonPnorMlPl = multiPolygonPnorMlPl;
    }

    public static Point getPointPnorMlPl() {
        return PointPnorMlPl;
    }

    public static void setPointPnorMlPl(Point pointPnorMlPl) {
        PointPnorMlPl = pointPnorMlPl;
    }

    public static int getMultiPolygonorPoint_Shapetype() {
        return MultiPolygonorPoint_Shapetype;
    }

    public static void setMultiPolygonorPoint_Shapetype(int multiPolygonorPoint_Shapetype) {
        MultiPolygonorPoint_Shapetype = multiPolygonorPoint_Shapetype;
    }

}



