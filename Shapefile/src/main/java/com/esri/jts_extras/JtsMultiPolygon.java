package com.esri.jts_extras;

import com.vividsolutions.jts.geom.MultiPolygon;

/**
 * Created by bard on 9/3/16.
 */
public class JtsMultiPolygon {

    public static MultiPolygon mlpl;


    public void set_my_multi_polygon(MultiPolygon mlpl2){

        mlpl=mlpl2;

    }

    public MultiPolygon my_multi_polygon(){

        return mlpl;

    }


}
