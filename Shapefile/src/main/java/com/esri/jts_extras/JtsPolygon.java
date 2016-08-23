
package com.esri.jts_extras;

import com.vividsolutions.jts.geom.Polygon;

/**
 */

//a class that holds the geometry of the polygon
public class JtsPolygon{
    
    
    public static Polygon pl;
    
    
    public void set_my_polygon(Polygon pl2){
        
        pl=pl2;
               
    }
    
    public Polygon my_polygon(){
            
        return pl;
        
    }
    
    
}