
package com.esri.io;

import com.esri.jts_extras.JtsMultiPolygon;
import com.esri.jts_extras.JtsPolygon;
import com.esri.jts_extras.PointorMultiPolygon;

import com.vividsolutions.jts.geom.*;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.logging.Level;
import java.util.logging.Logger;
import org.apache.hadoop.io.Writable;

/**
 */

public class PointPolygonWritable implements Writable{
    
    public Polygon polygon;

    public MultiPolygon multiPolygon;
    
    public Point point;
    
    public int ShapeType_for_Hadoop;
    
    
    public PointPolygonWritable()
    {  
        
        //Create a point
        GeometryFactory gf_point = new GeometryFactory();

        Coordinate coord = new Coordinate(0,0);
        point= gf_point.createPoint(coord);
        
        
        //Create a polygon
        JtsPolygon jtspl = new JtsPolygon();
        
        if(jtspl.my_polygon()!=null){
              
            polygon = jtspl.my_polygon();
         
        }
        else
        {

            GeometryFactory gf_polygon = new GeometryFactory();

            Coordinate[] coords  =
                new Coordinate[] {new Coordinate(1, 0), new Coordinate(10, 0),
                                  new Coordinate(10, 10),  new Coordinate(1,0)};
            
            LinearRing ring = gf_polygon.createLinearRing( coords );
            LinearRing holes[] = null;


            polygon = gf_polygon.createPolygon(ring, holes);
            jtspl.set_my_polygon(polygon);

        }

        //create a multipolygon
        JtsMultiPolygon jtsmlpl = new JtsMultiPolygon();
        if(jtsmlpl.my_multi_polygon()!=null) {

            multiPolygon = jtsmlpl.my_multi_polygon();

        }
        else
        {

            GeometryFactory gf_multipolygon = new GeometryFactory();

            GeometryFactory gf_polygon_1 = new GeometryFactory();

            Coordinate[] coords_1  =
                    new Coordinate[] {new Coordinate(0, 0), new Coordinate(10, 0),
                            new Coordinate(10, 10),  new Coordinate(0,0)};

            LinearRing ring_1 = gf_polygon_1.createLinearRing( coords_1 );
            LinearRing holes_1[] = null;


            Polygon polygon_1 = gf_polygon_1.createPolygon(ring_1, holes_1);



            GeometryFactory gf_polygon_2 = new GeometryFactory();

            Coordinate[] coords_2  =
                    new Coordinate[] {new Coordinate(20, 0), new Coordinate(30, 20),
                            new Coordinate(30, 30),  new Coordinate(20,0)};

            LinearRing ring_2 = gf_polygon_2.createLinearRing( coords_2 );
            LinearRing holes_2[] = null;


            Polygon polygon_2 = gf_polygon_2.createPolygon(ring_2, holes_2);


            Polygon[] polygonSet = new Polygon[2];

            polygonSet[0] = polygon_1;
            polygonSet[1] = polygon_2;

            GeometryFactory multi_factory = new GeometryFactory();

            multiPolygon = multi_factory.createMultiPolygon(polygonSet);


        }
                  
    
    }
    
    
    
    @Override
    public void write(final DataOutput dataOutput) throws IOException
    {            

        //point case
        dataOutput.writeDouble(point.getX());
        dataOutput.writeDouble(point.getY());
        
        
        //polygon case


//        LineString ls = polygon.getExteriorRing();
//
//        int ls_points = ls.getNumPoints();
//
//        dataOutput.writeInt(ls_points);
//
//        Point point_for_polygon_write;
//
//        for(int i = 0; i<ls_points; i++)
//        {
//
//            point_for_polygon_write = ls.getPointN(i);
//
//            dataOutput.writeDouble(point_for_polygon_write.getX());
//            dataOutput.writeDouble(point_for_polygon_write.getY());
//
//        }



        
    }
    
    
    @Override
    public void readFields(final DataInput dataInput) throws IOException
    {
        
        //point case
        point.apply(new CoordinateFilter() {
        
            @Override
            public void filter(Coordinate coord) {

                try {

                    coord.x = dataInput.readDouble();

                } catch (IOException ex) {

                    Logger.getLogger(PointWritable.class.getName()).log(Level.SEVERE, null, ex);

                }

                try {

                    coord.y = dataInput.readDouble();

                } catch (IOException ex) {

                    Logger.getLogger(PointWritable.class.getName()).log(Level.SEVERE, null, ex);

                }


            }

        });
        
        
//
//
//        //polygon case
//        int num_points = dataInput.readInt();
//
//        Coordinate[] coords  = new Coordinate[num_points];
//
//        for(int i=0; i < num_points; i++){
//
//            coords[i] =  new Coordinate(dataInput.readDouble(),dataInput.readDouble());
//
//        }
//
//        GeometryFactory gf=new GeometryFactory();
//
//        LinearRing ring = gf.createLinearRing( coords );
//
//        LinearRing holes[] = null;
//
//        polygon = gf.createPolygon(ring, holes);
//
//
//        JtsPolygon jtspl = new JtsPolygon();
//
//        jtspl.set_my_polygon(polygon);
                
        
    }
    
    
}
