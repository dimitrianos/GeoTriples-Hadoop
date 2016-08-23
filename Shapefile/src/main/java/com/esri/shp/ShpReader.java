
package com.esri.shp;


import com.esri.jts_extras.JtsPolygon;
import com.esri.jts_extras.PointorPolygon;

import com.vividsolutions.jts.geom.Point;
import com.vividsolutions.jts.geom.Polygon;

import com.esri.io.PolylineMWritable;

import com.vividsolutions.jts.geom.Coordinate;
import com.vividsolutions.jts.geom.CoordinateFilter;
import com.vividsolutions.jts.geom.CoordinateSequence;
import com.vividsolutions.jts.geom.GeometryFactory;
import com.vividsolutions.jts.geom.LineString;
import com.vividsolutions.jts.geom.LinearRing;


import org.apache.commons.io.EndianUtils;

import java.io.DataInputStream;
import java.io.IOException;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * http://www.esri.com/library/whitepapers/pdfs/shapefile.pdf
 */
public class ShpReader implements Serializable
{
    private transient DataInputStream m_dataInputStream;
    private transient ShpHeader m_shpHeader;

    private transient int m_parts[] = new int[4];

    public transient int recordNumber;
    public transient int contentLength;
    public transient int contentLengthInBytes;
    public transient int shapeType;
    public transient double xmin;
    public transient double ymin;
    public transient double xmax;
    public transient double ymax;
    public transient double mmin;
    public transient double mmax;    
    public transient int numParts;
    public transient int numPoints;

    public ShpReader(final DataInputStream dataInputStream) throws IOException
    {
        m_dataInputStream = dataInputStream;
        m_shpHeader = new ShpHeader(dataInputStream);
    }

    public ShpHeader getHeader()
    {
        return m_shpHeader;
    }

    public boolean hasMore() throws IOException
    {
        return m_dataInputStream.available() > 0;
    }

    private void readRecordHeader() throws IOException
    {
        recordNumber = m_dataInputStream.readInt();
        contentLength = m_dataInputStream.readInt();
        contentLengthInBytes = contentLength + contentLength - 4;

        shapeType = EndianUtils.readSwappedInteger(m_dataInputStream);
        
    }

    //a fuction that decides the geometry type of the record and sets the right values
    public void PointorPolygon_Selector(final Point point,final Polygon polygon) throws IOException
    {
        
        readRecordHeader();             
             
        //shapeType==1 => Point
        if(shapeType==1)
        {
            
            point.apply(new CoordinateFilter() {
        
                @Override
                public void filter(Coordinate coord) {

                    try {

                        coord.x = EndianUtils.readSwappedDouble(m_dataInputStream);

                    } catch (IOException ex) {

                        Logger.getLogger(ShpReader.class.getName()).log(Level.SEVERE, null, ex);

                    }

                    try {

                        coord.y =EndianUtils.readSwappedDouble(m_dataInputStream);

                    } catch (IOException ex) {

                        Logger.getLogger(ShpReader.class.getName()).log(Level.SEVERE, null, ex);

                    }


                }

            });
            
            PointorPolygon.setPointPnorPl(point);
            PointorPolygon.setPolygonPnorPl(null);
            PointorPolygon.setPolygonorPoint_Shapetype(shapeType);
            
            
        }
        //shapeType==5 => Polygon
        else if(shapeType==5)            
        {
        
            readShapeHeader();

            List<coordslist> cl_list = new ArrayList<coordslist>();
            List<list_of_coordslist> list_of_cl = new ArrayList<list_of_coordslist>();

            for (int i = 0, j = 1; i < numParts; )
            {
                final int count = m_parts[j++] - m_parts[i++];
                for (int c = 0; c < count; c++)
                {
                    final double x = EndianUtils.readSwappedDouble(m_dataInputStream);
                    final double y = EndianUtils.readSwappedDouble(m_dataInputStream);

                    if (c > 0)
                    {

                        coordslist cl=new coordslist();
                        cl.coord_x=x;
                        cl.coord_y=y;
                        cl_list.add(cl);


                    }
                    else
                    {
                        //list with coordinates of all polygons
                        list_of_coordslist locl = new list_of_coordslist();
                        locl.setCl(cl_list);
                        list_of_cl.add(locl);


                        cl_list.removeAll(cl_list);
                        coordslist cl=new coordslist();
                        cl.coord_x=x;
                        cl.coord_y=y;
                        cl_list.add(cl);

                    }



                }
            }

            //Get the outer polygon

            List<polygons_list> pll_list = new ArrayList<polygons_list>();

            for(list_of_coordslist i : list_of_cl){

                int c = i.cl.size();

                Coordinate[] coords  = new Coordinate[c];

                int counter=0;

                for(coordslist j : i.cl ){

                    coords[counter] =  new Coordinate(j.coord_x,j.coord_y);

                    counter++;

                }

                GeometryFactory gf=new GeometryFactory();

                LineString lineString = gf.createLineString(coords);

                LinearRing ring = null;

                if( lineString.isClosed() )
                ring = gf.createLinearRing( coords );
                else {
                CoordinateSequence sequence = lineString.getCoordinateSequence();
                Coordinate array[] = new Coordinate[ sequence.size() + 1 ];

                for( int n=0; n<sequence.size();n++){

                    array[n] = sequence.getCoordinate(n);
                    array[array.length-1] = sequence.getCoordinate(0);
                    ring = gf.createLinearRing( array );

                    }
                }


                 LinearRing holes[] = null;

                 Polygon polygon_1 = gf.createPolygon(ring, holes);

                 polygons_list pol_l = new polygons_list();
                 pol_l.pll = polygon_1;
                 pll_list.add(pol_l);

            }

            double max_polygon=0;

            Polygon polygon_last;

            GeometryFactory gf=new GeometryFactory();

            Coordinate[] coords  =
              new Coordinate[] {new Coordinate(1, 0), new Coordinate(10, 0),
                                new Coordinate(10, 10),  new Coordinate(1,0)};
              
            LinearRing ring = gf.createLinearRing( coords );
            
            LinearRing holes[] = null;


            polygon_last = gf.createPolygon(ring, holes);

            for(polygons_list i : pll_list){

                if(i.pll.getArea() > max_polygon){

                    max_polygon = i.pll.getArea();
                    polygon_last = i.pll;

                }


            }

            JtsPolygon jtspl = new JtsPolygon();

            jtspl.set_my_polygon(polygon_last);
            
            PointorPolygon.setPointPnorPl(null);
            PointorPolygon.setPolygonPnorPl(polygon_last);
            PointorPolygon.setPolygonorPoint_Shapetype(shapeType);
                        
        }
        else
        {
            
            PointorPolygon.setPointPnorPl(null);
            PointorPolygon.setPolygonPnorPl(null);
            PointorPolygon.setPolygonorPoint_Shapetype(0);
        
        }
            
    }
    

    public Point readPoint() throws IOException
    {
        //create a point
        GeometryFactory gf = new GeometryFactory();

        Coordinate coord = new Coordinate(-99.79634094297234,39.486310278100405);
             
        Point point = gf.createPoint(coord);

        return queryPoint(point);
        
    }
    
    
    
    

    public Polygon readPolygon() throws IOException
    {
        
        //create a polygon
        GeometryFactory gf=new GeometryFactory();
        
        Coordinate[] coords  =
            new Coordinate[] {new Coordinate(0, 0), new Coordinate(10, 0),
                          new Coordinate(10, 10), new Coordinate(0,0) };

        LinearRing ring = gf.createLinearRing(coords);
        LinearRing holes[] = null;
        Polygon polygon = gf.createPolygon(ring, holes);

        
        return queryPolygon(polygon);
        
    }
     
        
        
   

    public Point queryPoint(final Point point) throws IOException
    {
        
        readRecordHeader();
        
        
        point.apply(new CoordinateFilter() {
        
            @Override
            public void filter(Coordinate coord) {

                try {

                    coord.x = EndianUtils.readSwappedDouble(m_dataInputStream);

                } catch (IOException ex) {

                    Logger.getLogger(ShpReader.class.getName()).log(Level.SEVERE, null, ex);

                }

                try {

                    coord.y =EndianUtils.readSwappedDouble(m_dataInputStream);

                } catch (IOException ex) {

                    Logger.getLogger(ShpReader.class.getName()).log(Level.SEVERE, null, ex);

                }


            }

        });
        
        return point;
        
    }

    
    
    public Polygon queryPolygon(final Polygon polygon) throws IOException
    {
     
        readRecordHeader();

        readShapeHeader();
        
        List<coordslist> cl_list = new ArrayList<coordslist>();
        List<list_of_coordslist> list_of_cl = new ArrayList<list_of_coordslist>();
        
        for (int i = 0, j = 1; i < numParts; )
        {
            final int count = m_parts[j++] - m_parts[i++];
            for (int c = 0; c < count; c++)
            {
                final double x = EndianUtils.readSwappedDouble(m_dataInputStream);
                final double y = EndianUtils.readSwappedDouble(m_dataInputStream);
                
                if (c > 0)
                {
                    
                    coordslist cl=new coordslist();
                    cl.coord_x=x;
                    cl.coord_y=y;
                    cl_list.add(cl);
                    
                
                }
                else
                {
                    //list with coordinates of all polygons
                    list_of_coordslist locl = new list_of_coordslist();
                    locl.setCl(cl_list);
                    list_of_cl.add(locl);
                    
                    
                    cl_list.removeAll(cl_list);
                    coordslist cl=new coordslist();
                    cl.coord_x=x;
                    cl.coord_y=y;
                    cl_list.add(cl);
                
                }
                
                
             
            }
        }
                
        
        //Get the outer polygon        
        List<polygons_list> pll_list = new ArrayList<polygons_list>();
                                 
        for(list_of_coordslist i : list_of_cl){
        
            int c = i.cl.size();
            
            Coordinate[] coords  = new Coordinate[c];
            
            int counter=0;
            
            for(coordslist j : i.cl ){
            
                coords[counter] =  new Coordinate(j.coord_x,j.coord_y);
                     
                counter++;
            
            }
            
            GeometryFactory gf=new GeometryFactory();
        
            LineString lineString = gf.createLineString(coords);
            
            LinearRing ring = null;

            if( lineString.isClosed() )
            ring = gf.createLinearRing( coords );
            else {
            CoordinateSequence sequence = lineString.getCoordinateSequence();
            Coordinate array[] = new Coordinate[ sequence.size() + 1 ];
             
            for( int n=0; n<sequence.size();n++){
                
                array[n] = sequence.getCoordinate(n);
                array[array.length-1] = sequence.getCoordinate(0);
                ring = gf.createLinearRing( array );
                
                }
            }
            
            
            LinearRing holes[] = null;
        
            Polygon polygon_1 = gf.createPolygon(ring, holes);
             
            polygons_list pol_l = new polygons_list();
            pol_l.pll = polygon_1;
            pll_list.add(pol_l);
            
        }
        
        double max_polygon=0;
        
        Polygon polygon_last;
        
        GeometryFactory gf=new GeometryFactory();
        
        Coordinate[] coords  =
            new Coordinate[] {new Coordinate(1, 0), new Coordinate(10, 0),
                            new Coordinate(10, 10),  new Coordinate(1,0)};
            
        LinearRing ring = gf.createLinearRing( coords );
        LinearRing holes[] = null;
        
    
        polygon_last = gf.createPolygon(ring, holes);
        
        
        for(polygons_list i : pll_list){
        
            if(i.pll.getArea() > max_polygon){
            
                max_polygon = i.pll.getArea();
                polygon_last = i.pll;
                
            }         
                        
        }
        
        JtsPolygon jstpl = new JtsPolygon();
        
        jstpl.set_my_polygon(polygon_last);
        
        
         
        return polygon_last;
        
        //Get the outer polygon end

    }

    public PolylineMWritable readPolylineMWritable() throws IOException
    {
        final PolylineMWritable polylineMWritable = new PolylineMWritable();

        readRecordHeader();
        readShapeHeader();

        polylineMWritable.lens = new int[numParts];
        polylineMWritable.x = new double[numPoints];
        polylineMWritable.y = new double[numPoints];
        polylineMWritable.m = new double[numPoints];

        int p = 0;
        for (int i = 0, j = 1; i < numParts; i++, j++)
        {
            final int count = m_parts[j] - m_parts[i];
            polylineMWritable.lens[i] = count;
            for (int c = 0; c < count; c++, p++)
            {
                polylineMWritable.x[p] = EndianUtils.readSwappedDouble(m_dataInputStream);
                polylineMWritable.y[p] = EndianUtils.readSwappedDouble(m_dataInputStream);
            }
        }

        mmin = EndianUtils.readSwappedDouble(m_dataInputStream);
        mmax = EndianUtils.readSwappedDouble(m_dataInputStream);

        for (p = 0; p < numPoints; p++)
        {
            polylineMWritable.m[p] = EndianUtils.readSwappedDouble(m_dataInputStream);
        }

        return polylineMWritable;
    }

    private void readShapeHeader() throws IOException
    {
        xmin = EndianUtils.readSwappedDouble(m_dataInputStream);
        ymin = EndianUtils.readSwappedDouble(m_dataInputStream);
        xmax = EndianUtils.readSwappedDouble(m_dataInputStream);
        ymax = EndianUtils.readSwappedDouble(m_dataInputStream);

        numParts = EndianUtils.readSwappedInteger(m_dataInputStream);
        numPoints = EndianUtils.readSwappedInteger(m_dataInputStream);

        if ((numParts + 1) > m_parts.length)
        {
            m_parts = new int[numParts + 1];
        }
        for (int p = 0; p < numParts; p++)
        {
            m_parts[p] = EndianUtils.readSwappedInteger(m_dataInputStream);
        }
        m_parts[numParts] = numPoints;
    }
    
    
    public class coordslist{
    
        private double coord_x;
        
        private double coord_y;

        public double getCoord_x() {
            return coord_x;
        }

        public void setCoord_x(double coord_x) {
            this.coord_x = coord_x;
        }

        public double getCoord_y() {
            return coord_y;
        }

        public void setCoord_y(double coord_y) {
            this.coord_y = coord_y;
        } 
        
    }
    
    
    public class list_of_coordslist{
    
        private List<coordslist> cl;

        public List<coordslist> getCl() {
            return cl;
        }

        public void setCl(List<coordslist> cl) {
            this.cl = cl;
        }  
               
    }
    
    
    
    public class polygons_list{
    
        private Polygon pll;

        public Polygon getPll() {
            return pll;
        }

        public void setPll(Polygon pll) {
            this.pll = pll;
        }        
               
    }

}
