
package com.esri.shp;


import com.esri.jts_extras.JtsMultiPolygon;
import com.esri.jts_extras.PointorMultiPolygon;

import com.vividsolutions.jts.geom.Point;
import com.vividsolutions.jts.geom.Polygon;
import com.vividsolutions.jts.geom.Coordinate;
import com.vividsolutions.jts.geom.CoordinateFilter;
import com.vividsolutions.jts.geom.CoordinateSequence;
import com.vividsolutions.jts.geom.GeometryFactory;
import com.vividsolutions.jts.geom.LineString;
import com.vividsolutions.jts.geom.LinearRing;
import com.vividsolutions.jts.geom.MultiPolygon;

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
    public void PointorMultiPolygon_Selector(final Point point) throws IOException
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

            PointorMultiPolygon.setPointPnorMlPl(point);
            PointorMultiPolygon.setMultiPolygonPnorMlPl(null);
            PointorMultiPolygon.setMultiPolygonorPoint_Shapetype(shapeType);
            
            
        }
        //shapeType==5 => MultiPolygon
        else if(shapeType==5)
        {

            readShapeHeader();

            List<polygons_list> pll_list = new ArrayList<polygons_list>();

            for (int i = 0, j = 1; i < numParts; )
            {


                final int count = m_parts[j++] - m_parts[i++];

                Coordinate[] coords  = new Coordinate[count];

                for (int c = 0; c < count; c++)
                {
                    final double x = EndianUtils.readSwappedDouble(m_dataInputStream);
                    final double y = EndianUtils.readSwappedDouble(m_dataInputStream);
                    if (c > 0)
                    {
                        coords[c] =  new Coordinate(x,y);
                    }
                    else
                    {
                        coords[c] =  new Coordinate(x,y);
                    }
                }

                GeometryFactory gf=new GeometryFactory();

                LineString lineString = gf.createLineString(coords);

                LinearRing ring= null;

                if( lineString.isClosed() )
                    ring = gf.createLinearRing(coords);
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

                Polygon polygon = gf.createPolygon(ring, holes);

                polygons_list pol_l = new polygons_list();
                pol_l.pll = polygon;
                pll_list.add(pol_l);

            }

            Polygon[] polygonSet = new Polygon[pll_list.size()];


            GeometryFactory multi_factory = new GeometryFactory();

            int k=0;
            for(polygons_list i : pll_list){

                polygonSet[k] = i.pll;

                k++;

            }


            MultiPolygon multiPolygon = new MultiPolygon(polygonSet, multi_factory);

            JtsMultiPolygon mljtspl = new JtsMultiPolygon();

            mljtspl.set_my_multi_polygon(multiPolygon);

            PointorMultiPolygon.setPointPnorMlPl(null);
            PointorMultiPolygon.setMultiPolygonPnorMlPl(multiPolygon);
            PointorMultiPolygon.setMultiPolygonorPoint_Shapetype(shapeType);


        }
        else
        {

            PointorMultiPolygon.setPointPnorMlPl(null);
            PointorMultiPolygon.setMultiPolygonPnorMlPl(null);
            PointorMultiPolygon.setMultiPolygonorPoint_Shapetype(0);

        }
            
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
    

    
    public class polygons_list{

        private Polygon pll;

    }

}
