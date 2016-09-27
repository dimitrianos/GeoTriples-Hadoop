package com.esri.shp;


import com.esri.jts_extras.PointorMultiPolygon;
import com.vividsolutions.jts.geom.Coordinate;
import com.vividsolutions.jts.geom.CoordinateFilter;
import com.vividsolutions.jts.geom.GeometryFactory;
import com.vividsolutions.jts.geom.LinearRing;
import com.vividsolutions.jts.geom.MultiPolygon;

import org.apache.commons.io.EndianUtils;

import java.io.DataInputStream;
import java.io.IOException;
import java.io.Serializable;
import java.util.Arrays;
import java.util.logging.Level;
import java.util.logging.Logger;
import org.apache.commons.io.EndianUtils;

/**
 * http://www.esri.com/library/whitepapers/pdfs/shapefile.pdf
 */
public class ShpReader implements Serializable {

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

    public GeometryFactory multi_factory = new GeometryFactory();
    public GeometryFactory gf = new GeometryFactory();

    byte[] buffer = new byte[32];

    public ShpReader(final DataInputStream dataInputStream) throws IOException {
        m_dataInputStream = dataInputStream;
        m_shpHeader = new ShpHeader(dataInputStream);
    }

    public ShpHeader getHeader() {
        return m_shpHeader;
    }

    public boolean hasMore() throws IOException {
        return m_dataInputStream.available() > 0;
    }

    private void readRecordHeader() throws IOException {
        recordNumber = m_dataInputStream.readInt();
        contentLength = m_dataInputStream.readInt();
        contentLengthInBytes = contentLength + contentLength - 4;

        m_dataInputStream.readFully(buffer, 0, 4);
        shapeType = EndianUtils.readSwappedInteger(buffer, 0);

    }

    //a fuction that decides the geometry type of the record and sets the right values
    public void PointorMultiPolygon_Selector(final Point point) throws IOException {

        readRecordHeader();

        //shapeType==1 => Point
        if (shapeType == 1) {

            point.apply(new CoordinateFilter() {

                @Override
                public void filter(Coordinate coord) {

                    try {

                        coord.x = EndianUtils.readSwappedDouble(m_dataInputStream);

                    } catch (IOException ex) {

                        Logger.getLogger(ShpReader.class.getName()).log(Level.SEVERE, null, ex);

                    }

                    try {

                        coord.y = EndianUtils.readSwappedDouble(m_dataInputStream);

                    } catch (IOException ex) {

                        Logger.getLogger(ShpReader.class.getName()).log(Level.SEVERE, null, ex);

                    }

                }

            });

            PointorMultiPolygon.setPointPnorMlPl(point);
            PointorMultiPolygon.setMultiPolygonPnorMlPl(null);
            PointorMultiPolygon.setMultiPolygonorPoint_Shapetype(shapeType);

        } //shapeType==5 => MultiPolygon
        else if (shapeType == 5) {

            readShapeHeader();

            Polygon[] pll_list=new Polygon[numParts];

            for (int i = 0, j = 1; i < numParts; ++i) {

                final int count = m_parts[j++] - m_parts[i];

                Coordinate[] coords = new Coordinate[count];

                for (int c = 0; c < count; c++) {
                    m_dataInputStream.readFully(buffer, 0, 16);

                    final double x = EndianUtils.readSwappedDouble(buffer, 0);
                    final double y = EndianUtils.readSwappedDouble(buffer, 8);

                    if (c > 0) {
                        coords[c] = new Coordinate(x, y);
                    } else {
                        coords[c] = new Coordinate(x, y);
                    }
                }


                LinearRing ring= null;
                if( coords[coords.length-1].equals(coords[0])) {


                }
                else {

                    coords = Arrays.copyOf(coords,coords.length+1);
                    coords[coords.length-1]=coords[0];

                }

                ring = gf.createLinearRing(coords);

                LinearRing holes[] = null;

                Polygon polygon = gf.createPolygon(ring, holes);

                pll_list[i]=polygon;

            }


            MultiPolygon multiPolygon = new MultiPolygon(pll_list, multi_factory);


            PointorMultiPolygon.setPointPnorMlPl(null);
            PointorMultiPolygon.setMultiPolygonPnorMlPl(multiPolygon);
            PointorMultiPolygon.setMultiPolygonorPoint_Shapetype(shapeType);

        } else {

            PointorMultiPolygon.setPointPnorMlPl(null);
            PointorMultiPolygon.setMultiPolygonPnorMlPl(null);
            PointorMultiPolygon.setMultiPolygonorPoint_Shapetype(0);

        }

    }

    private void readShapeHeader() throws IOException {
        m_dataInputStream.readFully(buffer, 0, 32);
        xmin = EndianUtils.readSwappedDouble(buffer, 0);
        ymin = EndianUtils.readSwappedDouble(buffer, 8);
        xmax = EndianUtils.readSwappedDouble(buffer, 16);
        ymax = EndianUtils.readSwappedDouble(buffer, 24);

        m_dataInputStream.readFully(buffer, 0, 8);
        numParts = EndianUtils.readSwappedInteger(buffer, 0);
        numPoints = EndianUtils.readSwappedInteger(buffer, 4);

        if ((numParts + 1) > m_parts.length) {
            m_parts = new int[numParts + 1];
        }
        for (int p = 0; p < numParts; p++) {
            m_parts[p] = EndianUtils.readSwappedInteger(m_dataInputStream);
        }
        m_parts[numParts] = numPoints;


}
