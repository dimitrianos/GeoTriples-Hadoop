


import be.ugent.mmlab.rml.core.KeyGenerator;
import com.esri.io.PointPolygonFeatureWritable;



import com.esri.jts_extras.ShapeAttributeTitles;




import com.esri.mapreduce.PointPolygonFeatureInputFormat;

import org.apache.commons.codec.binary.Base64;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;



import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

import java.io.*;
import java.util.*;

import org.openrdf.model.Statement;



import org.openrdf.model.impl.URIImpl;
import org.openrdf.repository.RepositoryException;
import org.openrdf.rio.*;

import be.ugent.mmlab.rml.core.NodeRMLPerformer;
import be.ugent.mmlab.rml.core.RMLMappingFactory;
import be.ugent.mmlab.rml.function.Config;
import be.ugent.mmlab.rml.function.FunctionArea;
import be.ugent.mmlab.rml.function.FunctionAsGML;
import be.ugent.mmlab.rml.function.FunctionAsWKT;
import be.ugent.mmlab.rml.function.FunctionCentroidX;
import be.ugent.mmlab.rml.function.FunctionCentroidY;
import be.ugent.mmlab.rml.function.FunctionCoordinateDimension;
import be.ugent.mmlab.rml.function.FunctionDimension;
import be.ugent.mmlab.rml.function.FunctionEQUI;
import be.ugent.mmlab.rml.function.FunctionFactory;
import be.ugent.mmlab.rml.function.FunctionHasSerialization;
import be.ugent.mmlab.rml.function.FunctionIs3D;
import be.ugent.mmlab.rml.function.FunctionIsEmpty;
import be.ugent.mmlab.rml.function.FunctionIsSimple;
import be.ugent.mmlab.rml.function.FunctionLength;
import be.ugent.mmlab.rml.function.FunctionSpatialDimension;
import be.ugent.mmlab.rml.model.RMLMapping;
import be.ugent.mmlab.rml.model.TriplesMap;
import be.ugent.mmlab.rml.processor.RMLProcessor;
import be.ugent.mmlab.rml.processor.RMLProcessorFactory;
import be.ugent.mmlab.rml.processor.concrete.ConcreteRMLProcessorFactory;
import net.antidot.semantic.rdf.model.impl.sesame.SesameDataSet;
import net.antidot.semantic.rdf.rdb2rdf.r2rml.exception.InvalidR2RMLStructureException;
import net.antidot.semantic.rdf.rdb2rdf.r2rml.exception.InvalidR2RMLSyntaxException;
import net.antidot.semantic.rdf.rdb2rdf.r2rml.exception.R2RMLDataError;


//dimis

import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

/**
 * Created by bard on 4/3/16.
 */





public class Testing_class

{

    public static class ShapeFileMap extends
            Mapper<LongWritable, PointPolygonFeatureWritable, NullWritable, Writable> {


        private final Text m_text = new Text();

        private RDFFormat format = RDFFormat.NTRIPLES;

        private SesameDataSet outputDataSet = new SesameDataSet();

        private String m;

        private byte[] valueDecoded;

        private RMLProcessorFactory factory = new ConcreteRMLProcessorFactory();

        private RMLProcessor processor;

        private NodeRMLPerformer performer;

        private Map<String,KeyGenerator> keygens = new HashMap<String,KeyGenerator>();

        private List<TriplesMap> list_map;

        private List<String> field_titles;

        private int k=0;


        public void map(
                final LongWritable key,
                final PointPolygonFeatureWritable val,
                final Context context) throws IOException, InterruptedException {



            if(k==0) {


                //input filename to used on geotriples id's
                String filename = ((FileSplit) context.getInputSplit()).getPath().getName();

                eu.linkedeodata.geotriples.Config.variables.put("filename",filename);


                //get the shape file's attribute titles
                ShapeAttributeTitles shp_titles = new ShapeAttributeTitles();

                field_titles = shp_titles.getAll_titles();

                Set<String> hs = new HashSet();

                hs.addAll(field_titles);
                field_titles.clear();
                field_titles.addAll(hs);


                //read the mapping file (deserialize)
                Configuration conf = context.getConfiguration();

                m = conf.get("triplemap");

                valueDecoded = Base64.decodeBase64(m.getBytes());

                    try {

                        ObjectInputStream si = new ObjectInputStream( new ByteArrayInputStream(valueDecoded) );

                        list_map = (List<TriplesMap>) si.readObject();

                        for(TriplesMap tm:list_map) {

                            keygens.put(tm.getName(),new KeyGenerator());

                        }

                    } catch (Exception e) {

                        System.out.println(e);

                    }


                k++;

            }




            for(TriplesMap tm : list_map){

                processor = factory.create(tm.getLogicalSource().getReferenceFormulation());

                performer = new NodeRMLPerformer(processor);

                HashMap<String, Object> row = new HashMap();

                    try {

                        //write the attribute's titles
                        for(String a : field_titles) {

                            row.put(a,val.attributes.getText(a));

                        }

                        //check the record geometry type and write the right values
                        //ShapeType_for_Hadoop==5 => Polygon
                        if(val.ShapeType_for_Hadoop==5){

                            row.put("the_geom",val.polygon);

                        }
                        //ShapeType_for_Hadoop==1 => Point
                        else if(val.ShapeType_for_Hadoop==1){

                            row.put("the_geom",val.point);

                        }

                        row.put(Config.GEOTRIPLES_AUTO_ID,keygens.get(tm.getName()).Generate());

                    } catch (Exception e) {
                        e.printStackTrace();
                    }

                Collection<Statement> statements = performer.perform(row, outputDataSet,tm);

                ByteArrayOutputStream stream = new ByteArrayOutputStream();
                RDFWriter rdfWriter = Rio.createWriter(format,stream);

                    try {

                        rdfWriter.startRDF();


                            for(Statement st : statements){

                                rdfWriter.handleStatement(st);

                            }

                        rdfWriter.endRDF();

                        m_text.set(stream.toString());

                        context.write(NullWritable.get(), m_text);

                    } catch (RDFHandlerException e) {


                        e.printStackTrace();
                    }

            }


        }

    }



    public static class ShapeFileReduce
            extends Reducer<NullWritable,Text,NullWritable,Writable> {


        public void reduce(
                final NullWritable key,
                final Text val,
                final Context context) throws IOException, InterruptedException {

            context.write(key, val);
        }
    }




//    public static class PointMap extends
//            Mapper<LongWritable, PointFeatureWritable, NullWritable, Writable> {
//        private final static Text NAME = new Text("PERIGRAFH");
//
//        private final Text m_text = new Text();
//        private final Envelope m_envelope = new Envelope();
//        //private Point center = new Point();
//
//
//
//
//        public void map(
//                final LongWritable key,
//                final PointFeatureWritable val,
//                final Context context) throws IOException, InterruptedException {
//
//           // val.point.queryEnvelope(m_envelope);
//           // center=m_envelope.getCenter();
//
//           // m_text.set(String.format("%.6f %.6f %s",
//           //         center.getX(), center.getY(), val.attributes.get(NAME).toString()));
//
//            final Collection<Writable> values = val.attributes.values();
//
//
//
//
//            m_text.set(String.format("%.6f %.6f %s",
//                    val.point.getX(), val.point.getY(), values.toString()));
//
//
//            context.write(NullWritable.get(), m_text);
//        }
//    }





    public static void registerFunctions() {
        FunctionFactory.registerFunction(new URIImpl("http://www.w3.org/ns/r2rml-ext/functions/def/equi"),
                new FunctionEQUI()); // don't remove or change this line, it
        // replaces the equi join functionality
        // of R2RML

        FunctionFactory.registerFunction(new URIImpl("http://www.w3.org/ns/r2rml-ext/functions/def/asWKT"),
                new FunctionAsWKT());
        FunctionFactory.registerFunction(new URIImpl("http://www.w3.org/ns/r2rml-ext/functions/def/hasSerialization"),
                new FunctionHasSerialization());
        FunctionFactory.registerFunction(new URIImpl("http://www.w3.org/ns/r2rml-ext/functions/def/asGML"),
                new FunctionAsGML());
        FunctionFactory.registerFunction(new URIImpl("http://www.w3.org/ns/r2rml-ext/functions/def/isSimple"),
                new FunctionIsSimple());
        FunctionFactory.registerFunction(new URIImpl("http://www.w3.org/ns/r2rml-ext/functions/def/isEmpty"),
                new FunctionIsEmpty());
        FunctionFactory.registerFunction(new URIImpl("http://www.w3.org/ns/r2rml-ext/functions/def/is3D"),
                new FunctionIs3D());
        FunctionFactory.registerFunction(new URIImpl("http://www.w3.org/ns/r2rml-ext/functions/def/spatialDimension"),
                new FunctionSpatialDimension());
        FunctionFactory.registerFunction(new URIImpl("http://www.w3.org/ns/r2rml-ext/functions/def/dimension"),
                new FunctionDimension());
        FunctionFactory.registerFunction(
                new URIImpl("http://www.w3.org/ns/r2rml-ext/functions/def/coordinateDimension"),
                new FunctionCoordinateDimension());
        FunctionFactory.registerFunction(new URIImpl("http://www.w3.org/ns/r2rml-ext/functions/def/area"),
                new FunctionArea());
        FunctionFactory.registerFunction(new URIImpl("http://www.w3.org/ns/r2rml-ext/functions/def/length"),
                new FunctionLength());
        FunctionFactory.registerFunction(new URIImpl("http://www.w3.org/ns/r2rml-ext/functions/def/centroidx"),
                new FunctionCentroidX());
        FunctionFactory.registerFunction(new URIImpl("http://www.w3.org/ns/r2rml-ext/functions/def/centroidy"),
                new FunctionCentroidY());

    }

        public static String all_triples_maps()
                throws RepositoryException, R2RMLDataError, RDFParseException, IOException, InvalidR2RMLSyntaxException, InvalidR2RMLStructureException {

            registerFunctions();


            //an eimaste se perivallon gia peiramata sto hdfs tote exoume to parakatw block kwdika
//            Configuration conf = new Configuration();
//            FileSystem fs = FileSystem.get(conf);
//            Path inFile = new Path("hdfs://hadoop-p2-1:9000/hadoop/afg_adm_shp.ttl");
//            FSDataInputStream in = fs.open(inFile);
//            RMLMapping mapping = RMLMappingFactory.extractRMLMapping(in);

            //an trexoume se pseudodistributed xwris pragmatiko hdfs trexoume to parakatw
            RMLMapping mapping = RMLMappingFactory.extractRMLMapping("Hadoop_Implementation/hdfs_in/afg_adm_shp.ttl");



            Config.EPSG_CODE = "4326";

            String encoded = null;
            ArrayList<TriplesMap> list=new ArrayList<TriplesMap>();

            for (TriplesMap m : mapping.getTriplesMaps()) {
                list.add(m);
            }


                    // serialize the object
                    try {
                        ByteArrayOutputStream bo = new ByteArrayOutputStream();
                        ObjectOutputStream so = new ObjectOutputStream(bo);
                        so.writeObject(list);
                        so.flush();

                        encoded = new String(Base64.encodeBase64(bo.toByteArray()));
                    } catch (Exception e) {
                        System.out.println(e);
                    }






            return encoded;
        }



    public static void main(String[] args) throws Exception {


        Configuration conf = new Configuration();

        //read mapping file (serialize) and pass it to configuration
        conf.set("triplemap",all_triples_maps());

//
//
//        conf.addResource(new File("hdfs_in/myconf.xml").getAbsoluteFile().toURI().toURL());
//
//
//        System.out.println("FFFFFFFFFF " + conf);
//
//        System.out.println("GGGGGGGGGG " + conf.get("var1"));

        Job job = Job.getInstance(conf, "word count");

        job.setJarByClass(Testing_class.class);
        job.setMapperClass(ShapeFileMap.class);

        job.setMapOutputKeyClass(NullWritable.class);
        job.setMapOutputValueClass(Text.class);

        job.setNumReduceTasks(1);

        job.setCombinerClass(ShapeFileReduce.class);
        job.setReducerClass(ShapeFileReduce.class);

        job.setInputFormatClass(PointPolygonFeatureInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);


        //local
        FileInputFormat.addInputPath(job, new Path("Hadoop_Implementation/input"));
        FileInputFormat.setInputDirRecursive(job,true);
        FileOutputFormat.setOutputPath(job, new Path("Hadoop_Implementation/output"));

        //hdfs
       // FileInputFormat.addInputPath(job, new Path("hdfs://hadoop-p2-1:9000/hadoop/input"));
       // FileOutputFormat.setOutputPath(job, new Path("hdfs://hadoop-p2-1:9000/hadoop/output"));



        System.exit(job.waitForCompletion(true) ? 0 : 1);

    }

}