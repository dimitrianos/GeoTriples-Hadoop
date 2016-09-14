


import be.ugent.mmlab.rml.core.KeyGenerator;

import com.esri.io.PointMultiPolygonFeatureWritable;
import com.esri.jts_extras.ShapeAttributeTitles;
import com.esri.mapreduce.PointMultiPolygonFeatureInputFormat;

import org.apache.commons.codec.binary.Base64;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
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



/**
 * Created by bard on 4/3/16.
 */



public class Hadoop_Implementation_class
{

    public static class ShapeFileMap extends
            Mapper<LongWritable, PointMultiPolygonFeatureWritable, Writable, NullWritable> {


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

        @Override
        public void map(
                final LongWritable key,
                final PointMultiPolygonFeatureWritable val,
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


            registerFunctions();

            for(TriplesMap tm : list_map){


                processor = factory.create(tm.getLogicalSource().getReferenceFormulation());

                performer = new NodeRMLPerformer(processor);

                HashMap<String, Object> row = new HashMap();

                    try {

                        //write the attribute's titles
                        for(String a : field_titles) {

                            String field_value = val.attributes.getText(a).trim();

                            if(!field_value.isEmpty()) {
                                row.put(a, field_value);
                            }

                        }

                        //check the record geometry type and write the right values
                        //ShapeType_for_Hadoop==5 => MultiPolygon
                        if(val.ShapeType_for_Hadoop==5){


                            row.put("the_geom",val.multiPolygon);


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


                        String statements_str = stream.toString();


                        m_text.set(statements_str.substring(0,statements_str.length()-1));

                        context.write(m_text, NullWritable.get());

                    } catch (RDFHandlerException e) {


                        e.printStackTrace();
                    }

            }


        }

    }



    public static class ShapeFileReduce
            extends Reducer<Text, NullWritable,Writable,NullWritable> {

        @Override
        protected void reduce(Text key, Iterable<NullWritable> values, Context context) throws IOException, InterruptedException {
            System.out.println("The key is:" + key.toString());

            for (NullWritable val : values) {

                System.out.println(val.toString());
            }
            context.write(key, NullWritable.get());
        }

    }



////csv
//    public static class CsvFileMap extends
//            Mapper<LongWritable, Text, NullWritable, Writable> {
//
//
//        private final Text m_text = new Text();
//
//        private List<String> field_titles;
//
//        private ListregisterFunctions();<String> field_values;
//
//        private String m;
//
//        private byte[] valueDecoded;
//
//        private Map<String,KeyGenerator> keygens = new HashMap<String,KeyGenerator>();
//
//        private RMLProcessorFactory factory = new ConcreteRMLProcessorFactory();
//
//        private RMLProcessor processor;
//
//        private NodeRMLPerformer performer;
//
//        private List<TriplesMap> list_map;
//
//        private RDFFormat format = RDFFormat.NTRIPLES;
//
//        private SesameDataSet outputDataSet = new SesameDataSet();
//
//        private int k=0;
//
//
//        public void map(
//                final LongWritable key,
//                final Text val,
//                final Context context) throws IOException, InterruptedException {
//
//
//            String line = val.toString();
//
//            long key_value=key.get();
//
//
//            //read titles on first line
//            if(key_value==0) {
//
//                //input filename to used on geotriples id's
//                String filename = ((FileSplit) context.getInputSplit()).getPath().getName();
//
//                eu.linkedeodata.geotriples.Config.variables.put("filename",filename);
//
//
//                field_titles = Arrays.asList(line.split(","));
//
//                //read the mapping file (deserialize)
//                Configuration conf = context.getConfiguration();
//
//                m = conf.get("triplemap");
//
//                valueDecoded = Base64.decodeBase64(m.getBytes());
//
//                try {
//
//                    ObjectInputStream si = new ObjectInputStream( new ByteArrayInputStream(valueDecoded) );
//
//                    list_map = (List<TriplesMap>) si.readObject();
//
//                    for(TriplesMap tm:list_map) {
//
//                        keygens.put(tm.getName(),new KeyGenerator());
//
//                    }
//
//                } catch (Exception e) {
//
//                    System.out.println(e);
//
//                }
//
//                k++;
//
//
//            }
//            else {
//
//
//                for(TriplesMap tm : list_map){
//
//                    processor = factory.create(tm.getLogicalSource().getReferenceFormulation());
//
//                    performer = new NodeRMLPerformer(processor);
//
//                    HashMap<String, Object> row = new HashMap();
//
//                    try {
//
//                        //write the attribute's titles and values
//                        field_values = Arrays.asList(line.split(",(?=([^\"]*\"[^\"]*\")*[^\"]*$)"));
//
//
//                        int field_titles_index = 0;
//
//
//                        for(String a : field_titles) {
//
//                            //row.put(a,field_values.get(field_titles_index));
//                            row.put(a,field_values.get(field_titles_index).replaceAll("\"",""));
//
//                            field_titles_index++;
//
//                        }
//
//                        row.put(Config.GEOTRIPLES_AUTO_ID,keygens.get(tm.getName()).Generate());
//
//                    } catch (Exception e) {
//                        e.printStackTrace();
//                    }
//
//
//                    Collection<Statement> statements = performer.perform(row, outputDataSet,tm);
//
//                    ByteArrayOutputStream stream = new ByteArrayOutputStream();
//                    RDFWriter rdfWriter = Rio.createWriter(format,stream);
//
//
//                    try {
//
//                        rdfWriter.startRDF();
//
//
//                        for(Statement st : statements){
//
//                            rdfWriter.handleStatement(st);
//
//                        }
//
//                        rdfWriter.endRDF();
//
//                        m_text.set(stream.toString());
//
//                        context.write(NullWritable.get(), m_text);
//
//                    } catch (RDFHandlerException e) {
//
//
//                        e.printStackTrace();
//                    }
//
//
//                }
//
//            }
//
//
//        }
//    }



    //csv_with bufferer reader
    public static class CsvFileMap extends
            Mapper<LongWritable, Text, NullWritable, Writable> {


        private final Text m_text = new Text();

        private List<String> field_titles;

        private List<String> field_values;

        private String m;

        private String header_values;

        private byte[] valueDecoded;

        private Map<String,KeyGenerator> keygens = new HashMap<String,KeyGenerator>();

        private RMLProcessorFactory factory = new ConcreteRMLProcessorFactory();

        private RMLProcessor processor;

        private NodeRMLPerformer performer;

        private List<TriplesMap> list_map;

        private RDFFormat format = RDFFormat.NTRIPLES;

        private SesameDataSet outputDataSet = new SesameDataSet();

        private int k=0;


        public void map(
                final LongWritable key,
                final Text val,
                final Context context) throws IOException, InterruptedException {


            String line = val.toString();

            //ignore line with headers

            long key_value = key.get();

            if (key_value != 0) {

                //read titles and triples map
                if (k == 0) {

                    //input filename to used on geotriples id's
                    String filename = ((FileSplit) context.getInputSplit()).getPath().getName();

                    eu.linkedeodata.geotriples.Config.variables.put("filename", filename);


                    //read the mapping file (deserialize) and headers
                    Configuration conf = context.getConfiguration();

                    header_values = conf.get("header_values");

                    field_titles = Arrays.asList(header_values.split(","));

                    m = conf.get("triplemap");

                    valueDecoded = Base64.decodeBase64(m.getBytes());

                    try {

                        ObjectInputStream si = new ObjectInputStream(new ByteArrayInputStream(valueDecoded));

                        list_map = (List<TriplesMap>) si.readObject();

                        for (TriplesMap tm : list_map) {

                            keygens.put(tm.getName(), new KeyGenerator());

                        }

                    } catch (Exception e) {

                        System.out.println(e);

                    }

                    k++;


                }

                registerFunctions();

                for (TriplesMap tm : list_map) {

                    processor = factory.create(tm.getLogicalSource().getReferenceFormulation());

                    performer = new NodeRMLPerformer(processor);

                    HashMap<String, Object> row = new HashMap();

                    try {

                        //write the attribute's titles and values
                        field_values = Arrays.asList(line.split(",(?=([^\"]*\"[^\"]*\")*[^\"]*$)"));


                        int field_titles_index = 0;


                        for (String a : field_titles) {

                            //row.put(a,field_values.get(field_titles_index));
                            row.put(a, field_values.get(field_titles_index).replaceAll("\"", ""));

                            field_titles_index++;

                        }

                        row.put(Config.GEOTRIPLES_AUTO_ID, keygens.get(tm.getName()).Generate());

                    } catch (Exception e) {
                        e.printStackTrace();
                    }


                    Collection<Statement> statements = performer.perform(row, outputDataSet, tm);

                    ByteArrayOutputStream stream = new ByteArrayOutputStream();
                    RDFWriter rdfWriter = Rio.createWriter(format, stream);


                    try {

                        rdfWriter.startRDF();


                        for (Statement st : statements) {

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
    }



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

        public static String all_triples_maps(String mapping_file_path)
                throws RepositoryException, R2RMLDataError, RDFParseException, IOException, InvalidR2RMLSyntaxException, InvalidR2RMLStructureException {


            Configuration conf = new Configuration();
            FileSystem fs = FileSystem.get(conf);
            Path inFile = new Path(mapping_file_path);
            FSDataInputStream in = fs.open(inFile);
            RMLMapping mapping = RMLMappingFactory.extractRMLMapping(in);


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



        if(args.length>0) {

            Configuration conf = new Configuration();

            FileSystem fs = FileSystem.get(conf);
            Path inFile = new Path(args[0]);
            FSDataInputStream in = fs.open(inFile);


            conf.addResource(in);


            //shapefile
            if(conf.get("files_format").equals("shp")) {


                //read mapping file (serialize) and pass it to configuration
                conf.set("triplemap",all_triples_maps(conf.get("mapping_file")));

                Job job = Job.getInstance(conf, "geotriples");

                job.setJarByClass(Hadoop_Implementation_class.class);
                job.setMapperClass(ShapeFileMap.class);

                job.setMapOutputKeyClass(Text.class);
                job.setMapOutputValueClass(NullWritable.class);


                job.setNumReduceTasks(Integer.parseInt(conf.get("reducers_number")));

                //job.setCombinerClass(ShapeFileReduce.class);
                job.setReducerClass(ShapeFileReduce.class);

                job.setInputFormatClass(PointMultiPolygonFeatureInputFormat.class);
                job.setOutputFormatClass(TextOutputFormat.class);


                FileInputFormat.addInputPath(job, new Path(conf.get("input_dir")));
//            //FileInputFormat.setInputDirRecursive(job,true);
                FileOutputFormat.setOutputPath(job, new Path(conf.get("output_dir")));



                job.waitForCompletion(true);




            if(conf.get("merge").equals("1")) {


                FileSystem fileSystem = FileSystem.get(conf);


                Path source = new Path(conf.get("output_dir"));

                Path new_target = new Path(conf.get("output_dir")+"_new");

                fileSystem.mkdirs(new_target);

                Path target = new Path(conf.get("output_dir")+"_new");

                FileUtil.copyMerge(fileSystem, source,fileSystem, target, true , conf, null);

                fileSystem.rename(target,source);



            }


                TaskReport[] map_reports = job.getTaskReports(TaskType.MAP);

                System.out.println(map_reports.length);
                for(TaskReport report : map_reports) {

                    long time = report.getFinishTime() - report.getStartTime();
                    System.out.println("Map: " + report.getTaskId() + " took " + time + " millis!");
                }

                TaskReport[] reduce_reports = job.getTaskReports(TaskType.REDUCE);

                System.out.println(reduce_reports.length);
                for(TaskReport report : reduce_reports) {

                    long time = report.getFinishTime() - report.getStartTime();
                    System.out.println("Reduce: " + report.getTaskId() + " took " + time + " millis!");
                }

            }
            //csv
            else if(conf.get("files_format").equals("csv")) {


                File folder = new File(conf.get("input_dir"));
                File[] listOfFiles = folder.listFiles();

                String filename = listOfFiles[0].getName();

                BufferedReader br = new BufferedReader(new FileReader(conf.get("input_dir")+"/"+filename));

                String header_values = br.readLine();


                conf.set("header_values",header_values);


                Job job = Job.getInstance(conf, "geotriples");

                job.setJarByClass(Hadoop_Implementation_class.class);
                job.setMapperClass(CsvFileMap.class);

                job.setMapOutputKeyClass(NullWritable.class);
                job.setMapOutputValueClass(Text.class);

                job.setNumReduceTasks(Integer.parseInt(conf.get("reducers_number")));

                job.setCombinerClass(ShapeFileReduce.class);
                job.setReducerClass(ShapeFileReduce.class);

                job.setInputFormatClass(TextInputFormat.class);
                job.setOutputFormatClass(TextOutputFormat.class);


                FileInputFormat.addInputPath(job, new Path(conf.get("input_dir")));
//            //FileInputFormat.setInputDirRecursive(job,true);
                FileOutputFormat.setOutputPath(job, new Path(conf.get("output_dir")));



                job.waitForCompletion(true);


                if(conf.get("merge").equals("1")) {


                    FileSystem fileSystem = FileSystem.get(conf);


                    Path source = new Path(conf.get("output_dir"));

                    Path new_target = new Path(conf.get("output_dir")+"_new");

                    fileSystem.mkdirs(new_target);

                    Path target = new Path(conf.get("output_dir")+"_new");

                    FileUtil.copyMerge(fileSystem, source,fileSystem, target, true , conf, null);

                    fileSystem.rename(target,source);



                }


                TaskReport[] map_reports = job.getTaskReports(TaskType.MAP);

                System.out.println(map_reports.length);
                for(TaskReport report : map_reports) {

                    long time = report.getFinishTime() - report.getStartTime();
                    System.out.println("Map: " + report.getTaskId() + " took " + time + " millis!");
                }

                TaskReport[] reduce_reports = job.getTaskReports(TaskType.REDUCE);

                System.out.println(reduce_reports.length);
                for(TaskReport report : reduce_reports) {

                    long time = report.getFinishTime() - report.getStartTime();
                    System.out.println("Reduce: " + report.getTaskId() + " took " + time + " millis!");
                }




            }
            else {

                System.out.println("Please provide correct file format");
                System.exit(0);

            }


        }
        else {

            System.out.println("Please provide the xml configuration file");
            System.exit(0);

        }



    }

}






//        //shapefile
//        if(args[0].equals("shp")) {
//
//
//            Configuration conf = new Configuration();
//
//            //read mapping file (serialize) and pass it to configuration
//            //conf.set("triplemap",all_triples_maps());
//
//            Job job = Job.getInstance(conf, "geotriples");
//
//            job.setJarByClass(Hadoop_Implementation_class.class);
//            job.setMapperClass(ShapeFileMap.class);
//
//            job.setMapOutputKeyClass(Text.class);
//            job.setMapOutputValueClass(NullWritable.class);
//
//
//            job.setNumReduceTasks(Integer.parseInt(args[3]));
//
//            //job.setCombinerClass(ShapeFileReduce.class);
//            job.setReducerClass(ShapeFileReduce.class);
//
//            job.setInputFormatClass(PointMultiPolygonFeatureInputFormat.class);
//            job.setOutputFormatClass(TextOutputFormat.class);
//
//
//            //local
//           FileInputFormat.addInputPath(job, new Path(System.getProperty("user.dir")+"/Hadoop_Implementation/"+args[1]));
////            //FileInputFormat.setInputDirRecursive(job,true);
//          FileOutputFormat.setOutputPath(job, new Path(System.getProperty("user.dir")+"/Hadoop_Implementation/"+args[2]));
//
//            //hdfs
//            //FileInputFormat.addInputPath(job, new Path("hdfs://hadoop-p2-1:9000/hadoop/"+args[1]));
//            //FileOutputFormat.setOutputPath(job, new Path("hdfs://hadoop-p2-1:9000/hadoop/"+args[2]));
//
//
//            job.waitForCompletion(true);





//            FileSystem fileSystem = FileSystem.get(conf);
//
//
//            Path source = new Path(System.getProperty("user.dir")+"/Hadoop_Implementation/"+args[2]);
//
//            Path target = new Path(System.getProperty("user.dir")+"/Hadoop_Implementation/new");
//
//
//            FileUtil.copyMerge(fileSystem, source,fileSystem, target, true , conf, null);
//
//
//            fileSystem.rename(target,source);




//
//
//            TaskReport[] map_reports = job.getTaskReports(TaskType.MAP);
//
//            System.out.println(map_reports.length);
//            for(TaskReport report : map_reports) {
//
//                long time = report.getFinishTime() - report.getStartTime();
//                System.out.println("Map: " + report.getTaskId() + " took " + time + " millis!");
//            }
//
//            TaskReport[] reduce_reports = job.getTaskReports(TaskType.REDUCE);
//
//            System.out.println(reduce_reports.length);
//            for(TaskReport report : reduce_reports) {
//
//                long time = report.getFinishTime() - report.getStartTime();
//                System.out.println("Reduce: " + report.getTaskId() + " took " + time + " millis!");
//            }
//
//
//        }
//        //csv
//        else if(args[0].equals("csv")) {
//
//            Configuration conf = new Configuration();
//
//            //read mapping file (serialize) and pass it to configuration
//            //conf.set("triplemap",all_triples_maps());
//
//
//
//
//            File folder = new File("Hadoop_Implementation/"+args[2]);
//            File[] listOfFiles = folder.listFiles();
//
//            String filename = listOfFiles[0].getName();
//
//            BufferedReader br = new BufferedReader(new FileReader("Hadoop_Implementation/"+args[2]+"/"+filename));
//
//            String header_values = br.readLine();
//
//
//            conf.set("header_values",header_values);
//
//
//            Job job = Job.getInstance(conf, "word count");
//
//            job.setJarByClass(Hadoop_Implementation_class.class);
//            job.setMapperClass(CsvFileMap.class);
//
//            job.setMapOutputKeyClass(NullWritable.class);
//            job.setMapOutputValueClass(Text.class);
//
//            job.setNumReduceTasks(1);
//
//            job.setCombinerClass(ShapeFileReduce.class);
//            job.setReducerClass(ShapeFileReduce.class);
//
//            job.setInputFormatClass(TextInputFormat.class);
//            job.setOutputFormatClass(TextOutputFormat.class);
//
//
//
//            //local
//            //FileInputFormat.addInputPath(job, new Path("Hadoop_Implementation/"+args[2]));
//            //FileInputFormat.setInputDirRecursive(job,true);
//            //FileOutputFormat.setOutputPath(job, new Path("Hadoop_Implementation/"+args[3]));
//
//            //hdfs
//            //  FileInputFormat.addInputPath(job, new Path("hdfs://hadoop-p2-1:9000/"+args[2]));
//            //FileOutputFormat.setOutputPath(job, new Path("hdfs://hadoop-p2-1:9000/"+args[3]));
//
//            job.waitForCompletion(true);
//
//            TaskReport[] map_reports = job.getTaskReports(TaskType.MAP);
//
//            System.out.println(map_reports.length);
//            for(TaskReport report : map_reports) {
//
//                long time = report.getFinishTime() - report.getStartTime();
//                System.out.println("Map: " + report.getTaskId() + " took " + time + " millis!");
//            }
//
//            TaskReport[] reduce_reports = job.getTaskReports(TaskType.MAP);
//
//            System.out.println(reduce_reports.length);
//            for(TaskReport report : reduce_reports) {
//
//                long time = report.getFinishTime() - report.getStartTime();
//                System.out.println("Reduce: " + report.getTaskId() + " took " + time + " millis!");
//            }
//
//
//
//        }










//        Configuration conf = new Configuration();
//
//
//
//
//        //read mapping file (serialize) and pass it to configuration
//        conf.set("triplemap",all_triples_maps());
//
////
////
////        conf.addResource(new File("hdfs_in/myconf_local.xml").getAbsoluteFile().toURI().toURL());
////
////
////        System.out.println("FFFFFFFFFF " + conf);
////
////        System.out.println("GGGGGGGGGG " + conf.get("var1"));
//
//
//
//        //Shapefile
////
//
//
//        Job job = Job.getInstance(conf, "geotriples");
//
//
//
//        job.setJarByClass(Hadoop_Implementation_class.class);
//        job.setMapperClass(ShapeFileMap.class);
//
//        job.setMapOutputKeyClass(NullWritable.class);
//        job.setMapOutputValueClass(Text.class);
//
//        job.setNumReduceTasks(1);
//
//        job.setCombinerClass(ShapeFileReduce.class);
//        job.setReducerClass(ShapeFileReduce.class);
//
//        job.setInputFormatClass(PointMultiPolygonFeatureInputFormat.class);
//        job.setOutputFormatClass(TextOutputFormat.class);
//
//
//
//        //csv
////         Job job = Job.getInstance(conf, "word count");
////
////        job.setJarByClass(Hadoop_Implementation_class.class);
////        job.setMapperClass(CsvFileMap.class);
////
////        job.setMapOutputKeyClass(NullWritable.class);
////        job.setMapOutputValueClass(Text.class);
////
////        job.setNumReduceTasks(1);
////
////        job.setCombinerClass(ShapeFileReduce.class);
////        job.setReducerClass(ShapeFileReduce.class);
////
////        job.setInputFormatClass(TextInputFormat.class);
////        job.setOutputFormatClass(TextOutputFormat.class);
//
//
////        //csv with buffered reader
////
////        File folder = new File("Hadoop_Implementation/input");
////        File[] listOfFiles = folder.listFiles();
////
//////        for (int i = 0; i < listOfFiles.length; i++) {
//////            if (listOfFiles[i].isFile()) {
//////                System.out.println("File " + listOfFiles[i].getName());
//////            } else if (listOfFiles[i].isDirectory()) {
//////                System.out.println("Directory " + listOfFiles[i].getName());
//////            }
//////        }
////
////
////        String filename = listOfFiles[0].getName();
////
////        BufferedReader br = new BufferedReader(new FileReader("Hadoop_Implementation/input/"+filename));
////
////        String header_values = br.readLine();
////
////
////
////        conf.set("header_values",header_values);
////
////
////        Job job = Job.getInstance(conf, "word count");
////
////        job.setJarByClass(Hadoop_Implementation_class.class);
////        job.setMapperClass(CsvFileMap.class);
////
////        job.setMapOutputKeyClass(NullWritable.class);
////        job.setMapOutputValueClass(Text.class);
////
////        job.setNumReduceTasks(1);
////
////        job.setCombinerClass(ShapeFileReduce.class);
////        job.setReducerClass(ShapeFileReduce.class);
////
////        job.setInputFormatClass(TextInputFormat.class);
////        job.setOutputFormatClass(TextOutputFormat.class);
//
//
//
//
//
//
//        //local
//        FileInputFormat.addInputPath(job, new Path("Hadoop_Implementation/input"));
//        //FileInputFormat.setInputDirRecursive(job,true);
//        FileOutputFormat.setOutputPath(job, new Path("Hadoop_Implementation/output"));
//
//        //hdfs
//      //  FileInputFormat.addInputPath(job, new Path("hdfs://hadoop-p2-1:9000/"+args[0]));
//        //FileOutputFormat.setOutputPath(job, new Path("hdfs://hadoop-p2-1:9000/"+args[1]));
//
//
//
//        job.waitForCompletion(true);
//
//        TaskReport[] map_reports = job.getTaskReports(TaskType.MAP);
//
//        System.out.println(map_reports.length);
//        for(TaskReport report : map_reports) {
//
//            long time = report.getFinishTime() - report.getStartTime();
//            System.out.println("Map: " + report.getTaskId() + " took " + time + " millis!");
//        }
//
//        TaskReport[] reduce_reports = job.getTaskReports(TaskType.MAP);
//
//        System.out.println(reduce_reports.length);
//        for(TaskReport report : reduce_reports) {
//
//            long time = report.getFinishTime() - report.getStartTime();
//            System.out.println("Reduce: " + report.getTaskId() + " took " + time + " millis!");
//        }


