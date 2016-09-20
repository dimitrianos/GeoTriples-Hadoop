
import java.io.IOException;
import org.openrdf.model.Statement;
import org.openrdf.rio.RDFFormat;
import org.openrdf.rio.RDFHandlerException;
import org.openrdf.rio.RDFWriter;
import org.openrdf.rio.ntriples.NTriplesUtil;

/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */

/**
 *
 * @author dimis
 */
public class NTriplesAlternative implements RDFWriter {

    /*-----------
	  Variables
    * -----------*/

    private boolean writingStarted;
    private StringBuilder sb;

    /*--------------*
	  Constructors *
	 --------------*/

    /**
     * Creates a new NTriplesWriter that will write to the supplied
     * OutputStream.
     *
     * @param out The OutputStream to write the N-Triples document to.
     */
    public NTriplesAlternative() {
        this.sb = new StringBuilder(1024);
        writingStarted = false;
    }

    /*---------*
	 * Methods *
    * ---------*/
    public RDFFormat getRDFFormat() {
        return RDFFormat.NTRIPLES;
    }

    public void startRDF()
            throws RDFHandlerException {
        if (writingStarted) {
            throw new RuntimeException("Document writing has already started");
        }

        writingStarted = true;
    }

    public void endRDF()
            throws RDFHandlerException {
        if (!writingStarted) {
            throw new RuntimeException("Document writing has not yet started");
        }
        this.sb = new StringBuilder(sb.capacity());
//		try {
//			writer.flush();
//		}
//		catch (IOException e) {
//			throw new RDFHandlerException(e);
//		}
//		finally {
//			writingStarted = false;
//		}
    }

    public void handleNamespace(String prefix, String name) {
        // N-Triples does not support namespace prefixes.
    }

    public void handleStatement(Statement st)
            throws RDFHandlerException {
        if (!writingStarted) {
            throw new RuntimeException("Document writing has not yet been started");
        }

        try {
            NTriplesUtil.append(st.getSubject(), sb);
            sb.append(" ");
            NTriplesUtil.append(st.getPredicate(), sb);
            sb.append(" ");
            NTriplesUtil.append(st.getObject(), sb);
            sb.append(" .\n");
        } catch (IOException e) {
            throw new RDFHandlerException(e);
        }
    }

    public void handleComment(String comment){
        sb.append("# ");
        sb.append(comment);
        sb.append("\n");
    }

    public String getString(){
        return sb.toString();
    }
}
