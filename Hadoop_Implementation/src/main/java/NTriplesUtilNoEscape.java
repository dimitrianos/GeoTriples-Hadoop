
import java.io.IOException;
import org.openrdf.model.Literal;
import org.openrdf.model.Resource;
import org.openrdf.model.Value;
import org.openrdf.rio.ntriples.NTriplesUtil;
import static org.openrdf.rio.ntriples.NTriplesUtil.append;
import static org.openrdf.rio.ntriples.NTriplesUtil.escapeString;

/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */

/**
 *
 * @author dimis
 */
public class NTriplesUtilNoEscape {
    public static void append(Value value, Appendable appendable)
		throws IOException
	{
		if (value instanceof Resource) {
			NTriplesUtil.append((Resource)value, appendable);
		}
		else if (value instanceof Literal) {
			append((Literal)value, appendable);
		}
		else {
			throw new IllegalArgumentException("Unknown value type: " + value.getClass());
		}
	}
    public static void append(Literal lit, Appendable appendable)
		throws IOException
	{
		// Do some character escaping on the label:
		appendable.append("\"");
		appendable.append(lit.getLabel());
		appendable.append("\"");

		if (lit.getDatatype() != null) {
			// Append the literal's datatype
			appendable.append("^^");
			NTriplesUtil.append(lit.getDatatype(), appendable);
		}
		else if (lit.getLanguage() != null) {
			// Append the literal's language
			appendable.append("@");
			appendable.append(lit.getLanguage());
		}
	}
}
