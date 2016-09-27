
import net.antidot.semantic.rdf.model.impl.sesame.SesameDataSet;
import org.openrdf.model.Resource;
import org.openrdf.model.Statement;
import org.openrdf.model.URI;
import org.openrdf.model.Value;

/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */

/**
 *
 * @author admin
 */
class NullDataSet extends SesameDataSet {

    public NullDataSet() {
    }

    @Override
    public void add(Resource s, URI p, Value o, Resource... contexts) {
        return;
    }

    @Override
    public void addStatement(Statement s) {
        return;
    }
    
}
