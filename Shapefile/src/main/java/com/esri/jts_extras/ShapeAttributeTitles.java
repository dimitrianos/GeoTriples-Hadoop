
package com.esri.jts_extras;


import java.util.ArrayList;
import java.util.List;

/**
 */
public class ShapeAttributeTitles {
    
    
    // a list that contains the titles of the shape file's attributes
    public static List<String> all_titles = new ArrayList<String>();

    public List<String> getAll_titles() {
        
        return all_titles;
        
    }
    
    public void set_titles(String field_name){
                              
        all_titles.add(field_name);          
               
    }      
   
    
}
