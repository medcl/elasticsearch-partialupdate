package org.elasticsearch.plugin.infinitbyte;

import org.elasticsearch.plugins.AbstractPlugin;
import org.elasticsearch.rest.RestModule;


/**
 * Created by IntelliJ IDEA.
 * User: Medcl'
 * Date: 9/15/11
 * Time: 3:07 PM
 */
public class PartialUpdatePlugin extends AbstractPlugin {

    public String name() {
        return "DocumentPartialUpdate";
    }

    public String description() {
        return "a document partial update plugin for elasticsearch,allows you to update a document without full reindex.";
    }

    public void onModule(RestModule module){
        module.addRestAction(PartialUpdateRestAction.class);
    }
}
