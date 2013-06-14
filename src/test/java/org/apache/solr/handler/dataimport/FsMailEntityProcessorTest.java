package org.apache.solr.handler.dataimport;

import java.util.HashMap;
import java.util.Map;

import org.apache.solr.handler.dataimport.DataConfig.Entity;
import org.junit.Test;

public class FsMailEntityProcessorTest {

  @Test
  public void test() {
    FsMailEntityProcessor p = new FsMailEntityProcessor();
    Entity entity = new Entity();
    entity.allAttributes = new HashMap<String, String>();
    entity.allAttributes.put("dataDir", System.getProperty("dataDir"));
    entity.allAttributes.put("ignoreFrom", System.getProperty("ignoreFrom"));
    Context ctx = new ContextImpl(entity, new VariableResolverImpl(), null, null, null, null, null);

    p.firstInit(ctx);
    p.init(ctx);
    Map<String, Object> row = null;
    while((row = p.nextRow()) != null) {
      System.out.println(row);
    }
    p.close();
  }

}
