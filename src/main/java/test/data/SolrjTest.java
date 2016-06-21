package test.data;

import java.io.IOException;

import org.apache.solr.client.solrj.SolrServer;
import org.apache.solr.client.solrj.SolrServerException;
import org.apache.solr.client.solrj.impl.HttpSolrServer;
import org.apache.solr.client.solrj.response.UpdateResponse;
import org.apache.solr.common.SolrInputDocument;

public class SolrjTest {
	public static void main(String[] args) throws SolrServerException, IOException {
		SolrServer server = new HttpSolrServer("http://s00061:8983/solr/zbt/");
		SolrInputDocument doc = new SolrInputDocument();  
        doc.addField("UUID", "123");  
        doc.addField("tbszy", "nimeimei");  
        server.add(doc);
        //UpdateResponse rs = server.addBySolrInputDocument(doc);  
	}
}
