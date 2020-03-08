package elastic.mapper.importer;


import static org.elasticsearch.node.NodeBuilder.nodeBuilder;

import java.util.List;

import org.apache.commons.codec.digest.DigestUtils;
import org.elasticsearch.action.admin.indices.create.CreateIndexResponse;
import org.elasticsearch.action.bulk.BulkRequestBuilder;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.client.Client;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.logging.ESLogger;
import org.elasticsearch.common.logging.Loggers;


import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.InetSocketTransportAddress;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.node.Node;
import  org.elasticsearch.common.Base64;


public final class AttachmentTest {
	  Client client;
	  ESLogger log = Loggers.getLogger(AttachmentTest.class);

	  public void setupNode() {
		//client=new TransportClient().addTransportAddress(new InetSocketTransportAddress("localhost", 9300));
		  Node node = nodeBuilder().settings(Settings.settingsBuilder().put("path.home", "/Users/huazhang/elk-data"))
					.local(true).node();
	
	  }
	  
	  public static void main(String[] args) throws Exception {
		  
		  AttachmentTest test = new AttachmentTest();
		  test.setupNode();
		  test.mapperAttachmentTest();
	}

	  public void mapperAttachmentTest() throws Exception {
	    String idxName = "test";
	    String idxType = "attachment";
	    XContentBuilder map = XContentFactory.jsonBuilder().startObject()
	            .startObject(idxType)
	              .startObject("properties")
	                .startObject("file")
	                  .field("type", "attachment")
	                  .startObject("fields")
	                    .startObject("title")
	                     //.field("indexAnalyzer", "ik")  
	                     .field("searchAnalyzer", "ik") 
	                     .field("store", "yes")
	                    .endObject()
	                    .startObject("author")
	                     .field("searchAnalyzer", "ik") 
	                     .field("store", "yes")
	                    .endObject()
	                    .startObject("file")
	                      .field("term_vector","with_positions_offsets")
	                     // .field("indexAnalyzer", "ik")  
	                      .field("searchAnalyzer", "ik")
	                      .field("store","yes")
	                    .endObject()
	                  .endObject()
	                .endObject()
	              .endObject()
	         .endObject();
	    try {
	      client.admin().indices().prepareDelete(idxName).execute().actionGet();
	    } catch (Exception ex) {
	    	ex.printStackTrace();
	    }

	    log.info("create index and mapping");
	    
	    CreateIndexResponse resp = client.admin().indices().prepareCreate(idxName).setSettings(
	            Settings.settingsBuilder()
	            .put("number_of_shards", 1)
	            .put("index.numberOfReplicas", 0))
	            .addMapping("attachment", map).execute().actionGet();
	    
	    System.out.println(resp.isAcknowledged()); 
	    String path="/home/gd/pdf";
	    log.info("MD5: original file ");
	    log.info("Indexing");
	    BulkRequestBuilder bulkRequest = client.prepareBulk().setRefresh(true);
	    //文件所在位置url
	    List<String> filelist= FileUtil.listFileFullPath(path, "pdf", true);
	   
	    //编码为base64并进行批量建索引  
	    long start = System.currentTimeMillis();
	    int count=0;
	    log.info("MD5: encoded file ");
	    for(int i=0;i<filelist.size();i++){
	    String data64 =  "hellow world"; //org.elasticsearch.common.Base64.encodeFromFile(filelist.get(i));	
	    
	    XContentBuilder source = XContentFactory.jsonBuilder().startObject()
	            .field("file", data64).endObject();
	    bulkRequest.add(client.prepareIndex(idxName, idxType, i + "")
	            .setSource(source));   
	    count++;
	    if(count==2)
	    {
	    	BulkResponse bulkResponse = bulkRequest.execute().actionGet();
	    	count=0;
	    }
	    }
	    BulkResponse bulkResponse = bulkRequest.execute().actionGet();
		if (bulkResponse.hasFailures()) {
			System.out.print("导入索引失败！");
		}
		System.out.print("用时：");
		System.out.print(System.currentTimeMillis() - start);
		
}
	 
}