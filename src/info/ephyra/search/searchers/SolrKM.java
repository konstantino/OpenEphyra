package info.ephyra.search.searchers;

import info.ephyra.io.MsgPrinter;
import info.ephyra.querygeneration.Query;
import info.ephyra.search.Result;

import org.apache.solr.client.solrj.SolrClient;
import org.apache.solr.client.solrj.SolrQuery;
import org.apache.solr.client.solrj.impl.HttpSolrClient;
import org.apache.solr.client.solrj.response.QueryResponse;
import org.apache.solr.common.SolrDocumentList;

import java.util.ArrayList;
import java.util.Vector;

/**
 * <p>A <code>KnowledgeMiner</code> that deploys a connection to a Solr backend.</p>
 * 
 * <p>It runs as a separate thread, so several queries can be performed in
 * parallel.</p>
 * 
 * <p>This class extends the class <code>KnowledgeMiner</code>.</p>
 * 
 * @author Konstantinos Vandikas
 * @version 2017-02-22
 */
public class SolrKM extends KnowledgeMiner {

	/** Maximum total number of search results. */
	private static final int MAX_RESULTS_TOTAL = 100;
	/** Maximum number of search results per query. */
	private static final int MAX_RESULTS_PERQUERY = 10;
	/** Number of retries if search fails. */
	private static final int RETRIES = 50;
	
	/**
	 * Returns the maximum total number of search results.
	 * 
	 * @return maximum total number of search results
	 */
	protected int getMaxResultsTotal() {
		return MAX_RESULTS_TOTAL;
	}
	
	/**
	 * Returns the maximum number of search results per query.
	 * 
	 * @return maximum total number of search results
	 */
	protected int getMaxResultsPerQuery() {
		return MAX_RESULTS_PERQUERY;
	}

	private SolrClient getSolrClient() {
		String urlString = System.getProperty("solrEndpoint");
		return new HttpSolrClient.Builder(urlString).build();
	}

	/**
	 * Queries the Solr backend and returns an array containing up to
	 * <code>MAX_RESULTS_PERQUERY</code> search results.
	 * 
	 * @return Solr search results
	 */
	protected Result[] doSearch() {
		SolrClient client = getSolrClient();
		
		SolrQuery solrQuery = new SolrQuery().setRows(MAX_RESULTS_PERQUERY);
		solrQuery.setFields( "content", "url" );
		String queryString = "content:" + query.getQueryString();

		solrQuery.setQuery(queryString);

		// perform search
		QueryResponse solrResponse = null;
		int retries = 0;
		while (solrResponse == null)
			try {
				solrResponse = client.query(solrQuery);
			} catch (Exception e) {
				MsgPrinter.printSearchError(e);  // print search error message
				
				if (retries == RETRIES) {
					MsgPrinter.printErrorMsg("\nSearch failed.");
					System.exit(1);
				}
				retries++;
				
				try {
					SolrKM.sleep(1000);
				} catch (InterruptedException ie) {}
			}

		// get snippets and URLs of the corresponding websites
		SolrDocumentList list = solrResponse.getResults();

		ArrayList<String> snippets = new ArrayList<String>();
		ArrayList<String> urls = new ArrayList<String>();

		for (int i = 0; i < list.size(); i++) {
			//MsgPrinter.printStatusMsg(name);
			Object content = list.get(i).get("content");
			if ( content != null && content.toString().length() > 0 ) {
				snippets.add(content.toString());
				urls.add(list.get(i).get("url").toString());
			}
		}

		// return results
		return getResults(snippets.toArray(new String[snippets.size()]), urls.toArray(new String[urls.size()]), true);
	}
	
	/**
	 * Returns a new instance of <code>SolrKM</code>. A new instance is
	 * created for each query.
	 * 
	 * @return new instance of <code>SolrKM</code>
	 */
	public KnowledgeMiner getCopy() {
		return new SolrKM();
	}

	// TODO: Should be replaced by a more proper test
	// simple test
	// for a commandline tutorial visit: http://yonik.com/solr-tutorial/
	//public static void main(String[] args) {
	//	SolrKM km = new SolrKM();
	//	km.query = new Query("something");
	//	km.doSearch();
	//}
}
