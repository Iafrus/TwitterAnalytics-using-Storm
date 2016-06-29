/**
 * Taken from the storm-starter project on GitHub
 * https://github.com/nathanmarz/storm-starter/ 
 */
package twitter.esempio;

import backtype.storm.Config;
import backtype.storm.spout.SpoutOutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichSpout;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Values;
import backtype.storm.utils.Utils;
import twitter4j.*;
import twitter4j.conf.ConfigurationBuilder;

import java.util.Map;
import java.util.concurrent.LinkedBlockingQueue;

/*
 * Reads Twitter's sample feed using the twitter4j library.
 */
@SuppressWarnings({ "rawtypes", "serial" })
public class TwitterSampleSpout extends BaseRichSpout {

	private SpoutOutputCollector collector;
	private LinkedBlockingQueue<Status> queue;
	private TwitterStream twitterStream;

	@Override
	public void open(Map conf, TopologyContext context, SpoutOutputCollector collector) {
		this.collector = collector;
		queue = new LinkedBlockingQueue<Status>(1000); // BlockingQueue = una Queue che può aspettare che la coda si riempia per leggere o aspettare che si svuoti per scrivere.
													   // LinkedBlockingQueue = usa un protocollo FIFO, la head è l'elemento che ci sta da più tempo, gli elementi vengono aggiunti alla tail.
		StatusListener listener = new StatusListener() { // è uno StreamListener che si mette in ascolto dei tweet, status=tweet
			@Override
			public void onStatus(Status status) { // metodo che gestisce cosa fare quando si riceve uno stato
				queue.offer(status);  // offer aggiunge un elemento alla cosa se c'è spazio, se lo inserisce restituisce true
			}
			@Override
			public void onException(Exception arg0) {
			}
			@Override
			public void onDeletionNotice(StatusDeletionNotice arg0) {
			}
			@Override
			public void onScrubGeo(long arg0, long arg1) {
			}
			@Override
			public void onStallWarning(StallWarning arg0) {
			}
			@Override
			public void onTrackLimitationNotice(int arg0) {
			}
		};
		
		ConfigurationBuilder cb = new ConfigurationBuilder(); // costruttore della configurazione della TwitterStreamFactory, perché Configuration è un interface
		cb.setDebugEnabled(true)
		  .setOAuthConsumerKey("ZobdHlpcDyCS71hMi0h81w9tl")
		  .setOAuthConsumerSecret("IuLIn8tmvEuGLueJJ9dxsSYzDtf8xmmNyGTHZKavfK7zcqXi27")
		  .setOAuthAccessToken("747770669690142720-wn1WQB96H2qe74HvPuzfjVVauRxvPf0")
		  .setOAuthAccessTokenSecret("z0tZgtMXikV4t7UbutObpbLGQtiAyLsU6adubdaESCDeN");
		
		TwitterStreamFactory factory = new TwitterStreamFactory(cb.build()); // una factory è un generatore/istanziatore di classi
		twitterStream = factory.getInstance(); // l'oggetto che gestisce lo stream dei tweet
		twitterStream.addListener(listener);
		twitterStream.sample(); // inizia ad ascoltare un campione casuale di status pubblici
	}

	@Override
	public void nextTuple() { // codice ripetuto in un loop infinito
		Status s = queue.poll(); // prende e rimuove la head della coda, se è vuota da null
		if (s == null) {
			Utils.sleep(50);
		} else {
			collector.emit(new Values(s)); // manda lo status alla topology
		}
	}

	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) { // dichiara il tipo di output del componente (spout o bolt)
		declarer.declare(new Fields("tweet"));
	}
	
	@Override
	public void close() { // metodo chiamato quando si chiude il componente
		twitterStream.shutdown(); // chiude lo stream
	}

	@Override
	public Map<String, Object> getComponentConfiguration() {
		Config conf = new Config(); // legge la configurazione del componente attuale
		conf.setMaxTaskParallelism(1);
		return conf; // sovrascrive la configurazione della topology per questo componente
	}

//	@Override
//	public void ack(Object id) {
//	}
//
//	@Override
//	public void fail(Object id) {
//	}

}
