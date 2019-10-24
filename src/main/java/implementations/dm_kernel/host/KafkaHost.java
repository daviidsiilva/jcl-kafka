package implementations.dm_kernel.host;

import java.io.FileInputStream;
import java.io.IOException;
import java.util.Arrays;
import java.util.Locale;
import java.util.Properties;

import implementations.dm_kernel.ConnectorImpl;
import implementations.dm_kernel.MessageMetadataImpl;
import implementations.dm_kernel.host.MainHost;
import implementations.dm_kernel.server.MainServer;
import implementations.sm_kernel.PacuResource;
import implementations.util.ServerDiscovery;
import implementations.util.IoT.CryptographyUtils;
import interfaces.kernel.JCL_connector;
import interfaces.kernel.JCL_message_get_host;
import interfaces.kernel.JCL_message_metadata;

//Kafka imports
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.Materialized;
import org.apache.kafka.streams.kstream.Produced;
import org.apache.kafka.streams.state.KeyValueStore;

public class KafkaHost extends MainHost{

	public KafkaHost(int port, String BoardID) throws IOException {
		super(port, BoardID);
		// TODO Auto-generated constructor stub
	}
	
	private Properties readPropertiesFile() {
		Properties properties = new Properties();
		try {
		    properties.load(new FileInputStream("../jcl_conf/config.properties"));
		} catch (IOException e) {
			e.printStackTrace();
		}
		return properties;
	}

	@Override
	protected void beforeListening() {
		
		Properties properties = this.readPropertiesFile();
		
		serverAdd = properties.getProperty("serverMainAdd");
		serverPort = Integer.parseInt(properties.getProperty("superPeerMainPort"));
		System.out.println("1");
		Thread kafkaRegister = new Thread() {
			public void run() {
				System.out.println("2");
				Properties kafkaProperties = new Properties();
				System.out.println("3");
				kafkaProperties.put(StreamsConfig.APPLICATION_ID_CONFIG, "streams-jcl");
				kafkaProperties.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
				kafkaProperties.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass());
				kafkaProperties.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass());
				System.out.println("4");
				final StreamsBuilder builder = new StreamsBuilder();
				System.out.println("5");
				KStream<String, String> source = builder.stream("streams-jcl-input");
				
				System.out.println("6");
				
				source.flatMapValues(value -> Arrays.asList(value.toLowerCase(Locale.getDefault()).split("\\W+")))
					.groupBy((key, value) -> value)
	                .count(Materialized.<String, Long, KeyValueStore<Bytes, byte[]>>as("counts-store"))
	                .toStream()
	                .to("streams-wordcount-output", Produced.with(Serdes.String(), Serdes.Long()));
				
				System.out.println("7");
			}
		};
		kafkaRegister.start();
		
		Thread threadRegister = new Thread(){
		    public void run(){
		    	JCL_connector controlConnector = new ConnectorImpl(false);
		    	if(!controlConnector.connect(serverAdd, serverPort,null)){
		    		serverPort = Integer.parseInt(properties.getProperty("serverMainPort"));
		    		boolean connected = controlConnector.connect(serverAdd, serverPort,null);
		    		if (!connected){

		    			String serverData[] = ServerDiscovery.discoverServer();
		    			if (serverData != null){
		    				serverAdd = serverData[0];
		    				serverPort = Integer.parseInt(serverData[1]);
		    				connected = controlConnector.connect(serverAdd, serverPort, null);		    						    				

		    			} else if (BoardType < 4){
	    					System.out.println("Starting JCL-Server.");
	    						    					
	    					Thread thread = new Thread() {
	    				        public void run() {
	    				        	MainServer.main(null);
	    				        }
	    				    };
	    				    thread.start();
	    					
	    				    long startTime = System.currentTimeMillis(); //fetch starting time
		    				serverAdd = "127.0.0.1";
		    				while((!controlConnector.connect(serverAdd, serverPort, null)) && (System.currentTimeMillis()-startTime)<1000);		    				
		    			}		    			
		    		}
		    	}
		    	JCL_message_metadata msg = new MessageMetadataImpl();

		    	msg.setType(-1);				
				msg.setMetadados(metaData);
				
				boolean activateEncryption = false;
				if (ConnectorImpl.encryption){
					ConnectorImpl.encryption = false;
					activateEncryption = true;
				}
				
				JCL_message_get_host msgr = (JCL_message_get_host)controlConnector.sendReceiveG(msg,null);				
				
				if (activateEncryption)
					ConnectorImpl.encryption = true;
				
				
				
				if((msgr.getSlaves() != null)){	
					slaves.putAll(msgr.getSlaves());
					slavesIDs.addAll(msgr.getSlavesIDs());
					CryptographyUtils.setClusterPassword(msgr.getMAC());
					
					((PacuResource)rp).setHostIp(metaData);
					rp.wakeup();
					
					if (BoardType >= 4)
						configureBoard();
					
					System.out.println("HOST JCL/Kafka is OK");					 			
				}				
				else System.err.println("HOST JCL/Kafka NOT STARTED");
				
				ShutDownHook();
				controlConnector.disconnect();
		    }
		  };
		  threadRegister.start();
	}
	
}
