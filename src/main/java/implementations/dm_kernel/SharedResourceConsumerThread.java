package implementations.dm_kernel;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.channels.SelectionKey;
import java.nio.channels.Selector;
import java.nio.channels.ServerSocketChannel;
import java.time.Duration;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.StringDeserializer;

public class SharedResourceConsumerThread extends Thread{
	
	protected final int port;	
	protected final Selector selector;
	protected final ServerSocketChannel serverSocket;
	
	/** 3.0 begin **/
	private static final String ACQUIRED = "-1";
	private static final String RELEASED = "-2";
	
	public Map<String, String> localMemory;
	public Consumer<String, String> kafkaConsumer;
	public Boolean fromBeggining = false;
	public Long offset = 0L;
	/** 3.0 end **/
	
	public SharedResourceConsumerThread(
		int port, 
		Map<String, String> localMemory
	) throws IOException {
		this.port = 4952;
		this.selector = Selector.open();
		this.serverSocket = ServerSocketChannel.open();
		
		/** 3.0 begin **/
		this.localMemory = localMemory;
		/** 3.0 end **/
	}
	
	public SharedResourceConsumerThread(
		int port, 
		Map<String, String> localMemory,
		Long offset
	) throws IOException {
		this.port = 4952;
		this.selector = Selector.open();
		this.serverSocket = ServerSocketChannel.open();
		
		/** 3.0 begin **/
		this.localMemory = localMemory;
		this.offset = offset;
		/** 3.0 end **/
	}
	
	public SharedResourceConsumerThread(
		int port, 
		Map<String, String> localMemory,
		String userID
	) throws IOException {
		this.port = 4952;
		this.selector = Selector.open();
		this.serverSocket = ServerSocketChannel.open();
		
		/** 3.0 begin **/
		this.localMemory = localMemory;
		/** 3.0 end **/
	}
	
	public SharedResourceConsumerThread(
		int port, 
		Map<String, String> localMemory, 
		Boolean fromBeggining
	) throws IOException {
		this.port = 4952;
		this.selector = Selector.open();
		this.serverSocket = ServerSocketChannel.open();
		
		/** 3.0 begin **/
		this.localMemory = localMemory;
		this.fromBeggining = fromBeggining;
		/** 3.0 end **/
	}
	
	public void end(){
		try {
			selector.close();
		} catch (IOException e) {
			e.printStackTrace();
		}
	}
				
	public void run(){
//		openServerSocket();

		/** 3.0 begin **/
		Properties consumerProperties = new Properties();
		consumerProperties.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, 
				"localhost:9092");
		consumerProperties.put(
			ConsumerConfig.CLIENT_ID_CONFIG, 
			"jcl-client");
		consumerProperties.put(
			ConsumerConfig.GROUP_ID_CONFIG, 
			"jcl-consumer-group");
		consumerProperties.put(
			ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, 
			StringDeserializer.class.getName());
		consumerProperties.put(
			ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, 
			StringDeserializer.class.getName());
		
		this.kafkaConsumer = new KafkaConsumer<>(consumerProperties);
		
		try {
			this.kafkaConsumer.subscribe(
				Arrays.asList("jcl-output"));
		
			ConsumerRecords<String, String> consumedRecords = this.kafkaConsumer
				.poll(Duration.ofNanos(Long.MAX_VALUE));
			
			if(this.offset != null) {
				this.kafkaConsumer.seek(
					new TopicPartition("jcl-output", 0), 
					this.offset
				);
			}
		
        	synchronized(this.localMemory) {
        		consumedRecords.forEach(
    				(record) -> {
    					this.offset = record.offset();
    					
    					if(record.key().charAt(0) == '$') {
    						if(Long.parseLong(record.value()) == Long.parseLong(ACQUIRED)) {
    							this.localMemory.put(
    								record.key(), 
    								record.value()
    							);
    							
    						} else if(Long.parseLong(record.value()) == Long.parseLong(RELEASED)) {
    							this.localMemory.remove(
									record.key()
								);
    							
    						} else {
    							this.localMemory.put(
									record.key(), 
									record.offset() + ""
								);
    						}
    					} else {
    						this.localMemory.put(
								record.key(), 
								record.value()
							);
    					}
  						
    					Map<String, Object> recordOutput = new HashMap<>();
    					
    					recordOutput.put("partition", record.partition());
    					recordOutput.put("offset", record.offset());
    					recordOutput.put("value", record.value());
						recordOutput.put("key", record.key());
						
						System.out.println(recordOutput.toString());
    				}
				);
        	}      	        
		} catch (Exception e) {
			
			e.printStackTrace();
       } finally {
    	   
    	   this.kafkaConsumer.close();
    	   /** 3.0 end **/
    	   try {
				selector.close();
				serverSocket.socket().close();
				serverSocket.close();
			} catch (Exception e) {
				System.err.println(this.getClass().getName() + " failed!");
			}
        }
	}

    private void openServerSocket() {
        try {
        	this.serverSocket.configureBlocking(false);
            
        	//set some options
            this.serverSocket.socket().setReuseAddress(true);
            this.serverSocket.socket().bind(new InetSocketAddress(this.port));                                  
            this.serverSocket.register(this.selector,SelectionKey.OP_ACCEPT);

            
        } catch (IOException e) {
            throw new RuntimeException("Cannot open port " + this.port, e);
        }
    }
}
