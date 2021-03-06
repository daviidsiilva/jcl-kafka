package implementations.dm_kernel.user;
import implementations.collections.JCLFuture;
import implementations.collections.JCLHashMap;
import implementations.collections.JCLPFuture;
import implementations.collections.JCLSFuture;
import implementations.collections.JCLVFuture;
import implementations.dm_kernel.ConnectorImpl;
import implementations.dm_kernel.JCLTopicAdmin;
import implementations.dm_kernel.MessageGenericImpl;
import implementations.dm_kernel.MessageListGlobalVarImpl;
import implementations.dm_kernel.MessageListTaskImpl;
import implementations.dm_kernel.MessageMetadataImpl;
import implementations.dm_kernel.MessageRegisterImpl;
import implementations.dm_kernel.SimpleServer;
import implementations.dm_kernel.server.RoundRobin;
import implementations.util.ObjectWrap;
import implementations.util.ServerDiscovery;
import implementations.util.XORShiftRandom;
import implementations.util.IoT.CryptographyUtils;
import interfaces.kernel.JCL_connector;
import interfaces.kernel.JCL_facade;
import interfaces.kernel.JCL_message;
import interfaces.kernel.JCL_message_bool;
import interfaces.kernel.JCL_message_generic;
import interfaces.kernel.JCL_message_list_global_var;
import interfaces.kernel.JCL_message_list_task;
import interfaces.kernel.JCL_message_metadata;
import interfaces.kernel.JCL_message_register;
import interfaces.kernel.JCL_result;
import interfaces.kernel.JCL_task;
import interfaces.kernel.datatype.Device;
import io.protostuff.LinkedBuffer;
import io.protostuff.ProtobufIOUtil;
import io.protostuff.Schema;
import io.protostuff.runtime.RuntimeSchema;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.net.InetAddress;
import java.net.UnknownHostException;

import implementations.util.ByteBuffer;
import implementations.util.KafkaConfigProperties;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Properties;
import java.util.Queue;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;


import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;

import javassist.ClassPool;
import javassist.CtClass;
import commom.JCLResultResource;
import commom.JCLResultSerializer;
import commom.JCL_resultImpl;
import commom.JCL_taskImpl;
import commom.Constants;
import commom.KafkaConsumerThread;


public class JCL_FacadeImpl extends implementations.sm_kernel.JCL_FacadeImpl.Holder implements JCL_facade{

	private final ScheduledExecutorService scheduler = Executors.newSingleThreadScheduledExecutor();
	private Map<Integer,Map<String,Map<String,String>>> devices;
	//	private Map<String, Map<String, String>> devicesExec;
	private static List<Entry<String, Map<String, String>>> devicesStorage,devicesExec;
	private JCL_message_list_task msgTask = new MessageListTaskImpl();
	private static ReadWriteLock lock = new ReentrantReadWriteLock();
	private Set<String>  registerClass = new HashSet<String>();
	private static ConcurrentMap<String, JCL_message_register> jars;
	private static ConcurrentMap<String,List<String>> jarsSlaves;
	//	private static ConcurrentMap<String,String[]> slaves;	
	private boolean watchExecMeth = true;	
	private static JCL_facade instance;
	private SimpleServer simpleSever;
	//	private static List<String> slavesIDs;
	private static XORShiftRandom rand;
	private boolean JPF = true;
	public static String serverAdd;	
	private int watchdog = 0;
	private int JPBsize = 50;
	private static JCL_facade jcl;
	public static int serverPort,serverSPort;
	private static int delta;
	private int port;
	
	/** 3.0 begin **/
	private Producer<String, JCL_result> kafkaProducer;
	
	private static JCLResultResource localResourceGlobalVar;
	private static JCLResultResource localResourceExecute;
	private static List<String> subscribedTopics;
	private static KafkaConsumerThread kct;
	private static JCLTopicAdmin jclTopicAdmin;
	
	public static String topicGranularity;
	private String userIPAddress;
	/** 3.0 end **/
	
	protected JCL_FacadeImpl(Properties properties){
		//Start seed rand GV
		rand = new XORShiftRandom();
		
		initKafka();
		
		try {
			//single pattern
			if (instance == null){
				instance = this;
			}

			//ini variables
			JPF = Boolean.valueOf(properties.getProperty("enablePBA"));
			JPBsize =  Integer.parseInt(properties.getProperty("PBAsize"));
			delta =  Integer.parseInt(properties.getProperty("delta"));
			boolean DA = Boolean.valueOf(properties.getProperty("enableDinamicUp"));
			serverAdd = properties.getProperty("serverMainAdd");
			serverPort = Integer.parseInt(properties.getProperty("serverMainPort"));
			serverSPort = Integer.parseInt(properties.getProperty("superPeerMainPort"));
			int timeOut = Integer.parseInt(properties.getProperty("timeOut"));
			this.port = Integer.parseInt(properties.getProperty("simpleServerPort"));
			jars = new ConcurrentHashMap<String, JCL_message_register>();			
			jarsSlaves = new ConcurrentHashMap<String,List<String>>();			
			
			jcl = super.getInstance();

			//config connection			
			ConnectorImpl.timeout = timeOut;			

			JCL_connector controlConnector = new ConnectorImpl(false);

			if (controlConnector.connect(serverAdd, serverPort, null)){
				controlConnector.disconnect();
			}else{
				String serverData[] = ServerDiscovery.discoverServer();
				if (serverData != null){
					serverAdd = serverData[0];
					serverPort = Integer.parseInt(serverData[1]);
					controlConnector.connect(serverAdd, serverPort, null);
				}
			}

			//ini jcl lambari 
			jcl.register(JCL_FacadeImplLamb.class, "JCL_FacadeImplLamb");

			// scheduler flush in execute
			if(JPF){
				scheduler.scheduleAtFixedRate(
						new Runnable() {
							public void run(){
								try {
									//watchdog end bin exec  
									if((watchdog != 0) && (watchdog == msgTask.taskSize()) && (watchExecMeth)){
										//Get host
										//Init RoundRobin
										Map<String, String> hostPort =RoundRobin.getDevice();
										String host = hostPort.get("IP");
										String port = hostPort.get("PORT");
										String mac = hostPort.get("MAC");
										String portS = hostPort.get("PORT_SUPER_PEER");

										//Register missing class 
										for(String classReg:registerClass){
											if(!jarsSlaves.get(classReg).contains(host+port+mac+portS)){
												Object[] argsLam = {host,port,mac,portS,jars.get(classReg)};
												Future<JCL_result> ti =jcl.execute("JCL_FacadeImplLamb", "register", argsLam);									
												ti.get();
												//									jcl.getResultBlocking(ti);
												jarsSlaves.get(classReg).add(host+port+mac+portS);
											}
										}

										//Send to host task bin
										Object[] argsLam = {host,port,mac,portS,msgTask};
										jcl.execute("JCL_FacadeImplLamb", "binexecutetask", argsLam);
										msgTask = new MessageListTaskImpl();
									}else{
										//update watchdog
										watchdog = msgTask.taskSize();
									}

								} catch (Exception e) {
									// TODO Auto-generated catch block
									System.err.println("JCL facade watchdog error");
									e.printStackTrace();
								}
							}

						},0,5, TimeUnit.SECONDS);
			}

			//getHosts using lambari
			int type = 5;


			//Get devices thar compose the cluster

			this.update();						

			RoundRobin.ini(devicesExec);

			//finish
			System.out.println("client JCL is OK");


		} catch (Exception e) {
			System.err.println("JCL facade constructor error");
			e.printStackTrace();			
		}
	}

	private void initKafka() {
		Properties kafkaProperties = KafkaConfigProperties.getInstance().get();
		
		topicGranularity = kafkaProperties.getProperty(Constants.Environment.GRANULARITY_CONFIG_KEY);
		
		if(topicGranularity == Constants.Environment.HIGH_GRANULARITY_CONFIG_VALUE) {
			this.initKafkaHighGranularity();
		} else {
			this.initKafkaLowGranularity();
		}
	}
	
	private void initKafkaHighGranularity() {
		kafkaProducer = new KafkaProducer<>(
			KafkaConfigProperties.getInstance().get(),
			new StringSerializer(),
			new JCLResultSerializer()
		);
		
		jclTopicAdmin = JCLTopicAdmin.getInstance();
		
		String defaultTopic = "jclDefault";
		subscribedTopics = new CopyOnWriteArrayList<String>();
		subscribedTopics.add(defaultTopic);
		
		localResourceGlobalVar = new JCLResultResource();
		localResourceExecute = new JCLResultResource();
		
		kct = new KafkaConsumerThread(
			subscribedTopics,
			localResourceGlobalVar, 
			localResourceExecute
		);
		
		try {
			kct.start();
			
			synchronized (kct) {
				kct.wait();
			}
		} catch (InterruptedException e) {
			System.err
				.println("Problem in void initKafka()");
			e.printStackTrace();
		}
	}
	
	private void initKafkaLowGranularity() {
		
		try {
			this.userIPAddress = InetAddress.getLocalHost().getHostAddress();
		} catch (UnknownHostException e) {
			System.err
				.println("Problem in void initKafka() when getHostAddress");
			e.printStackTrace();
		}
		
		kafkaProducer = new KafkaProducer<>(
			KafkaConfigProperties.getInstance().get(),
			new StringSerializer(),
			new JCLResultSerializer()
		);
		
		jclTopicAdmin = JCLTopicAdmin.getInstance();
		
		subscribedTopics = new CopyOnWriteArrayList<String>();
		subscribedTopics.add(this.userIPAddress.toString());
		
		localResourceGlobalVar = new JCLResultResource();
		localResourceExecute = new JCLResultResource();
		
		kct = new KafkaConsumerThread(
			subscribedTopics,
			localResourceGlobalVar, 
			localResourceExecute
		);
		
		try {
			kct.start();
			
			synchronized (kct) {
				kct.wait();
			}
		} catch (InterruptedException e) {
			System.err
				.println("Problem in void initKafka()");
			e.printStackTrace();
		}
	}
	
	public void update(){
		try{
			//			Object[] argsLam = {};
			//			Future<JCL_result> t = jcl.execute("JCL_FacadeImplLamb", "getSlaveIds", argsLam);
			JCL_message_generic mgh = (JCL_message_generic)this.getSlaveIds(serverAdd, serverPort,serverSPort,3);

			Object obj[] = (Object[]) mgh.getRegisterData();
			devices = (Map<Integer, Map<String, Map<String, String>>>) obj[0];

			//Init RoundRobin
			devicesExec = new ArrayList<Entry<String, Map<String, String>>>();
			devicesStorage = new ArrayList<Entry<String, Map<String, String>>>();

			devicesExec.addAll(devices.get(2).entrySet());			
			devicesExec.addAll(devices.get(3).entrySet());
			devicesExec.addAll(devices.get(6).entrySet());
			devicesExec.addAll(devices.get(7).entrySet());

			devicesStorage.addAll(devices.get(1).entrySet());			
			devicesStorage.addAll(devices.get(3).entrySet());
			devicesStorage.addAll(devices.get(5).entrySet());
			devicesStorage.addAll(devices.get(7).entrySet());



			// Sorting			
			Comparator com = new Comparator<Entry<String, Map<String, String>>>() {
				@Override
				public int compare(Entry<String, Map<String, String>> entry2, Entry<String, Map<String, String>> entry1)
				{
					return  entry1.getKey().compareTo(entry2.getKey());
				}
			};

			Collections.sort(devicesExec, com);			
			Collections.sort(devicesStorage, com);
		} catch (Exception e){
			e.printStackTrace();
		}
	}

	//Get a list of hosts
	public JCL_message getSlaveIds(String serverAdd,int serverPort,int serverSPort, int deviceType){

		try {
			//Get a list of hosts
			//this.port = port;
			JCL_message_generic mc = new MessageGenericImpl();
			mc.setType(42);
			mc.setRegisterData(deviceType);

			boolean activateEncryption = false;
			if (ConnectorImpl.encryption){
				activateEncryption = true;
				ConnectorImpl.encryption = false;
			}
			JCL_connector controlConnector = new ConnectorImpl(false);
			if(!controlConnector.connect(serverAdd, serverPort,null)){
				this.serverPort = this.serverSPort;
				controlConnector.connect(serverAdd, serverSPort,null);
			}

			JCL_message mr = controlConnector.sendReceiveG(mc,null);
			JCL_message_generic mg = (MessageGenericImpl)  mr;
			Object obj[] = (Object[])  mg.getRegisterData();
			CryptographyUtils.setClusterPassword(obj[1]+"");
			controlConnector.disconnect();
			if( activateEncryption )
				ConnectorImpl.encryption = true;
			return mr;

		} catch (Exception e) {
			System.err.println("problem in JCL facade getSlaveIds()");
			e.printStackTrace();
			return null;
		}
	}

	//Register a file of jars
	@Override
	public boolean register(File[] f, String classToBeExecuted) {
		try {

			// Local register
			JCL_message_register msg = new MessageRegisterImpl();
			msg.setJars(f);
			msg.setJarsNames(f);
			msg.setClassName(classToBeExecuted);
			msg.setType(1);


			Object[] argsLam = {serverAdd, String.valueOf(serverPort),null,"0",msg};
			Future<JCL_result> t = jcl.execute("JCL_FacadeImplLamb", "register", argsLam);

			if(((Boolean)t.get().getCorrectResult()).booleanValue()){
				jars.put(classToBeExecuted, msg);
				jarsSlaves.put(classToBeExecuted, new ArrayList<String>());	

				return true;				

			} else{

				return false;
			}

		} catch (Exception e) {

			System.err
			.println("problem in JCL facade register(File f, String classToBeExecuted)");
			e.printStackTrace();
			return false;
		}
	}

	//Register just class
	@Override
	public boolean register(Class<?> serviceClass,
			String classToBeExecuted) {
		// TODO Auto-generated method stub		
		try {

			// Local register
			ClassPool pool = ClassPool.getDefault();
			CtClass cc = pool.get(serviceClass.getName());
			JCL_message_register msg = new MessageRegisterImpl();
			byte[][] cb = new byte[1][];
			cb[0] = cc.toBytecode();
			msg.setJars(cb);
			msg.setJarsNames(new String[]{cc.getName()});
			msg.setClassName(classToBeExecuted);
			msg.setType(3);

			Object[] argsLam = {serverAdd, String.valueOf(serverPort),null,"0",msg};
			Future<JCL_result> t = jcl.execute("JCL_FacadeImplLamb", "register", argsLam);
			
			if(((Boolean) t.get().getCorrectResult()).booleanValue()){
				jars.put(classToBeExecuted, msg);
				jarsSlaves.put(classToBeExecuted, new ArrayList<String>());	

				return true;				

			} else{

				return false;
			}

		} catch (Exception e){

			System.err
			.println("problem in JCL facade register(Class<?> serviceClass,String classToBeExecuted)");
			e.printStackTrace();
			return false;
		}
	}

	//unRegister a class
	@Override
	public boolean unRegister(String nickName) {				
		boolean ok = true;
		try {
			//List host
			for(Entry<String, Map<String, String>> oneHostPort: devicesExec){

				if (jarsSlaves.get(nickName).contains(oneHostPort.getValue().get("IP")+oneHostPort.getValue().get("PORT")+oneHostPort.getValue().get("MAC")+oneHostPort.getValue().get("PORT_SUPER_PEER"))){
					// UnRegister using lambari on host
					Object[] argsLam = {nickName,oneHostPort.getValue().get("IP"),oneHostPort.getValue().get("PORT"),oneHostPort.getValue().get("MAC")};
					Future<JCL_result> t = jcl.execute("JCL_FacadeImplLamb", "unRegister", argsLam);					
					if ((t.get()).getCorrectResult() != null){
						jarsSlaves.get(nickName).remove(oneHostPort.getValue().get("IP")+oneHostPort.getValue().get("PORT")+oneHostPort.getValue().get("MAC")+oneHostPort.getValue().get("PORT_SUPER_PEER"));
					}
					else{
						ok = false;
					}				
				}
			}
			//remove class
			jars.remove(nickName);
			jarsSlaves.remove(nickName);			
			return ok;

		} catch (Exception e) {
			System.err.println("JCL problem in unRegister(String nickName) method");
			e.printStackTrace();

			return false;
		}
	}

	@Override
	public Future<JCL_result> execute(String objectNickname,Object... args) {

		try {
			if (!JPF){
				//Get host
				String host = null,port = null,mac = null,portS=null;
				
				if (jars.containsKey(objectNickname)){
					// Get host
					Map<String, String> hostPort = RoundRobin.getDevice();

					host = hostPort.get("IP");
					port = hostPort.get("PORT");
					mac = hostPort.get("MAC");
					portS = hostPort.get("PORT_SUPER_PEER");
					
				} else {
					Object[] argsLam = {
						serverAdd, 
						String.valueOf(serverPort),
						null,
						null,
						objectNickname
					};
					Future<JCL_result> ticket = jcl.execute("JCL_FacadeImplLamb", "registerByServer", argsLam);
					
					Map<String, String> hostPort = (Map<String, String>) ticket.get().getCorrectResult();

					if(hostPort.size()==0){
						System.err.println("No class Found!!!");
					}

					host = hostPort.get("IP");
					port = hostPort.get("PORT");
					mac = hostPort.get("MAC");
					portS = hostPort.get("PORT_SUPER_PEER");

					List<String> js = new ArrayList<String>();
					js.add(host+port+mac+portS);
					jarsSlaves.put(objectNickname,js);
				}	    		  	    			    		  

				//Test if host contain jar
				if(jarsSlaves.get(objectNickname).contains(host+port+mac+portS)){
					//Just exec					
					Object[] argsLam = {objectNickname,host,port,mac,portS,new Boolean(true),args};
					Future<JCL_result> ticket = super.execute("JCL_FacadeImplLamb", "execute", argsLam);
					
					return ticket;
				} else{
					//Exec and register
					Object[] argsLam = {
						objectNickname,
						host,
						port,
						mac,
						portS,
						jars.get(objectNickname),
						new Boolean(true),
						args
					};
					Future<JCL_result> ticket = super.execute("JCL_FacadeImplLamb", "executeAndRegister", argsLam);
					
					jarsSlaves.get(objectNickname).add(host+port+mac+portS);
					
					return ticket;								
				}
			} else{
				//watch this method
				watchExecMeth = false;

				//Create bin task message
				JCL_task t = new JCL_taskImpl(null, objectNickname, args);
				Long ticket = super.createTicketH();
				t.setPort(this.port);
				msgTask.addTask(ticket,t);			
				registerClass.add(objectNickname);

				//Send bin task
				if (this.msgTask.taskSize() == (JPBsize*RoundRobin.core)){
					Map<String, String> hostPort =RoundRobin.getDevice();
					String host = hostPort.get("IP");
					String port = hostPort.get("PORT");
					String mac = hostPort.get("MAC");
					String portS = hostPort.get("PORT_SUPER_PEER");

					//Register bin task class
					for(String classReg:registerClass){
						if(!jarsSlaves.get(classReg).contains(host+port+mac+portS)){
							Object[] argsLam = {host,port,mac,portS,jars.get(classReg)};
							Future<JCL_result> ti =jcl.execute("JCL_FacadeImplLamb", "register", argsLam);
							ti.get();
							jarsSlaves.get(classReg).add(host+port+mac+portS);
						}
					}

					//execute lambari
					Object[] argsLam = {host,port,mac,portS,this.msgTask};
					jcl.execute("JCL_FacadeImplLamb", "binexecutetask", argsLam);
					msgTask = new MessageListTaskImpl();
				}				

				//watch this method
				watchExecMeth = true;
				
				return new JCLFuture<JCL_result>(ticket);
			}
		} catch (Exception e) {
			System.err
			.println("JCL facade Pacu problem in execute(String className, Object... args)");
			e.printStackTrace();
			return null;
		}
	}	

	@Override
	public Future<JCL_result> execute(String objectNickname, String methodName,
			Object... args) {
		try {
			if (!JPF){
				//Get host
				String host = null,port = null,mac = null, portS = null;


				if (jars.containsKey(objectNickname)){
					// Get host			

					Map<String, String> hostPort = RoundRobin.getDevice();

					host = hostPort.get("IP");
					port = hostPort.get("PORT");
					mac = hostPort.get("MAC");
					portS = hostPort.get("PORT_SUPER_PEER");

				}else{

					Object[] argsLam = {serverAdd, String.valueOf(serverPort),null,null,objectNickname};
					Future<JCL_result> ticket = jcl.execute("JCL_FacadeImplLamb", "registerByServer", argsLam);

					Map<String, String> hostPort = (Map<String, String>) ticket.get().getCorrectResult();

					if(hostPort.size()==0){
						System.err.println("No class Found!!!");
					}

					host = hostPort.get("IP");
					port = hostPort.get("PORT");
					mac = hostPort.get("MAC");
					portS = hostPort.get("PORT_SUPER_PEER");

					List<String> js = new ArrayList<String>();
					js.add(host+port+mac+portS);
					jarsSlaves.put(objectNickname,js);

				}

				//Test if host contain jar
				if(jarsSlaves.get(objectNickname).contains(host+port+mac+portS)){
					// Just exec
					Object[] argsLam = {objectNickname,methodName,host,port,mac,portS,new Boolean(true),args};
					Future<JCL_result> ticket = super.execute("JCL_FacadeImplLamb", "execute", argsLam);
					
					return ticket;
				} else{
					//Exec and register
					Object[] argsLam = {objectNickname,methodName,host,port,mac,portS,jars.get(objectNickname),new Boolean(true),args};					
					Future<JCL_result> ticket = super.execute("JCL_FacadeImplLamb", "executeAndRegister", argsLam);
					//	ticket.get();
					jarsSlaves.get(objectNickname).add(host+port+mac+portS);
					return ticket;								
				}
			} else{
				//watch this method
				watchExecMeth = false;

				//Create bin task message
				JCL_task t = new JCL_taskImpl(null, objectNickname, methodName, args);
				Long ticket = super.createTicketH();
				t.setPort(this.port);
				msgTask.addTask(ticket,t);				
				registerClass.add(objectNickname);

				//Send bin task
				if (this.msgTask.taskSize() == (JPBsize*RoundRobin.core)){
					Map<String, String> hostPort =RoundRobin.getDevice();
					String host = hostPort.get("IP");
					String port = hostPort.get("PORT");
					String mac = hostPort.get("MAC");
					String portS = hostPort.get("PORT_SUPER_PEER");

					//Register bin task class
					for(String classReg:registerClass){
						if(!jarsSlaves.get(classReg).contains(host+port+mac+portS)){
							Object[] argsLam = {host,port,mac,portS,jars.get(classReg)};
							Future<JCL_result> ti =jcl.execute("JCL_FacadeImplLamb", "register", argsLam);
							ti.get();
							jarsSlaves.get(classReg).add(host+port+mac+portS);
						}
					}

					//execute lambari		
					Object[] argsLam = {host,port,mac,portS,this.msgTask};
					jcl.execute("JCL_FacadeImplLamb", "binexecutetask", argsLam);
					msgTask = new MessageListTaskImpl();
				}				
				//watch this method
				watchExecMeth = true;

				return new JCLFuture<JCL_result>(ticket);
			}

		} catch (Exception e) {
			System.err
			.println("JCL facade problem in execute(String className, String methodName, Object... args)");

			return null;
		}
	}

	@Override
	public List<Future<JCL_result>> executeAll(String objectNickname, Object... args) {
		List<Entry<String, String>> hosts;
		List<Future<JCL_result>> tickets;
		tickets = new ArrayList<Future<JCL_result>>();
		
		try {

			//get all host
			int[] d = {2,3,6,7};
			hosts = this.getDevices(d);

			//Exec in all host
			for (Entry<String, String> host:hosts) {
				tickets.add(this.executeOnDevice(host, objectNickname,args));
			}

			return tickets;
		} catch (Exception e) {
			System.err
			.println("JCL facade problem in executeAll(String className, Object... args)");
			return null;
		}
	}

	@Override
	public List<Future<JCL_result>> executeAll(String objectNickname, String methodName,
			Object... args) {
		List<Entry<String, String>> hosts;
		List<Future<JCL_result>> tickets;
		tickets = new ArrayList<Future<JCL_result>>();
		try {

			//get all host
			int[] d = {2,3,6,7};
			hosts = this.getDevices(d);

			//Exec in all host
			for (Entry<String, String> host:hosts) {
				tickets.add(this.executeOnDevice(host, objectNickname,methodName,args));
			}

			return tickets;

		} catch (Exception e) {
			System.err
			.println("JCL facade problem in executeAll(String objectNickname, String methodName, Object... args)");
			return null;
		}
	}

	@Override
	public List<Future<JCL_result>> executeAll(String objectNickname, Object[][] args) {
		List<Entry<String, String>> hosts;
		List<Future<JCL_result>> tickets;
		tickets = new ArrayList<Future<JCL_result>>();
		try {
			
			//get all host
			int[] d = {2,3,6,7};
			hosts = this.getDevices(d);

			//Exec in all host
			for (int i=0; i < hosts.size(); i++) {
				tickets.add(this.executeOnDevice(hosts.get(i), objectNickname,args[i]));
			}

			return tickets;
		} catch (Exception e){
			System.err
			.println("JCL facade problem in executeAll(String objectNickname, Object[][] args)");
			return null;
		}
	}

	@Override
	public List<Future<JCL_result>> executeAll(String objectNickname,String methodName, Object[][] args) {
		List<Entry<String, String>> hosts;
		List<Future<JCL_result>> tickets;
		tickets = new ArrayList<Future<JCL_result>>();
		try {

			//get all host
			int[] d = {2,3,6,7};
			hosts = this.getDevices(d);

			//Exec in all host
			for (int i=0; i < hosts.size(); i++) {
				tickets.add(this.executeOnDevice(hosts.get(i), objectNickname,methodName,args[i]));
			}

			return tickets;
		} catch (Exception e) {
			System.err
			.println("JCL facade problem in executeAll(String objectNickname,String methodName, Object[][] args)");
			return null;
		}
	}

	@Override
	public List<Future<JCL_result>> executeAllCores(String objectNickname, Object... args) {
		List<Future<JCL_result>> tickets;
		List<Entry<String, String>> hosts;
		tickets = new ArrayList<Future<JCL_result>>();
		try {

			//get all host
			//get all host
			int[] d = {2,3,6,7};
			hosts = this.getDevices(d);

			//		hosts = this.getAllDevicesCores();


			//Exec in all host
			for (int i=0; i < hosts.size(); i++) {
				//Execute o same host all cores 
				Entry<String, String> device = hosts.get(i); 
				int core = this.getDeviceCore(device); 
				for(int j=0; j < core; j++){
					tickets.add(this.executeOnDevice(device, objectNickname,args));
				}
			}

			return tickets;
		} catch (Exception e) {
			System.err
			.println("JCL facade problem in executeAllCores(String objectNickname, Object... args)");
			return null;
		}
	}

	@Override
	public List<Future<JCL_result>> executeAllCores(String objectNickname,String methodName, Object... args) {
		List<Future<JCL_result>> tickets;
		List<Entry<String, String>> hosts;
		tickets = new ArrayList<Future<JCL_result>>();
		try {

			//get all host
			//get all host
			int[] d = {2,3,6,7};
			hosts = this.getDevices(d);

			//		hosts = this.getAllDevicesCores();


			//Exec in all host
			for (int i=0; i < hosts.size(); i++) {
				//Execute o same host all cores 
				Entry<String, String> device = hosts.get(i); 
				int core = this.getDeviceCore(device); 
				for(int j=0; j < core; j++){
					tickets.add(this.executeOnDevice(device, objectNickname,methodName,args));
				}
			}

			return tickets;
		} catch (Exception e) {
			System.err
			.println("JCL facade problem in executeAllCores(String objectNickname,String methodName, Object... args)");
			return null;
		}
	}

	@Override
	public List<Future<JCL_result>> executeAllCores(String objectNickname,String methodName, Object[][] args) {
		List<Future<JCL_result>> tickets;
		List<Entry<String, String>> hosts;
		tickets = new ArrayList<Future<JCL_result>>();
		try {

			//get all host
			//get all host
			int[] d = {2,3,6,7};
			hosts = this.getDevices(d);

			//Exec in all host
			int cont = 0;
			for (int i=0; i < hosts.size(); i++) {
				//Execute o same host all cores 
				Entry<String, String> device = hosts.get(i); 
				int core = this.getDeviceCore(device); 
				for(int j=0; j < core; j++){
					tickets.add(this.executeOnDevice(device, objectNickname, methodName,args[cont]));
					++cont;
				}			
			}

			return tickets;
		} catch (Exception e) {
			System.err
			.println("JCL facade problem in executeAllCores(String objectNickname,String methodName, Object[][] args)");
			return null;
		}
	}

	@Override
	public List<Future<JCL_result>> executeAllCores(String objectNickname, Object[][] args) {
		List<Future<JCL_result>> tickets;
		List<Entry<String, String>> hosts;
		tickets = new ArrayList<Future<JCL_result>>();
		try {

			//get all host
			//get all host
			int[] d = {2,3,6,7};
			hosts = this.getDevices(d);

			//Exec in all host
			int cont = 0;
			for (int i=0; i < hosts.size(); i++) {
				//Execute o same host all cores 
				Entry<String, String> device = hosts.get(i); 
				int core = this.getDeviceCore(device); 
				for(int j=0; j < core; j++){
					tickets.add(this.executeOnDevice(device, objectNickname,args[cont]));
					++cont;
				}			
			}

			return tickets;
		} catch (Exception e) {
			System.err
			.println("JCL facade problem in executeAllCores(String objectNickname, Object[][] args)");
			return null;
		}
	}

	@Override
	public Future<JCL_result> executeOnDevice(Entry<String, String> device, String objectNickname,
			Object... args) {

		try {

			//Get host
			String host = null,port = null,mac = null, portS = null;


			if (jars.containsKey(objectNickname)){
				// Get host	

				// Get host			
				Map<String, String> hostPort = this.getDeviceMetadata(device);

				host = hostPort.get("IP");
				port = hostPort.get("PORT");
				mac = hostPort.get("MAC");
				portS = hostPort.get("PORT_SUPER_PEER");

			}else{

				Object[] argsLam = {serverAdd, String.valueOf(serverPort),device.getKey(),device.getValue(),objectNickname};
				Future<JCL_result> ticket = jcl.execute("JCL_FacadeImplLamb", "registerByServer", argsLam);

				Map<String, String> hostPort = (Map<String, String>) ticket.get().getCorrectResult();

				if(hostPort.size()==0){
					System.err.println("No class Found!!!");
				}

				host = hostPort.get("IP");
				port = hostPort.get("PORT");
				mac = hostPort.get("MAC");
				portS = hostPort.get("PORT_SUPER_PEER");

				List<String> js = new ArrayList<String>();
				js.add(host+port+mac+portS);
				jarsSlaves.put(objectNickname,js);
			}

			//Test if host contain jar
			if(jarsSlaves.get(objectNickname).contains(host+port+mac+portS)){
				//Just exec
				Object[] argsLam = {objectNickname,host,port,mac,portS,new Boolean(false),args};
				Future<JCL_result> ticket = super.execute("JCL_FacadeImplLamb", "execute", argsLam);
				return ticket;
			} else{

				//Exec and register
				Object[] argsLam = {objectNickname,host,port,mac,portS,jars.get(objectNickname),new Boolean(false),args};
				Future<JCL_result> ticket = super.execute("JCL_FacadeImplLamb", "executeAndRegister", argsLam);
				//ticket.get();
				jarsSlaves.get(objectNickname).add(host+port+mac+portS);
				return ticket;								
			}
		} catch (Exception e) {
			System.err
			.println("JCL facade problem in executeOnHost(String className, Object... args)");
			e.printStackTrace();
			return null;
		}
	}

	@Override
	public Map<String, String> getDeviceMetadata(Entry<String, String> device) {
		try {

			//getHosts			
			for(Map<String, Map<String, String>> ids:devices.values()){				
				for (Entry<String, Map<String, String>>  d: ids.entrySet()) {
					if (d.getKey().equals(device.getKey()))
						return d.getValue(); 
				}				
			}

			System.err.println("Device not found!!!");
			return null;

		} catch (Exception e) {
			System.err.println("problem in JCL facade getHosts()");
			e.printStackTrace();
			return null;
		}
	}

	@Override
	public boolean setDeviceMetadata(Entry<String, String> device, Map<String, String> metadata) {
		try {

			//getHosts			
			for(Map<String, Map<String, String>> ids:devices.values()){				
				for (Entry<String, Map<String, String>>  d: ids.entrySet()) {
					if (d.getKey().equals(device.getKey()))
						return true; 
				}				
			}

			return false;

		} catch (Exception e) {
			System.err.println("problem in JCL facade getHosts()");
			e.printStackTrace();
			return false;
		}
	}

	@Override
	public Future<JCL_result> executeOnDevice(Entry<String, String> device, String objectNickname,
			String methodName, Object... args) {
		try {
			String host = null,port = null,mac = null, portS=null;

			if (jars.containsKey(objectNickname)){
				// Get host			

				Map<String, String> hostPort = this.getDeviceMetadata(device);


				host = hostPort.get("IP");
				port = hostPort.get("PORT");
				mac = hostPort.get("MAC");
				portS = hostPort.get("PORT_SUPER_PEER");

			}else{

				Object[] argsLam = {serverAdd, String.valueOf(serverPort),device.getKey(),device.getValue(),objectNickname};
				Future<JCL_result> ticket = jcl.execute("JCL_FacadeImplLamb", "registerByServer", argsLam);

				Map<String, String> hostPort = (Map<String, String>) ticket.get().getCorrectResult();

				if(hostPort.size()==0){
					System.err.println("No class Found!!!");
				}

				host = hostPort.get("IP");
				port = hostPort.get("PORT");
				mac = hostPort.get("MAC");
				portS = hostPort.get("PORT_SUPER_PEER");

				List<String> js = new ArrayList<String>();
				js.add(host+port+mac+portS);
				jarsSlaves.put(objectNickname,js);
			}


			//Test if host contain jar
			if(jarsSlaves.get(objectNickname).contains(host+port+mac+portS)){
				//Just exec				
				Object[] argsLam = {objectNickname,methodName,host,port,mac,portS,new Boolean(false),args};
				Future<JCL_result> ticket = super.execute("JCL_FacadeImplLamb", "execute", argsLam);
				return ticket;
			} else{

				//Exec and register
				Object[] argsLam = {objectNickname,methodName,host,port,mac,portS,jars.get(objectNickname),new Boolean(false),args};
				Future<JCL_result> ticket = super.execute("JCL_FacadeImplLamb", "executeAndRegister", argsLam);
				//ticket.get();
				jarsSlaves.get(objectNickname).add(host+port+mac+portS);
				return ticket;								
			}

		} catch (Exception e) {
			System.err
			.println("JCL facade problem in executeOnDevice(String host,String className, String methodName, Object... args)");
			e.printStackTrace();
			return null;
		}
	}

	@Override
	public List<JCL_result> getAllResultBlocking(List<Future<JCL_result>> ID){
		List<JCL_result> result = new ArrayList<JCL_result>(ID.size());
		//		List<Future<JCL_result>> Ids = new ArrayList<Future<JCL_result>>(ID.size());
		try {
			//result = jcl.getAllResultBlocking(ID);
			//Get Pacu results IDs

			for (Future<JCL_result> t:ID){	
				JCL_result re = t.get();
				result.add(re);
			}
			//Get all Results
			//			resultF = jcl.getAllResultBlocking(Ids);

			return result;
		} catch (Exception e){
			System.err
			.println("problem in JCL facade getAllResultBlocking(List<String> ID)");
			e.printStackTrace();
			return null;
		}
	}

	@Override
	public List<JCL_result> getAllResultUnblocking(List<Future<JCL_result>> ID) {
		//Vars
		List<JCL_result> result,resultF;
		List<Future<JCL_result>> Ids = new ArrayList<Future<JCL_result>>(ID.size());
		resultF = new ArrayList<JCL_result>(ID.size());
		try {
			//	result = jcl.getAllResultBlocking(ID);

			//Get Pacu results IDs
			for (Future<JCL_result> t:ID){
				//				long tL = Long.parseLong(t);
				JCL_result id = t.get(); 
				Object[] res = (Object[])id.getCorrectResult();
				Object[] arg = {((JCLFuture)t).getTicket(),res[0],res[1],res[2],res[3],res[4]};
				Ids.add(jcl.execute("JCL_FacadeImplLamb", "getResultUnblocking", arg));
			}

			//Get all Results
			for(Future<JCL_result> t:Ids){
				JCL_result res = t.get();
				if (res.getCorrectResult().equals("NULL")){
					res.setCorrectResult(null);
				}
				resultF.add(res);
			}

			return resultF;

		} catch (Exception e) {
			System.err
			.println("problem in JCL facade getAllResultUnblocking(String ID)");
			e.printStackTrace();
			return null;
		}
	}

	@Override
	public JCL_result removeResult(Future<JCL_result> ID) {
		try {

			//getResultUnblocking using lambari	
			Long ticket = ((JCLPFuture)ID).getTicket();

			Object[] res = (Object[])super.getResultBlocking(ticket).getCorrectResult();
			Object[] arg = {ticket,res[0],res[1],res[2],res[3],res[4]};
			Future<JCL_result> t = jcl.execute("JCL_FacadeImplLamb", "removeResult", arg);
			//			jcl.removeResult(ticket);
			super.removeResult(ticket);
			return t.get();

		} catch (Exception e) {
			System.err
			.println("problem in JCL facade removeResult(Future<JCL_result> ID)");
			JCL_result jclr = new JCL_resultImpl();
			jclr.setErrorResult(e);
			e.printStackTrace();

			return jclr;
		}
	}


	@Override
	public boolean instantiateGlobalVar(Object key,String nickName,
			File[] jar, Object[] defaultVarValue) {
		lock.readLock().lock();
		try {
			//Get Host
			int hostId = rand.nextInt(delta, key.hashCode(), devicesStorage.size());
			Entry<String, Map<String, String>> hostPort = devicesStorage.get(hostId);

			String host = hostPort.getValue().get("IP");
			String port = hostPort.getValue().get("PORT");
			String mac = hostPort.getValue().get("MAC");
			String portS = hostPort.getValue().get("PORT_SUPER_PEER");


			if(!jarsSlaves.containsKey(nickName)){
				// Local register
				JCL_message_register msg = new MessageRegisterImpl();
				msg.setJars(jar);
				msg.setJarsNames(jar);
				msg.setClassName(nickName);
				msg.setType(1);
				jars.put(nickName, msg);
				jarsSlaves.put(nickName, new ArrayList<String>());	
			}



			if(jarsSlaves.get(nickName).contains(host+port+mac+portS)){
				//instantiateGlobalVar using lambari
				Object[] argsLam = {key,nickName,defaultVarValue,host,port,mac,portS,hostId};
				Future<JCL_result> t = jcl.execute("JCL_FacadeImplLamb", "instantiateGlobalVar", argsLam);
				return (Boolean) (t.get()).getCorrectResult();
			}else{
				//instantiateGlobalVar using lambari
				Object[] argsLam = {key,nickName,jars.get(nickName),defaultVarValue,host,port,mac,portS,hostId};
				Future<JCL_result> t = jcl.execute("JCL_FacadeImplLamb", "instantiateGlobalVarAndReg", argsLam);
				jarsSlaves.get(nickName).add(host+port+mac+portS);
				return (Boolean)(t.get()).getCorrectResult();				
			}

		} catch (Exception e) {
			System.err
			.println("problem in JCL facade instantiateGlobalVar(Object key, String nickName,File[] jars, Object[] defaultVarValue)");
			e.printStackTrace();
			return false;
		} finally {
			lock.readLock().unlock();
		}
	}

	@Override
	synchronized public boolean instantiateGlobalVar(Object key, Object instance) {
		
		if(topicGranularity == Constants.Environment.HIGH_GRANULARITY_CONFIG_VALUE) {
			return this.instantiateGlobalVarHighGranularity(key, instance);
		} else {
			return this.instantiateGlobalVarLowGranularity(key, instance);
		}	
	}
	
	synchronized private boolean instantiateGlobalVarHighGranularity(Object key, Object instance) {
		JCL_result jclResultInstance = new JCL_resultImpl();
		
		jclResultInstance.setCorrectResult(instance);
		
		kafkaProducer
			.send(new ProducerRecord<>(
				key.toString(),
				Constants.Environment.GLOBAL_VAR_KEY,
			    jclResultInstance
			)
		);
		
		subscribedTopics.add(key.toString());
		kct.wakeup();
		
		return true;
	}
	
	synchronized private boolean instantiateGlobalVarLowGranularity(Object key, Object instance) {
		JCL_result jclResultInstance = new JCL_resultImpl();
		jclResultInstance.setCorrectResult(instance);
		
		ProducerRecord<String, JCL_result> record = new ProducerRecord<>(
				userIPAddress.toString(),
				key.toString(),
			    jclResultInstance
			); 
		record.headers().add("jcl-action", Constants.Environment.GLOBAL_VAR_KEY.getBytes());
		
		kafkaProducer.send(record);
		
		return true;
	}

	//Use on JCLHashMap to inst bins values
	protected static boolean instantiateGlobalVar(Set<Entry<?,?>> set, String gvname){
		//		TODO
		lock.readLock().lock();
		try {
			Map<Integer,JCL_message_list_global_var> gvList = new HashMap<Integer,JCL_message_list_global_var>();
			Schema scow = RuntimeSchema.getSchema(ObjectWrap.class);
			LinkedBuffer buffer = LinkedBuffer.allocate(1048576);

			//Create bin of global vars
			for(Entry<?,?> ent:set){
				Object key = (ent.getKey().toString()+"�Map�"+gvname);
				Object value = ent.getValue();
				int hostId = rand.nextInt(0, key.hashCode(), devicesStorage.size());

				// ################ Serialization key ########################
				buffer.clear();
				ObjectWrap objW = new ObjectWrap(key);					
				key = ProtobufIOUtil.toByteArray(objW,scow, buffer);			
				// ################ Serialization key ########################

				// ################ Serialization value ######################
				buffer.clear();
				objW = new ObjectWrap(value);
				value = ProtobufIOUtil.toByteArray(objW,scow, buffer);			
				// ################ Serialization value ######################



				if (gvList.containsKey(hostId)){
					JCL_message_list_global_var gvm = gvList.get(hostId);
					gvm.putVarKeyInstance(key, value);
				}else{
					JCL_message_list_global_var gvm = new MessageListGlobalVarImpl(key,value);
					gvm.setType(35);
					gvList.put(hostId, gvm);
				}
			}

			List<Future<JCL_result>> tick = new ArrayList<Future<JCL_result>>();

			//Create on host using lambari
			for(Entry<Integer, JCL_message_list_global_var> ent:gvList.entrySet()){
				Integer key = ent.getKey();
				JCL_message_list_global_var value = ent.getValue();

				//Get Host
				Entry<String, Map<String, String>> hostPort = devicesStorage.get(key);

				String host = hostPort.getValue().get("IP");
				String port = hostPort.getValue().get("PORT");
				String mac = hostPort.getValue().get("MAC");
				String portS = hostPort.getValue().get("PORT_SUPER_PEER");


				//instantiateGlobalVar using lambari
				Object[] argsLam = {host,port,mac,portS,value,key};
				tick.add(jcl.execute("JCL_FacadeImplLamb", "instantiateGlobalVar", argsLam));
			}

			List<JCL_result> result = jcl.getAllResultBlocking(tick);

			for(JCL_result res:result){
				if(!((Boolean)res.getCorrectResult())){
					return false;
				}
			}

			return true;
		} catch (Exception e){
			System.err
			.println("problem in JCL facade instantiateGlobalVar JCLHashMap.");
			e.printStackTrace();
			return false;
		}finally {
			lock.readLock().unlock();
		}
	}

	//Create bins of request JCLHashMap.
	protected static Map<Integer,JCL_message_generic> getBinValueInterator(Set set, String gvname){
		lock.readLock().lock();
		try {

			Map<Integer,JCL_message_generic> gvList = new HashMap<Integer,JCL_message_generic>();
			Schema<ObjectWrap> scow = RuntimeSchema.getSchema(ObjectWrap.class);

			//Create bin request
			for(Object key:set){

				ObjectWrap obj = scow.newMessage();
				ProtobufIOUtil.mergeFrom((((ByteBuffer)key).getArray()), obj, scow);    		
				Object k = obj.getobj();

				k = (k.toString()+"�Map�"+gvname);
				int hostId = rand.nextInt(0, k.hashCode(), devicesStorage.size());

				// ################ Serialization key ########################
				LinkedBuffer buffer = LinkedBuffer.allocate(1048576);
				ObjectWrap objW = new ObjectWrap(k);					
				k = ProtobufIOUtil.toByteArray(objW,scow, buffer);			
				// ################ Serialization key ########################

				if (gvList.containsKey(hostId)){
					JCL_message_generic gvm = gvList.get(hostId);
					((Set<implementations.util.Entry<Object, Object>>)gvm.getRegisterData()).add(new implementations.util.Entry<Object, Object>(k,key));
				}else{
					Set<implementations.util.Entry<Object, Object>> gvs = new HashSet();
					gvs.add(new implementations.util.Entry<Object, Object>(k,key));
					JCL_message_generic gvm = new MessageGenericImpl();
					gvm.setRegisterData(gvs);
					gvm.setType(38);
					gvList.put(hostId, gvm);
				}
			}			

			return gvList;
		} catch (Exception e) {
			System.err
			.println("problem in JCL facade getBinValueInterator(Set set)");
			e.printStackTrace();
			return null;
		} finally {
			lock.readLock().unlock();
		}
	}

	//Use on JCLHashMap put method
	protected static Object instantiateGlobalVarAndReturn(Object key, Object instance){
		// TODO Auto-generated method stub
		lock.readLock().lock();
		try {			

			//Get Host
			int hostId = rand.nextInt(0, key.hashCode(), devicesStorage.size());
			Entry<String, Map<String, String>> hostPort = devicesStorage.get(hostId);

			String host = hostPort.getValue().get("IP");
			String port = hostPort.getValue().get("PORT");
			String mac = hostPort.getValue().get("MAC");
			String portS = hostPort.getValue().get("PORT_SUPER_PEER");

			//instantiateGlobalVar using lambari
			Object[] argsLam = {key,instance,host,port,mac,portS,hostId};
			Future<JCL_result> t = jcl.execute("JCL_FacadeImplLamb", "instantiateGlobalVarReturn", argsLam);
			return (t.get()).getCorrectResult();

		} catch (Exception e) {
			// TODO Auto-generated catch block
			System.out.println("Erro in instantiateGlobalVar(Object key, Object instance,String classVar, boolean Registers)");
			e.printStackTrace();
			return false;
		} finally {
			lock.readLock().unlock();
		}
	}

	@Override
	public Future<Boolean> instantiateGlobalVarAsy(Object key,String nickName,
			File[] jar, Object[] defaultVarValue) {
		lock.readLock().lock();
		try {
			//Get Host
			int hostId = rand.nextInt(delta, key.hashCode(), devicesStorage.size());
			Entry<String, Map<String, String>> hostPort = devicesStorage.get(hostId);

			String host = hostPort.getValue().get("IP");
			String port = hostPort.getValue().get("PORT");
			String mac = hostPort.getValue().get("MAC");
			String portS = hostPort.getValue().get("PORT_SUPER_PEER");


			if(!jarsSlaves.containsKey(nickName)){
				// Local register
				JCL_message_register msg = new MessageRegisterImpl();
				msg.setJars(jar);
				msg.setJarsNames(jar);
				msg.setClassName(nickName);
				msg.setType(1);
				jars.put(nickName, msg);
				jarsSlaves.put(nickName, new ArrayList<String>());	
			}



			if(jarsSlaves.get(nickName).contains(host+port+mac+portS)){
				//instantiateGlobalVar using lambari
				Object[] argsLam = {key,nickName,defaultVarValue,host,port,mac,portS,hostId};
				Future<JCL_result> t = jcl.execute("JCL_FacadeImplLamb", "instantiateGlobalVar", argsLam);
				return new JCLVFuture<Boolean>(((JCLFuture)t).getTicket());
			}else{
				//instantiateGlobalVar using lambari
				Object[] argsLam = {key,nickName,jars.get(nickName),defaultVarValue,host,port,mac,portS,hostId};
				Future<JCL_result> t = jcl.execute("JCL_FacadeImplLamb", "instantiateGlobalVarAndReg", argsLam);
				jarsSlaves.get(nickName).add(host+port+mac+portS);
				return new JCLVFuture<Boolean>(((JCLFuture)t).getTicket());

			}

		} catch (Exception e) {
			System.err
			.println("problem in JCL facade instantiateGlobalVar(Object key, String nickName,File[] jars, Object[] defaultVarValue)");
			e.printStackTrace();
			return new JCLSFuture<Boolean>(false);
		}finally {
			lock.readLock().unlock();
		}
	}

	@Override
	public Future<Boolean> instantiateGlobalVarAsy(Object key, Object instance) {

		lock.readLock().lock();

		try {
			//Get Host
			int hostId = rand.nextInt(delta, key.hashCode(), devicesStorage.size());
			Entry<String, Map<String, String>> hostPort = devicesStorage.get(hostId);

			String host = hostPort.getValue().get("IP");
			String port = hostPort.getValue().get("PORT");
			String mac = hostPort.getValue().get("MAC");
			String portS = hostPort.getValue().get("PORT_SUPER_PEER");

			//instantiateGlobalVarAsy using lambari
			Object[] argsLam = {key,instance,host,port,mac,portS,hostId};
			Future<JCL_result> t = jcl.execute("JCL_FacadeImplLamb", "instantiateGlobalVar", argsLam);
			return new JCLVFuture<Boolean>(((JCLFuture)t).getTicket());

		} catch (Exception e) {
			System.err
			.println("problem in JCL facade instantiateGlobalVarAsy(String varName, Object instance)");
			e.printStackTrace();
			return new JCLSFuture<Boolean>(false);
		}finally {
			lock.readLock().unlock();
		}
	}

	@Override
	public boolean instantiateGlobalVarOnDevice(Entry<String, String> device, Object key, String className, File[] jar,
			Object[] args){
		try {

			if(containsGlobalVar(key) || key==null) return false;

			Map<String, String> hostPort = this.getDeviceMetadata(device);

			String host = hostPort.get("IP");
			String port = hostPort.get("PORT");
			String mac = hostPort.get("MAC");
			String portS = hostPort.get("PORT_SUPER_PEER");


			if(!jarsSlaves.containsKey(className)){
				// Local register
				JCL_message_register msg = new MessageRegisterImpl();
				msg.setJars(jar);
				msg.setJarsNames(jar);
				msg.setClassName(className);
				msg.setType(1);
				jars.put(className, msg);
				jarsSlaves.put(className, new ArrayList<String>());	
			}

			Object[] argsLamS = {hostPort,key,serverAdd,serverPort};
			Future<JCL_result> tS = jcl.execute("JCL_FacadeImplLamb", "instantiateGlobalVarOnHost", argsLamS);

			if (!(boolean)tS.get().getCorrectResult()){
				System.err
				.println("problem in JCL facade instantiateGlobalVarOnHost(String host, String nickName, String varName, File[] jars, Object[] defaultVarValue)");
				System.err
				.println("Erro in Server Register!!!!");
				return false;
			}

			if(jarsSlaves.get(className).contains(host+port+mac+portS)){
				//instantiateGlobalVar using lambari
				Object[] argsLam = {key,className,args,host,port,mac,portS,new Integer(0)};
				Future<JCL_result> t = jcl.execute("JCL_FacadeImplLamb", "instantiateGlobalVar", argsLam);
				return (Boolean) (t.get()).getCorrectResult();
			}else{
				//instantiateGlobalVar using lambari
				Object[] argsLam = {key,className,jars.get(className),args,host,port,mac,portS,new Integer(0)};
				Future<JCL_result> t = jcl.execute("JCL_FacadeImplLamb", "instantiateGlobalVarAndReg", argsLam);
				jarsSlaves.get(className).add(host+port+mac+portS);
				return (Boolean)(t.get()).getCorrectResult();				
			}		

		} catch (Exception e) {
			System.err
			.println("problem in JCL facade instantiateGlobalVarOnHost(String host, String nickName, String varName, File[] jars, Object[] defaultVarValue)");
			return false;
		}
	}

	@Override
	public boolean instantiateGlobalVarOnDevice(Entry<String, String> device, Object key,
			Object instance) {
		try {

			if(containsGlobalVar(key) || key==null) return false;
			for(Map.Entry<String,Map<String,String>> deviceI:devicesStorage){
				if(deviceI.getKey().equals(device.getKey())){

					Object[] argsLamS = {deviceI.getValue(),key,serverAdd,serverPort};
					Future<JCL_result> tS = jcl.execute("JCL_FacadeImplLamb", "instantiateGlobalVarOnHost", argsLamS);

					if (!(boolean)tS.get().getCorrectResult()){
						System.err
						.println("problem in JCL facade instantiateGlobalVarOnHost(String host, String nickName, String varName, File[] jars, Object[] defaultVarValue)");
						System.err
						.println("Erro in Server Register!!!!");
						return false;
					}

					String host = deviceI.getValue().get("IP");
					String port = deviceI.getValue().get("PORT");
					String mac = deviceI.getValue().get("MAC");
					String portS = deviceI.getValue().get("PORT_SUPER_PEER");

					//instantiateGlobalVar using lambari
					Object[] argsLam = {key,instance,host,port,mac,portS,new Integer(0)};
					Future<JCL_result> t = jcl.execute("JCL_FacadeImplLamb", "instantiateGlobalVar", argsLam);
					return (Boolean) (t.get()).getCorrectResult();
				}
			}
			return false;

		} catch (Exception e) {
			System.err
			.println("problem in JCL facade instantiateGlobalVarOnHost(String host, String varName, Object instance)");
			return false;
		}
	}

	@Override
	public boolean deleteGlobalVar(Object key) {
		try {
			kafkaProducer
				.send(new ProducerRecord<>(
					key.toString(),
					Constants.Environment.GLOBAL_VAR_DEL,
				    null
				)
			);
			
			return true;
			
		} catch (Exception e) {
			System.err
				.println("problem in JCL facade destroyGlobalVar(Object " + key + ")");
			e.printStackTrace();
			return false;
		}
	}

	@Override
	public boolean setValueUnlocking(Object key, Object value) {
		
		if(topicGranularity == Constants.Environment.HIGH_GRANULARITY_CONFIG_VALUE) {
			return this.setValueUnlockingHighGranularity(key, value);
		} else {
			return this.setValueUnlockingLowGranularity(key, value);
		}
	}
	
	private boolean setValueUnlockingHighGranularity(Object key, Object value) {
		JCL_result jclResult = new JCL_resultImpl();

		try {
			jclResult.setCorrectResult(value);
			
			kafkaProducer.send(
				new ProducerRecord<>(
					key.toString(),
					Constants.Environment.GLOBAL_VAR_KEY,
					jclResult
				)
			);
			
			kafkaProducer.send(
				new ProducerRecord<>(
					key.toString(),
					Constants.Environment.GLOBAL_VAR_RELEASE,
					new JCL_resultImpl()
				)
			);
			
			if((localResourceGlobalVar.isFinished()==false) || (localResourceGlobalVar.getNumOfRegisters()!=0)){
				while (localResourceGlobalVar.read(key + ":" + Constants.Environment.GLOBAL_VAR_ACQUIRE) != null);
			}
			
			// TODO deletar mensagens ?
			
			return true;
			
		} catch (Exception e) {
			System.err.println("problem in JCL facade setValueUnlocking(Object " + key + ", Object " + value + ")");
			
			e.printStackTrace();
			
			return false;
		}
	}
	
	private boolean setValueUnlockingLowGranularity(Object key, Object value) {
		JCL_result jclResult = new JCL_resultImpl();

		try {
			jclResult.setCorrectResult(value);
			
			ProducerRecord <String, JCL_result> pr = new ProducerRecord<String, JCL_result>(
					this.userIPAddress,
					key.toString(),
					jclResult
				);
			pr.headers().add("jcl-action", Constants.Environment.GLOBAL_VAR_KEY.getBytes());
			
			kafkaProducer.send(
				pr
			);
			
			pr = new ProducerRecord<String, JCL_result>(
					this.userIPAddress,
					key.toString(),
					new JCL_resultImpl()
				);
			pr.headers().add("jcl-action", Constants.Environment.GLOBAL_VAR_RELEASE.getBytes());
			
			kafkaProducer.send(
				pr
			);
			
			if((localResourceGlobalVar.isFinished()==false) || (localResourceGlobalVar.getNumOfRegisters()!=0)){
				while (localResourceGlobalVar.read(key + ":" + Constants.Environment.GLOBAL_VAR_ACQUIRE) != null);
			}
			
			// TODO deletar mensagens ?
			
			return true;
			
		} catch (Exception e) {
			System.err.println("problem in JCL facade setValueUnlocking(Object " + key + ", Object " + value + ")");
			
			e.printStackTrace();
			
			return false;
		}
	}
	
	@Override
	public JCL_result getValue(Object key) {
		
		if(topicGranularity == Constants.Environment.HIGH_GRANULARITY_CONFIG_VALUE) {
			return this.getValueHighGranularity(key);
		} else {
			return this.getValueLowGranularity(key);
		}
	}
	
	private JCL_result getValueHighGranularity(Object key) {
		JCL_result kafkaReturn = new JCL_resultImpl();
		AtomicBoolean checkedIfExistsOnServer = new AtomicBoolean();
		checkedIfExistsOnServer.set(false);
		
		try {
			if((localResourceGlobalVar.isFinished()==false) || (localResourceGlobalVar.getNumOfRegisters()!=0)){
				while ((kafkaReturn = localResourceGlobalVar.read(key.toString())) == null) {
					if(!subscribedTopics.contains(key.toString())) {
						subscribedTopics.add(key.toString());
						kct.wakeup();
					} else if(!checkedIfExistsOnServer.get()) { 
						checkedIfExistsOnServer.set(true);
						
						if(!containsGlobalVar(key)) {
							return kafkaReturn;
						}
					}
				}
			}
		} catch (Exception e) {
			System.err
				.println("problem in JCL_result getValue(" + key + ")");
			e.printStackTrace();
			kafkaReturn.setErrorResult(e);
		}
		return kafkaReturn;
	}

	private JCL_result getValueLowGranularity(Object key) {

		JCL_result kafkaReturn = new JCL_resultImpl();
		
		try {
			if((localResourceGlobalVar.isFinished()==false) || (localResourceGlobalVar.getNumOfRegisters()!=0)){
				while ((kafkaReturn = localResourceGlobalVar.read(key.toString())) == null);
			}
		} catch (Exception e) {
			System.err
				.println("problem in JCL_result getValue(" + key + ")");
			e.printStackTrace();
			kafkaReturn.setErrorResult(e);
		}
		
		return kafkaReturn;
	}
	
	private boolean canAcquireGlobalVar (Object key, String lockToken) {
		Entry<String, JCL_result> minEntry = null;
		String prefix = key + ":" + Constants.Environment.LOCK_PREFIX + ":";
		
		for (Entry<String, JCL_result> entry : localResourceGlobalVar.entrySet()) {
			if(entry.getKey().startsWith(prefix)) {				
				if (minEntry == null || Long.parseLong(entry.getValue().getCorrectResult().toString()) < Long.parseLong(minEntry.getValue().getCorrectResult().toString())) {
					minEntry = entry;
				}
			}
		}
		
		if(minEntry != null && minEntry.getKey().toString().contains(lockToken)) {
			return true;
		}
		
		return false;
	}
	
	// TODO
	@Override
	public JCL_result getValueLocking(Object key) {
		
		if(topicGranularity == Constants.Environment.HIGH_GRANULARITY_CONFIG_VALUE) {
			return this.getValueLockingHighGranularity(key);
		} else {
			return this.getValueLockingLowGranularity(key);
		}
	}
	
	private JCL_result getValueLockingHighGranularity(Object key) {
		JCL_result jclResult = new JCL_resultImpl();
		JCL_result jclResultLockToken = new JCL_resultImpl();
		String lockToken = UUID.randomUUID().toString();
		AtomicBoolean checkedIfExistsOnServer = new AtomicBoolean();
		checkedIfExistsOnServer.set(false);
		
		jclResultLockToken.setCorrectResult(lockToken);

		kafkaProducer.send(
			new ProducerRecord<>(
				key.toString(),
				Constants.Environment.GLOBAL_VAR_LOCK_KEY,
				jclResultLockToken
			)
		);
		
		try {

			if((localResourceGlobalVar.isFinished()==false) || (localResourceGlobalVar.getNumOfRegisters()!=0)){
				while ((jclResult = localResourceGlobalVar.read(key + ":" + Constants.Environment.LOCK_PREFIX + ":" + lockToken)) == null) {
					if(!subscribedTopics.contains(key.toString())) {
						subscribedTopics.add(key.toString());
						kct.wakeup();
					} else if(!checkedIfExistsOnServer.get()) {
						checkedIfExistsOnServer.set(true);
						
						if(!containsGlobalVar(key)) {
							return jclResult;
						}
					}
				};
			}

			while(!canAcquireGlobalVar(key, lockToken));			

			kafkaProducer
				.send(new ProducerRecord<>(
					key.toString(),
					Constants.Environment.GLOBAL_VAR_ACQUIRE,
					jclResultLockToken
				));

			if((localResourceGlobalVar.isFinished()==false) || (localResourceGlobalVar.getNumOfRegisters()!=0)){
				while ((jclResult = localResourceGlobalVar.read(key.toString())) == null);
			}
			
			return jclResult;
		} catch (Exception e){
			System.err
				.println("problem in JCL facade getValueLocking(Object " + key + ")");
			e.printStackTrace();
			
			jclResult.setErrorResult(e);
			
			return jclResult;
		}
	}
	
	private JCL_result getValueLockingLowGranularity(Object key) {
		JCL_result jclResult = new JCL_resultImpl();
		JCL_result jclResultLockToken = new JCL_resultImpl();
		String lockToken = UUID.randomUUID().toString();
		
		jclResultLockToken.setCorrectResult(lockToken);

		ProducerRecord <String, JCL_result> pr = new ProducerRecord<String, JCL_result>(
				this.userIPAddress,
				key.toString(),
				jclResultLockToken
			);
		pr.headers().add("jcl-action", Constants.Environment.GLOBAL_VAR_LOCK_KEY.getBytes());
		
		kafkaProducer.send(
			pr
		);
		
		try {

			if((localResourceGlobalVar.isFinished()==false) || (localResourceGlobalVar.getNumOfRegisters()!=0)){
				while ((jclResult = localResourceGlobalVar.read(key + ":" + Constants.Environment.LOCK_PREFIX + ":" + lockToken)) == null);
			}
			
			while(!canAcquireGlobalVar(key, lockToken));

			pr = new ProducerRecord<String, JCL_result>(
					this.userIPAddress,
					key.toString(),
					jclResultLockToken
				);
			pr.headers().add("jcl-action", Constants.Environment.GLOBAL_VAR_ACQUIRE.getBytes());
			
			kafkaProducer.send(
				pr
			);

			if((localResourceGlobalVar.isFinished()==false) || (localResourceGlobalVar.getNumOfRegisters()!=0)){
				while ((jclResult = localResourceGlobalVar.read(key.toString())) == null);
			}
			
			return jclResult;
		} catch (Exception e){
			System.err
				.println("problem in JCL facade getValueLocking(Object " + key + ")");
			e.printStackTrace();
			
			jclResult.setErrorResult(e);
			
			return jclResult;
		}
	}

	@Override
	public void destroy() {
		try {
			scheduler.shutdown();

			if (simpleSever!=null){
				simpleSever.end();
				Object[] argsLam = {serverAdd,serverPort};
				Future<JCL_result> t = jcl.execute("JCL_FacadeImplLamb", "removeClient", argsLam);
				t.get();
			}	

			ConnectorImpl.closeSocketMap();
			jcl.destroy();
			instance = null;
			jcl = null;

		} catch (Exception e) {
			System.err.println("problem in JCL facade destroy()");
			e.printStackTrace();
		}

	}

	@Override
	public boolean containsTask(String nickName){

		try {

			if (jars.containsKey(nickName))
				return true;


			Object[] argsLam = {serverAdd, String.valueOf(serverPort),null,"0",nickName};
			Future<JCL_result> t = jcl.execute("JCL_FacadeImplLamb", "containsTask", argsLam);
			return (boolean)t.get().getCorrectResult();

		} catch (Exception e) {
			System.err
			.println("problem in JCL facade containsTask(String nickName)");

			return false;
		}
	}

	@Override
	public boolean containsGlobalVar(Object key) {
		Properties properties = KafkaConfigProperties.getInstance().get();
		properties.put("topic.name", key);
		
		boolean exists = false;
		
		try {
			exists = (localResourceGlobalVar.read(key.toString()) != null) || jclTopicAdmin.exists(properties);
		} catch (Exception e) {
			System.err
				.println("problem in boolean containsGlobalVar(" + key + ")");
			e.printStackTrace();
		}
		
		return exists;
	}

	@Override
	public <T extends java.util.Map.Entry<String, String>> List<T> getDevices(){

		try {

			//getHosts

			List<T> result = new ArrayList<T>();	
			for(Map<String, Map<String, String>> ids:devices.values()){				
				for (Entry<String, Map<String, String>>  d: ids.entrySet()) {
					result.add((T)new Device(d.getKey(), d.getValue().get("DEVICE_ID")));
				}				
			}

			return result;

		} catch (Exception e) {
			System.err.println("problem in JCL facade getHosts()");
			e.printStackTrace();
			return null;
		}
	}

	//	@Override
	public List<Entry<String, String>> getDevices(int type[]){

		try {

			//getHosts

			List<Entry<String, String>> result = new ArrayList<Entry<String, String>>();	
			for(int ids:type){				
				for (Entry<String, Map<String, String>>  d: devices.get(ids).entrySet()) {
					result.add(new implementations.util.Entry(d.getKey(), d.getValue().get("DEVICE_ID")));
				}				
			}			
			return result;

		} catch (Exception e) {
			System.err.println("problem in JCL facade getHosts()");
			e.printStackTrace();
			return null;
		}
	}

	public Map<String, Map<String, String>> getDevicesMetadados(int type[]){

		try {

			//getHosts

			Map<String, Map<String, String>> result = new HashMap<String, Map<String, String>>();	
			for(int ids:type){
				result.putAll(devices.get(ids));
				//				for (Entry<String, Map<String, String>>  d: devices.get(ids).entrySet()) {
				//					result.add(new implementations.util.Entry(d.getKey(), d.getValue().get("DEVICE_ID")));
				//				}				
			}			
			return result;

		} catch (Exception e) {
			System.err.println("problem in JCL facade getHosts()");
			e.printStackTrace();
			return null;
		}
	}

	@Override
	public int getDeviceCore(Entry<String, String> device){
		try {
			// Get host ID			

			for(Map<String, Map<String, String>> ids:devices.values()){				
				if (ids.containsKey(device.getKey())){
					return Integer.parseInt(ids.get(device.getKey()).get("CORE(S)"));
				}
			}

			return 0;

		} catch (Exception e) {
			System.err.println("problem in JCL facade getDeviceCore(String hostID)");
			e.printStackTrace();
			return 0;
		}
	}

	@Override
	public int getClusterCores() {
		try {
			//var
			int core = 0;
			//sun all cores

			for(Entry<String, Map<String, String>> ids:devicesExec){	
				core+=Integer.parseInt(ids.getValue().get("CORE(S)"));
			}

			return core;

		} catch (Exception e) {
			System.err.println("problem in JCL facade getClusterCores()");
			e.printStackTrace();
			return 0;
		}
	}

	@Override
	public <T extends java.util.Map.Entry<String, String>> Map<T, Integer> getAllDevicesCores() {
		try {
			//var
			Map<T, Integer> hosts = new HashMap<T, Integer>();
			List<Entry<String, String>> list = new ArrayList<>();			

			//getHosts
			List<Entry<String, String>> result = new ArrayList<Entry<String, String>>();	
			for(Map<String, Map<String, String>> ids:devices.values()){				
				for (Entry<String, Map<String, String>>  d: ids.entrySet()) {					
					hosts.put((T) new Device(d.getKey(), d.getValue().get("DEVICE_ID")), Integer.parseInt(d.getValue().get("CORE(S)")));					
				}				
			}

			return hosts;

		} catch (Exception e) {
			System.err.println("problem in JCL facade getAllHostCores()");
			e.printStackTrace();
			return null;
		}
	}

	@Override
	public boolean isLock(Object key){
		try {
			if((localResourceGlobalVar.isFinished()==false) || (localResourceGlobalVar.getNumOfRegisters()!=0)) {
				if (localResourceGlobalVar.read(key + ":" + Constants.Environment.GLOBAL_VAR_ACQUIRE) == null) {
					return false;
				}
			}

			return true;

		} catch (Exception e) {
			System.err
				.println("problem in JCL facade isLock(Object " + key + ")");
			e.printStackTrace();
			return false;
		}
	}

	@Override
	public boolean cleanEnvironment() {

		try {

			//cleanEnvironment using lambari
			Object[] argsLam = {serverAdd,serverPort};
			Future<JCL_result> t = jcl.execute("JCL_FacadeImplLamb", "cleanEnvironment", argsLam);
			return (Boolean) (t.get()).getCorrectResult();

		} catch (Exception e) {
			System.err.println("problem in JCL facade cleanEnvironment()");
			e.printStackTrace();
			return false;
		}
	}

	//Get HashMap
	public static <K, V> Map<K, V> GetHashMap(String gvName){
		getInstance();
		
		return new JCLHashMap<K, V>(gvName);
	}

	//Get HashMap
	//	public static <K, V> Map<K, V> GetHashMap(String gvName,String ClassName,File[] f){
	//		return new JCLHashMap<K, V>(gvName,ClassName,f);
	//	}

	public static JCL_facade getInstance() {
		return Holder.getInstance();
	}

	public static JCL_facade getInstancePacu() {
		Properties properties = new Properties();
		try {
			properties.load(new FileInputStream("../jcl_conf/config.properties"));
		}catch (FileNotFoundException e){					
			System.err.println("File not found (../jcl_conf/config.properties) !!!!!");
			System.out.println("Create properties file ../jcl_conf/config.properties.");
			try {
				File file = new File("../jcl_conf/config.properties");
				file.getParentFile().mkdirs(); // Will create parent directories if not exists
				file.createNewFile();

				OutputStream output = new FileOutputStream(file,false);

				// set the properties value
				properties.setProperty("distOrParell", "true");
				properties.setProperty("serverMainPort", "6969");
				properties.setProperty("superPeerMainPort", "6868");


				properties.setProperty("routerMainPort", "7070");
				properties.setProperty("serverMainAdd", "127.0.0.1");
				properties.setProperty("hostPort", "5151");


				properties.setProperty("nic", "");
				properties.setProperty("simpleServerPort", "4949");
				properties.setProperty("timeOut", "5000");

				properties.setProperty("byteBuffer", "5242880");
				properties.setProperty("routerLink", "5");
				properties.setProperty("enablePBA", "false");

				properties.setProperty("PBAsize", "50");
				properties.setProperty("delta", "0");
				properties.setProperty("PGTerm", "10");

				properties.setProperty("twoStep", "false");
				properties.setProperty("useCore", "100");
				properties.setProperty("deviceID", "Host1");

				properties.setProperty("enableDinamicUp", "false");
				properties.setProperty("findServerTimeOut", "1000");
				properties.setProperty("findHostTimeOut", "1000");

				properties.setProperty("enableFaultTolerance", "false");
				properties.setProperty("verbose", "true");
				properties.setProperty("encryption", "false");

				properties.setProperty("deviceType", "3");
				properties.setProperty("mqttBrokerAdd", "127.0.0.1");
				properties.setProperty("mqttBrokerPort", "1883");

				//save properties to project root folder

				properties.store(output, null);
			} catch (IOException e1) {
				// TODO Auto-generated catch block
				e1.printStackTrace();
			}
		}catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

		return Holder.getInstancePacu(properties);
	}

	public static JCL_facade getInstanceLambari(){
		return Holder.getInstanceLambari();
	}
	
	public static class Holder extends implementations.sm_kernel.JCL_FacadeImpl.Holder{
//		private static JCLResultResource localResourceExecute;
		
		protected static String ServerIP(){
			return serverAdd;
		}

		protected static int ServerPort(){
			return serverPort;
		}

		protected synchronized static JCL_facade getInstance(){
			Properties properties = new Properties();
			Properties kafkaProperties = new Properties();
			
			try {
				properties.load(new FileInputStream("../jcl_conf/config.properties"));
			}catch (FileNotFoundException e){					
				System.err.println("File not found (../jcl_conf/config.properties) !!!!!");
				System.out.println("Create properties file ../jcl_conf/config.properties.");
				try {
					File file = new File("../jcl_conf/config.properties");
					file.getParentFile().mkdirs(); // Will create parent directories if not exists
					file.createNewFile();

					OutputStream output = new FileOutputStream(file,false);

					// set the properties value
					properties.setProperty("distOrParell", "true");
					properties.setProperty("serverMainPort", "6969");
					properties.setProperty("superPeerMainPort", "6868");


					properties.setProperty("routerMainPort", "7070");
					properties.setProperty("serverMainAdd", "127.0.0.1");
					properties.setProperty("hostPort", "5151");


					properties.setProperty("nic", "");
					properties.setProperty("simpleServerPort", "4949");
					properties.setProperty("timeOut", "5000");

					properties.setProperty("byteBuffer", "5242880");
					properties.setProperty("routerLink", "5");
					properties.setProperty("enablePBA", "false");

					properties.setProperty("PBAsize", "50");
					properties.setProperty("delta", "0");
					properties.setProperty("PGTerm", "10");

					properties.setProperty("twoStep", "false");
					properties.setProperty("useCore", "100");
					properties.setProperty("deviceID", "Host1");

					properties.setProperty("enableDinamicUp", "false");
					properties.setProperty("findServerTimeOut", "1000");
					properties.setProperty("findHostTimeOut", "1000");

					properties.setProperty("enableFaultTolerance", "false");
					properties.setProperty("verbose", "true");
					properties.setProperty("encryption", "false");

					properties.setProperty("deviceType", "3");
					properties.setProperty("mqttBrokerAdd", "127.0.0.1");
					properties.setProperty("mqttBrokerPort", "1883");

					//save properties to project root folder

					properties.store(output, null);
				} catch (IOException e1) {
					// TODO Auto-generated catch block
					e1.printStackTrace();
				}
			}catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
			
			try {
				kafkaProperties.load(new FileInputStream(Constants.Environment.JCLKafkaConfig()));
				
				String groupId = "jcl-" + InetAddress.getLocalHost().getHostAddress() + "-" + UUID.randomUUID().toString();
				kafkaProperties.put("group.id", groupId);
				
				kafkaProperties.store(new FileOutputStream(Constants.Environment.JCLKafkaConfig()), null);
			}catch (FileNotFoundException e){					
				System.err.println("File not found (" + Constants.Environment.JCLKafkaConfig() + ") !!!!!");
				System.out.println("Create properties file " + Constants.Environment.JCLKafkaConfig());
				try {
					File kafkaFile = new File(Constants.Environment.JCLKafkaConfig());
					kafkaFile.getParentFile().mkdirs(); // Will create parent directories if not exists
					kafkaFile.createNewFile();

					OutputStream output = new FileOutputStream(kafkaFile,false);
					
					// set the properties value
					kafkaProperties.setProperty("bootstrap.servers", "192.168.1.7:9092");
					kafkaProperties.setProperty("client.id", "jcl-client");
					kafkaProperties.setProperty("group.id", "jcl-" + "192.168.1.7");
					kafkaProperties.setProperty("auto.offset.reset", "earliest");
					kafkaProperties.setProperty("topic.partitions", "1");
					kafkaProperties.setProperty("topic.replication.factor", "1");
					//save properties to project root folder

					kafkaProperties.store(output, null);
				} catch (IOException e1) {
					// TODO Auto-generated catch block
					e1.printStackTrace();
				}
			}catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}

			//get type of Instance 
			if (Boolean.valueOf(properties.getProperty("distOrParell"))){
				return getInstancePacu(properties);
			}else{
				return getInstanceLambari();
			}
		}
		
		protected synchronized static JCL_facade getInstancePacu(Properties properties){
			//Pacu type

			if (instance == null){
				instance = new JCL_FacadeImpl(properties);
			}	

			return instance;
		}

		protected synchronized static JCL_facade getInstanceLambari(){
			//Lambari type
			if (jcl == null){
				jcl = implementations.sm_kernel.JCL_FacadeImpl.getInstance();
			}			
			return jcl;
		}

		protected List<Entry<String, Map<String, String>>> getDeviceS(){
			return devicesStorage;
		}

		//create hash key map
		protected boolean createhashKey(String gvName, int IDhost){

			//Get Ip host
			Entry<String, Map<String, String>> hostPort = devicesStorage.get(IDhost);


			String host = hostPort.getValue().get("IP");
			String port = hostPort.getValue().get("PORT");
			String mac = hostPort.getValue().get("MAC");
			String portS = hostPort.getValue().get("PORT_SUPER_PEER");




			JCL_message_register msgReg = null;


			//Create connection
			JCL_connector controlConnector = new ConnectorImpl();
			controlConnector.connect(host,Integer.parseInt(port),mac);

			//createhashKey using lambari
			JCL_message_generic mc = new MessageGenericImpl();

			mc.setRegisterData(gvName);
			mc.setType(28);
			JCL_message_generic mr = (JCL_message_generic) controlConnector.sendReceiveG(mc,portS);
			controlConnector.disconnect();
			return (Boolean) mr.getRegisterData();
		}

		//add key to hash key map
		protected boolean hashAdd(String gvName,Object Key, int IDhost){


			//Get Ip host
			Entry<String, Map<String, String>> hostPort = devicesStorage.get(IDhost);

			String host = hostPort.getValue().get("IP");
			String port = hostPort.getValue().get("PORT");
			String mac = hostPort.getValue().get("MAC");
			String portS = hostPort.getValue().get("PORT_SUPER_PEER");

			//hashAdd using lambari
			JCL_message_generic mc = new MessageGenericImpl();
			Object[] ob = {gvName,Key};
			mc.setRegisterData(ob);
			mc.setType(29);
			JCL_connector controlConnector = new ConnectorImpl();
			controlConnector.connect(host,Integer.parseInt(port),mac);
			JCL_message_generic mr = (JCL_message_generic) controlConnector.sendReceiveG(mc,portS);
			controlConnector.disconnect();
			return (Boolean) mr.getRegisterData();
		}

		//remove key from hash key map
		protected boolean hashRemove(String gvName,Object Key, int IDhost){
			//Get Ip host
			Entry<String, Map<String, String>> hostPort = devicesStorage.get(IDhost);

			String host = hostPort.getValue().get("IP");
			String port = hostPort.getValue().get("PORT");
			String mac = hostPort.getValue().get("MAC");
			String portS = hostPort.getValue().get("PORT_SUPER_PEER");

			// ################ Serialization key ########################
			LinkedBuffer buffer = LinkedBuffer.allocate(1048576);
			ObjectWrap objW = new ObjectWrap(Key);	
			Schema scow = RuntimeSchema.getSchema(ObjectWrap.class);
			Key = ProtobufIOUtil.toByteArray(objW,scow, buffer);			
			// ################ Serialization key ########################


			//hashRemove using lambari
			JCL_message_generic mc = new MessageGenericImpl();
			Object[] ob = {gvName,Key};
			mc.setRegisterData(ob);
			mc.setType(30);
			JCL_connector controlConnector = new ConnectorImpl();
			controlConnector.connect(host,Integer.parseInt(port),mac);
			JCL_message_generic mr = (JCL_message_generic) controlConnector.sendReceiveG(mc,portS);
			controlConnector.disconnect();
			return (Boolean) mr.getRegisterData();
		}

		//hash key map contain key
		protected boolean containsKey(String gvName,Object Key, int IDhost){

			//Get Ip host
			Entry<String, Map<String, String>> hostPort = devicesStorage.get(IDhost);

			String host = hostPort.getValue().get("IP");
			String port = hostPort.getValue().get("PORT");
			String mac = hostPort.getValue().get("MAC");
			String portS = hostPort.getValue().get("PORT_SUPER_PEER");

			// ################ Serialization key ########################
			LinkedBuffer buffer = LinkedBuffer.allocate(1048576);
			ObjectWrap objW = new ObjectWrap(Key);	
			Schema scow = RuntimeSchema.getSchema(ObjectWrap.class);
			Key = ProtobufIOUtil.toByteArray(objW,scow, buffer);			
			// ################ Serialization key ########################


			//containsKey using lambari
			JCL_message_generic mc = new MessageGenericImpl();
			Object[] ob = {gvName,Key};
			mc.setRegisterData(ob);
			mc.setType(31);
			JCL_connector controlConnector = new ConnectorImpl();
			controlConnector.connect(host,Integer.parseInt(port),mac);
			JCL_message_generic mr = (JCL_message_generic) controlConnector.sendReceiveG(mc,portS);
			controlConnector.disconnect();
			return (Boolean) mr.getRegisterData();
		}

		//hash key map size
		protected int hashSize(String gvName, int IDhost){

			//Get Ip host
			Entry<String, Map<String, String>> hostPort = devicesStorage.get(IDhost);

			String host = hostPort.getValue().get("IP");
			String port = hostPort.getValue().get("PORT");
			String mac = hostPort.getValue().get("MAC");
			String portS = hostPort.getValue().get("PORT_SUPER_PEER");	

			//hashSize using lambari
			JCL_message_generic mc = new MessageGenericImpl();
			mc.setRegisterData(gvName);
			mc.setType(32);
			JCL_connector controlConnector = new ConnectorImpl();
			controlConnector.connect(host,Integer.parseInt(port),mac);
			JCL_message_generic mr = (JCL_message_generic) controlConnector.sendReceiveG(mc,portS);
			controlConnector.disconnect();
			return (Integer) mr.getRegisterData();
		}		

		//clean hash key map
		protected Set hashClean(String gvName, int IDhost){			
			//Get Ip host
			Entry<String, Map<String, String>> hostPort = devicesStorage.get(IDhost);

			String host = hostPort.getValue().get("IP");
			String port = hostPort.getValue().get("PORT");
			String mac = hostPort.getValue().get("MAC");
			String portS = hostPort.getValue().get("PORT_SUPER_PEER");

			//hashClean using lambari
			JCL_message_generic mc = new MessageGenericImpl();
			mc.setRegisterData(gvName);
			mc.setType(33);
			JCL_connector controlConnector = new ConnectorImpl();
			controlConnector.connect(host,Integer.parseInt(port),mac);
			JCL_message_generic mr = (JCL_message_generic) controlConnector.sendReceiveG(mc,portS);
			controlConnector.disconnect();
			return (Set) mr.getRegisterData();
		}

		//get set of keys
		protected Set getHashSet(String gvName, int IDhost){

			//Get Ip host
			Entry<String, Map<String, String>> hostPort = devicesStorage.get(IDhost);

			String host = hostPort.getValue().get("IP");
			String port = hostPort.getValue().get("PORT");
			String mac = hostPort.getValue().get("MAC");
			String portS = hostPort.getValue().get("PORT_SUPER_PEER");

			//getHashSet using lambari
			JCL_message_generic mc = new MessageGenericImpl();
			mc.setRegisterData(gvName);
			mc.setType(34);
			JCL_connector controlConnector = new ConnectorImpl();
			controlConnector.connect(host,Integer.parseInt(port),mac);
			JCL_message_generic mr = (JCL_message_generic) controlConnector.sendReceiveG(mc,portS);
			controlConnector.disconnect();
			return (Set) mr.getRegisterData();
		}

		//Inst key and values bins
		protected boolean instantiateBin(Object object,String gvname){
			return instantiateGlobalVar((Set<Entry<?, ?>>) object,gvname);
		}

		//put on cluster
		protected Object hashPut(Object key, Object instance){
			return instantiateGlobalVarAndReturn(key,instance);
		}

		//Get queue interator
		protected Map<Integer,JCL_message_generic> getHashQueue(Queue queue,Set key, String gvname){
			try {
				Map<Integer,JCL_message_generic> gvList = getBinValueInterator(key, gvname);			

				//getHashQueue using lambari
				Iterator<Entry<Integer,JCL_message_generic>> intGvList = gvList.entrySet().iterator();

				if (intGvList.hasNext()){

					Entry<Integer,JCL_message_generic> entHost = intGvList.next();
					JCL_message_generic mc = entHost.getValue();				

					//Get Host
					Entry<String, Map<String, String>> hostPort = devicesStorage.get(entHost.getKey());

					String host = hostPort.getValue().get("IP");
					String port = hostPort.getValue().get("PORT");
					String mac = hostPort.getValue().get("MAC");
					String portS = hostPort.getValue().get("PORT_SUPER_PEER");


					//Using lambari			
					Object[] argsLam = {mc,queue,host,port,mac,portS,entHost.getKey()};
					Future<JCL_result> t = jcl.execute("JCL_FacadeImplLamb", "getHashValues", argsLam);						
					t.get();
					//			jcl.getResultBlocking(t).getCorrectResult();			

					intGvList.remove();
				}

				return gvList;
			} catch (Exception e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
				return null;
			}
		}

		//get value from cluster
		protected Future<JCL_result> getHashValues(Queue queue,JCL_message_generic mc, int key){
			//Get Host
			Entry<String, Map<String, String>> hostPort = devicesStorage.get(key);
			String host = hostPort.getValue().get("IP");
			String port = hostPort.getValue().get("PORT");
			String mac = hostPort.getValue().get("MAC");
			String portS = hostPort.getValue().get("PORT_SUPER_PEER");


			//Using lambari
			Object[] argsLam = {mc,queue,host,port,mac,portS,key};
			Future<JCL_result> t = jcl.execute("JCL_FacadeImplLamb", "getHashValues", argsLam);

			return t;
		}

		protected Object getResultBlocking(Future<JCL_result> t){
			try {
				return (t.get()).getCorrectResult();
			} catch (Exception e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
				return null;
			}
		}
		
		protected JCL_result getResultBlocking(Long ID) {
			if(topicGranularity == Constants.Environment.HIGH_GRANULARITY_CONFIG_VALUE) {
				return this.getResultBlockingHighGranularity(ID);
			} else {
				return this.getResultBlockingLowGranularity(ID);
			}
		}
		
		private JCL_result getResultBlockingHighGranularity(Long ID) {
			JCL_result result, resultF = new JCL_resultImpl();
			Object[] resultConfig;
			Object hostAddress, hostTaskId;

			try {
				result = super.getResultBlocking(ID);
				resultConfig = (Object[])result.getCorrectResult();
				hostTaskId = resultConfig[0];
				hostAddress = resultConfig[1].toString().replace(".", "");
				String topicName = hostTaskId.toString() + hostAddress;

				try {
					if((localResourceExecute.isFinished()==false) || (localResourceExecute.getNumOfRegisters()!=0)){
						while ((resultF = localResourceExecute.read(topicName)) == null) {
							if(!subscribedTopics.contains(topicName)) {
								subscribedTopics.add(topicName);
								kct.wakeup();
							}
						}
					}
				} catch (Exception e){
					System.err
						.println("problem in JCL_result getResultBlocking(" + ID + ")");
					e.printStackTrace();

					resultF.setErrorResult(e);
				}

				return resultF;
			} catch (Exception e) {
				System.err
				.println("problem in JCL facade getResultBlocking(String ID)");
				e.printStackTrace();

				JCL_result jclr = new JCL_resultImpl();
				jclr.setErrorResult(e);

				return jclr;
			}
		}
		
		private JCL_result getResultBlockingLowGranularity(Long ID) {
			JCL_result result, resultF = new JCL_resultImpl();
			Object[] resultConfig;
			Object hostAddress, hostTaskId;

			try {
				result = super.getResultBlocking(ID);
				resultConfig = (Object[])result.getCorrectResult();
				hostTaskId = resultConfig[0];
				hostAddress = resultConfig[1].toString();
				String topicName = hostAddress.toString();
				String executeKeyOnLocalResourceExecute = Constants.Environment.EXECUTE_KEY + hostTaskId;
//				System.out.println(Thread.currentThread().getId() + " | " + topicName);
				try {
					if((localResourceExecute.isFinished()==false) || (localResourceExecute.getNumOfRegisters()!=0)){
						while ((resultF = localResourceExecute.read(executeKeyOnLocalResourceExecute)) == null) {
							if(!subscribedTopics.contains(topicName)) {
								subscribedTopics.add(topicName);
								kct.wakeup();
							}
						}
					}
				} catch (Exception e){
					System.err
						.println("problem in JCL_result getResultBlocking(" + ID + ")");
					e.printStackTrace();

					resultF.setErrorResult(e);
				}
//				System.out.println(Thread.currentThread().getId() + " | " + topicName + " | "  + resultF.getCorrectResult());
				return resultF;
			} catch (Exception e) {
				System.err
				.println("problem in JCL facade getResultBlocking(String ID)");
				e.printStackTrace();

				JCL_result jclr = new JCL_resultImpl();
				jclr.setErrorResult(e);

				return jclr;
			}
		}
	}

	@Override
	public Map<String, String> getDeviceConfig(Entry<String, String> deviceNickname) {
		try {
			Map<String, String> hostData = this.getDeviceMetadata(deviceNickname);

			String DeviceIP = hostData.get("IP");
			String DevicePort = hostData.get("PORT");
			String MAC = hostData.get("MAC");
			String portSP = hostData.get("PORT_SUPER_PEER");

			JCL_message_metadata msg = new MessageMetadataImpl();

			msg.setType(42);

			JCL_connector controlConnector = new ConnectorImpl(false);
			controlConnector.connect(DeviceIP,Integer.parseInt(DevicePort),MAC);       

			JCL_message_metadata jclR = (JCL_message_metadata) controlConnector.sendReceiveG(msg, portSP);

			if(jclR != null){
				JCL_connector conn = new ConnectorImpl(false);
				conn.connect(Holder.ServerIP(), Holder.ServerPort(),null);
				JCL_message_metadata jclRe = (JCL_message_metadata) controlConnector.sendReceiveG(msg, portSP);           
				return jclR.getMetadados();
			}

		} catch (Exception e) {
			System.err.println("Problem at JCL in getDeviceMetadata(Entry<String, String> deviceNickname)");
			e.printStackTrace();
		}
		return null;
	}

	@Override
	public boolean setDeviceConfig(Entry<String, String> deviceNickname, Map<String, String> metadata) {
		try {

			if(metadata==null)return false;

			Map<String, String> hostData = this.getDeviceMetadata(deviceNickname);

			String deviceIP = hostData.get("IP");
			String devicePort = hostData.get("PORT");
			String MAC = hostData.get("MAC");
			String portSP = hostData.get("PORT_SUPER_PEER");
			JCL_message_metadata msg = new MessageMetadataImpl();

			msg.setType(43);

			msg.setMetadados(metadata);

			JCL_connector controlConnector = new ConnectorImpl(false);
			controlConnector.connect(deviceIP,Integer.parseInt(devicePort), MAC);       

			JCL_message_bool jclR = (JCL_message_bool) controlConnector.sendReceiveG(msg, portSP);

			if(jclR.getRegisterData()[0]){
				JCL_connector conn = new ConnectorImpl(false);
				conn.connect(Holder.ServerIP(), Holder.ServerPort(), MAC);
				JCL_message_bool jclRe = (JCL_message_bool) controlConnector.sendReceiveG(msg, portSP);           
				return true;
			} else{
				return false;
			}
		} catch (Exception e){
			System.err.println("Problem at JCL in setDeviceMetadata(Entry<String, String> deviceNickname, Map<String, String> metadata)");
			e.printStackTrace();
			return false;
		}
	}	
}
