package implementations.sm_kernel;

import interfaces.kernel.JCL_execute;
import interfaces.kernel.JCL_orb;
import interfaces.kernel.JCL_result;
import interfaces.kernel.JCL_task;
import java.io.File;
import java.lang.reflect.Constructor;
import java.lang.reflect.Method;
import java.net.URL;
import java.net.URLClassLoader;
import java.util.Enumeration;
import java.util.Map;
import java.util.Properties;
import java.util.Map.Entry;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentSkipListSet;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.jar.JarEntry;
import java.util.jar.JarFile;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.apache.kafka.common.serialization.StringSerializer;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.google.common.primitives.Primitives;

import implementations.dm_kernel.KafkaMessageSerializer;
import javassist.ClassClassPath;
import javassist.ClassPool;
import javassist.CtClass;
import javassist.CtMethod;
import javassist.CtNewMethod;
import javassist.CtPrimitiveType;

public class JCL_orbImpl<T extends JCL_result> implements JCL_orb<T> {

	private Map<String, Class<?>> nameMap;
	private Map<Object, Object> globalVars;
	private static AtomicInteger RegisterMsg;
	private Map<String, JCL_execute> cache1;
	private AtomicLong idClass = new AtomicLong(0);
	private Map<String, Integer> cache2;
	private Set<Object> locks;
	private long timeOut = 3000L;
	private static JCL_orb instance;
	private static JCL_orb instancePacu;
	private Map<Long, T> results;
	/** 3.0 begin **/
	private Producer<String, String> kafkaProducer;
	/** 3.0 end **/
	
	private URLClassLoader classLoader = (URLClassLoader) ClassLoader.getSystemClassLoader();
	
	private JCL_orbImpl() {
		nameMap = new ConcurrentHashMap<String, Class<?>>();
		locks = new ConcurrentSkipListSet<Object>();
		globalVars = new ConcurrentHashMap<Object, Object>();
		cache1 = new ConcurrentHashMap<String, JCL_execute>();
		cache2 = new ConcurrentHashMap<String, Integer>();
		
		/** 3.0 begin **/
		Properties producerProperties = new Properties();
		producerProperties.put(
			ProducerConfig.CLIENT_ID_CONFIG, 
			"jcl-client");
		producerProperties.put(
			ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, 
			"localhost" + ":" + "9092");
		producerProperties.put(
			ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, 
			StringSerializer.class.getName());
		producerProperties.put(
			ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, 
			StringSerializer.class.getName());
		
		this.kafkaProducer = new KafkaProducer<>(producerProperties);
		/** 3.0 end **/
	}

	@Override
	public void execute(JCL_task task) {
		try {
			int para;
			
			if (nameMap.containsKey(task.getObjectName())){

				T jResult = results.get(task.getTaskID());
				JCL_execute instance = cache1.get(task.getObjectName());
				if (task.getMethodParameters() == null)
					para = 0;
				else
					para = task.getMethodParameters().length;
					
				int type = cache2.get(task.getObjectName() + ":" + task.getObjectMethod() + ":" + para);
				task.setTaskTime(System.nanoTime());
				Object result = instance.JCLExecPacu(type, task.getMethodParameters());
				task.setTaskTime(System.nanoTime());

				jResult.setTime(task.getTaskTime());
//				jResult.setMemorysize(ObjectSizeCalculator.getObjectSize(instance));
				jResult.setMemorysize(10);
				
				if (result != null) {
					jResult.setCorrectResult(result);
				} else {
					jResult.setCorrectResult("no result");
				}

				/** begin 3.0 **/
				Long ID = new Long(task.getTaskID());
				
				ObjectMapper objectMapper = new ObjectMapper()
						.configure(SerializationFeature.INDENT_OUTPUT, true)
						.setSerializationInclusion(JsonInclude.Include.NON_NULL);
				
				ProducerRecord<String, String> producedRecord = new ProducerRecord<>(
					"jcl-input", 
					ID.toString(),
					objectMapper.writeValueAsString(result)
				);
				
				try {
					this.kafkaProducer.send(producedRecord);
					
				} catch(Throwable t) {
					System
					.err
					.println(t);
				}
				/** end 3.0 **/
				
				synchronized (jResult) {
					jResult.notifyAll();
				}

			} else {
				Long ini = System.currentTimeMillis();
				boolean ok = true;

				while ((System.currentTimeMillis() - ini) < timeOut) {
					
					if (RegisterMsg.get() > 0){
						ini = System.currentTimeMillis();
					}
										
					if (nameMap.containsKey(task.getObjectName())) {

						T jResult = results.get(task.getTaskID());
						JCL_execute instance = (JCL_execute) cache1.get(task.getObjectName());
						if (task.getMethodParameters() == null)
							para = 0;
						else
							para = task.getMethodParameters().length;
						int type = cache2.get(task.getObjectName() + ":" + task.getObjectMethod() + ":" + para);

						task.setTaskTime(System.nanoTime());
						Object result = instance.JCLExecPacu(type, task.getMethodParameters());
						task.setTaskTime(System.nanoTime());

						jResult.setTime(task.getTaskTime());
						jResult.setMemorysize(10);
						
						if (result != null) {
							jResult.setCorrectResult(result);
						} else {
							jResult.setCorrectResult("no result");
						}

						/** begin 3.0 **/
						Long ID = new Long(task.getTaskID());
						
						ObjectMapper objectMapper = new ObjectMapper()
								.configure(SerializationFeature.INDENT_OUTPUT, true)
								.setSerializationInclusion(JsonInclude.Include.NON_NULL);
						
						ProducerRecord<String, String> producedRecord = new ProducerRecord<>(
							"jcl-input", 
							ID.toString(),
							objectMapper.writeValueAsString(result)
						);
						
						try {
							this.kafkaProducer.send(producedRecord);
							
						} catch(Throwable t) {
							System
							.err
							.println(t);
						}
						/** end 3.0 **/

						synchronized (jResult) {
							jResult.notifyAll();
						}

						ok = false;
						break;
					}
				}
								
				if (((System.currentTimeMillis() - ini) >= timeOut) && (ok)) {
					System.out.println("Timeout!!");
					System.out.println("Class: " + task.getObjectName() + "  Register: "
							+ nameMap.containsKey(task.getObjectName()));
					T jResult = results.get(task.getTaskID());
					jResult.setTime(task.getTaskTime());
					jResult.setErrorResult(new Exception("No register class"));
					synchronized (jResult) {
						jResult.notifyAll();
					}
				}
			}
		} catch (IllegalArgumentException el) {
			System.err.println("Invalid argument. Method:" + task.getObjectMethod());
			T jResult = results.get(task.getTaskID());
			jResult.setTime(task.getTaskTime());
			jResult.setErrorResult(el);

			synchronized (jResult) {
				jResult.notifyAll();
			}

		} catch (NullPointerException en) {

			System.err.println("Method invalid:" + task.getObjectMethod());
			en.printStackTrace();
			T jResult = results.get(task.getTaskID());
			jResult.setTime(task.getTaskTime());
			jResult.setErrorResult(en);

			synchronized (jResult) {
				jResult.notifyAll();
			}

		} catch (Exception e) {
			System.err.println("problem in JCL orb execute(JCL_task task, Map<String, T> results)");
			System.err.println("Dados");
			System.err.println(task.getObjectName());
			System.err.println(task.getObjectMethod());
			System.out.println(task.getMethodParameters());

			e.printStackTrace();
			T jResult = results.get(task.getTaskID());
			jResult.setTime(task.getTaskTime());
			jResult.setErrorResult(e);

			synchronized (jResult) {
				jResult.notifyAll();
			}
		}
	}

	@Override
	public synchronized boolean register(Class<?> serviceClass, String nickName) {
		try {
			if (nameMap.containsKey(nickName)) {
				return false;
			} else {
				String mainComponentClass = serviceClass.getName();
				ClassPool pool = ClassPool.getDefault();
				pool.insertClassPath(new ClassClassPath(serviceClass));
				CtClass cc = pool.getAndRename(mainComponentClass, mainComponentClass + idClass.getAndIncrement());
				CtMethod[] ms = cc.getDeclaredMethods();
				StringBuilder buffer = new StringBuilder();
				buffer.append("public Object JCLExecPacu(int type,Object[] arg){");
				buffer.append("switch(type) {");
				
				for (int i = 0; i < ms.length; i++) {
					CtClass[] paType = ms[i].getParameterTypes();
					cache2.put(nickName + ":" + ms[i].getName() + ":" + paType.length, i);
					buffer.append("case " + i + ":{");
					CtClass retur = ms[i].getReturnType();
					if ((retur.isPrimitive()) && (!retur.getName().equals("void"))) {
						buffer.append("return new " + ((CtPrimitiveType) retur).getWrapperName() + "(" + ms[i].getName()
								+ "(");
					} else if (retur.getName().equals("void")) {
						buffer.append(ms[i].getName() + "(");
					} else {
						buffer.append("return " + ms[i].getName() + "(");
					}
					for (int cont = 0; cont < paType.length; cont++) {
						if (paType[cont].isPrimitive()) {
							CtPrimitiveType priType = (CtPrimitiveType) paType[cont];
							buffer.append("((" + priType.getWrapperName() + ")arg[" + cont + "])."
									+ priType.getGetMethodName() + "()");
						} else {
							buffer.append("(" + paType[cont].getName() + ")arg[" + cont + "]");
						}

						if (cont < (paType.length - 1))
							buffer.append(",");
					}

					if ((retur.isPrimitive()) && (!retur.getName().equals("void"))) {
						buffer.append("));");
					} else if (retur.getName().equals("void")) {
						buffer.append("); return null;");
					} else {
						buffer.append(");");
					}
					buffer.append("}");
				}
				buffer.append("}");
				buffer.append("return null;}");
				CtClass ccInt = pool.get("interfaces.kernel.JCL_execute");
				cc.addInterface(ccInt);
				CtMethod method = CtNewMethod.make(buffer.toString(), cc);
				cc.addMethod(method);
				
				Class<? extends JCL_execute> cla = cc.toClass();
				cache1.put(nickName, cla.newInstance());
				nameMap.put(nickName, cla);
				
				return true;
			}

		} catch (Exception e) {
			System.err.println("problem in JCL orb register(Class<?> serviceClass, String nickName)");
			e.printStackTrace();
			return false;
		}

	}

	@Override
	public synchronized boolean register(CtClass cc, String nickName) {
		try {
			if (nameMap.containsKey(nickName)) {
				return false;
			} else {
				ClassPool pool = ClassPool.getDefault();
				CtMethod[] ms = cc.getDeclaredMethods();
				StringBuilder buffer = new StringBuilder();
				buffer.append("public Object JCLExecPacu(int type,Object[] arg){");
				buffer.append("switch(type) {");
				
				for (int i = 0; i < ms.length; i++) {
					CtClass[] paType = ms[i].getParameterTypes();
					cache2.put(nickName + ":" + ms[i].getName() + ":" + paType.length, i);
					buffer.append("case " + i + ":{");
					CtClass retur = ms[i].getReturnType();
					if ((retur.isPrimitive()) && (!retur.getName().equals("void"))) {
						buffer.append("return new " + ((CtPrimitiveType) retur).getWrapperName() + "(" + ms[i].getName()
								+ "(");
					} else if (retur.getName().equals("void")) {
						buffer.append(ms[i].getName() + "(");
					} else {
						buffer.append("return " + ms[i].getName() + "(");
					}
					for (int cont = 0; cont < paType.length; cont++) {
						if (paType[cont].isPrimitive()) {
							CtPrimitiveType priType = (CtPrimitiveType) paType[cont];
							buffer.append("((" + priType.getWrapperName() + ")arg[" + cont + "])."
									+ priType.getGetMethodName() + "()");
						} else {
							buffer.append("(" + paType[cont].getName() + ")arg[" + cont + "]");
						}

						if (cont < (paType.length - 1))
							buffer.append(",");
					}

					if ((retur.isPrimitive()) && (!retur.getName().equals("void"))) {
						buffer.append("));");
					} else if (retur.getName().equals("void")) {
						buffer.append("); return null;");
					} else {
						buffer.append(");");
					}
					buffer.append("}");
				}
				buffer.append("}");
				buffer.append("return null;}");
				CtClass ccInt = pool.get("interfaces.kernel.JCL_execute");
				cc.defrost();
				String oldName = cc.getName();
				cc.replaceClassName(oldName, oldName + idClass.getAndIncrement());
				cc.addInterface(ccInt);
				CtMethod method = CtNewMethod.make(buffer.toString(), cc);
				cc.addMethod(method);
				
				Class<? extends JCL_execute> cla = cc.toClass();
				cache1.put(nickName, cla.newInstance());
				nameMap.put(nickName, cla);

				return true;
			}

		} catch (Exception e) {
			System.err.println("problem in JCL orb register(Class<?> serviceClass, String nickName)");
			e.printStackTrace();
			return false;
		}

	}

	@Override
	public boolean register(String mainComponentClass, String nickName) {
		try {
			if (nameMap.containsKey(nickName)) {
				return false;
			} else {
				ClassPool pool = ClassPool.getDefault();
				CtClass cc = pool.get(mainComponentClass);
				CtMethod[] ms = cc.getDeclaredMethods();
				StringBuilder buffer = new StringBuilder();
				buffer.append("public Object JCLExecPacu(int type,Object[] arg){");
				buffer.append("switch(type) {");
				for (int i = 0; i < ms.length; i++) {
					CtClass[] paType = ms[i].getParameterTypes();
					cache2.put(nickName + ":" + ms[i].getName() + ":" + paType.length, i);
					buffer.append("case " + i + ":{");
					CtClass retur = ms[i].getReturnType();
					if ((retur.isPrimitive()) && (!retur.getName().equals("void"))) {
						buffer.append("return new " + ((CtPrimitiveType) retur).getWrapperName() + "(" + ms[i].getName()
								+ "(");
					} else if (retur.getName().equals("void")) {
						buffer.append(ms[i].getName() + "(");
					} else {
						buffer.append("return " + ms[i].getName() + "(");
					}
					for (int cont = 0; cont < paType.length; cont++) {
						if (paType[cont].isPrimitive()) {
							CtPrimitiveType priType = (CtPrimitiveType) paType[cont];
							buffer.append("((" + priType.getWrapperName() + ")arg[" + cont + "])."
									+ priType.getGetMethodName() + "()");
						} else {
							buffer.append("(" + paType[cont].getName() + ")arg[" + cont + "]");
						}

						if (cont < (paType.length - 1))
							buffer.append(",");
					}

					if ((retur.isPrimitive()) && (!retur.getName().equals("void"))) {
						buffer.append("));");
					} else if (retur.getName().equals("void")) {
						buffer.append("); return null;");
					} else {
						buffer.append(");");
					}
					buffer.append("}");
				}
				buffer.append("}");
				buffer.append("return null;}");
				CtClass ccInt = pool.get("interfaces.kernel.JCL_execute");
				cc.addInterface(ccInt);
				CtMethod method = CtNewMethod.make(buffer.toString(), cc);
				cc.addMethod(method);
				
				Class<? extends JCL_execute> cla = cc.toClass();
				cache1.put(nickName, cla.newInstance());
				nameMap.put(nickName, cla);

				return true;
			}

		} catch (Exception e) {
			System.err.println("problem in JCL orb register(Class<?> serviceClass, String nickName)");
			e.printStackTrace();
			return false;
		}
	}

	@Override
	public synchronized boolean unRegister(String nickName) {
		try {
			
			System.out.println("Unregister");

			if (nameMap.containsKey(nickName)) {
				nameMap.remove(nickName);
				cache1.remove(nickName);
				for (String key : cache2.keySet()) {
					if (key.startsWith(nickName)) {
						cache2.remove(key);
					}
				}
				return true;
			} else {
				return false;
			}

		} catch (Exception e) {
			System.err.println("problem in JCL orb unRegister(String nickName)");

			return false;
		}
	}

	@Override
	public synchronized boolean destroyGlobalVar(Object key) {
		try {

			if (globalVars.containsKey(key)) {
				globalVars.remove(key);
				return true;
			} else
				return false;

		} catch (Exception e) {
			System.err.println("problem in JCL orb destroyGlobalVar(String varName)");

			return false;
		}
	}

	@Override
	public synchronized boolean instantiateGlobalVar(Object key, Object instance) {
		try {
			if (instance == null) {
				return false;
			}
			if (!globalVars.containsKey(key)) {
				globalVars.put(key, instance);
				return true;
			} else
				return false;

		} catch (Exception e) {
			System.err.println("problem in JCL orb instantiateGlobalVar(String varName, Object instance)");

			return false;
		}

	}

	@Override
	public synchronized boolean instantiateGlobalVar(Object key, String nickName, File[] jars,
			Object[] defaultVarValue) {
		try {

			if (globalVars.containsKey(key)) {
				return false;
			} else {

				for (File f : jars) {
					this.addURL((f.toURI().toURL()));
				}

				if (defaultVarValue == null) {
					Object var = Class.forName(nickName).newInstance();
					globalVars.put(key, var);
					return true;
				} else {
					Constructor[] cs = Class.forName(nickName).getConstructors();
					for (Constructor c : cs) {
						if (c.getParameterTypes() != null) {
							boolean flag = true;
							if (c.getParameterTypes().length == defaultVarValue.length)
								for (int i = 0; i < c.getParameterTypes().length; i++) {
									Class<?> aClass = c.getParameterTypes()[i];
									if (aClass.isPrimitive()) aClass = Primitives.wrap(aClass);
									if (!aClass.equals(defaultVarValue[i].getClass())) {
										flag = false;
									}

								}
							if (flag) {
								Object var = c.newInstance(defaultVarValue);
								globalVars.put(key, var);

								return true;

							}
						}
					}
				}

				return false;
			}

		} catch (Exception e) {
			
			System.err.println(
					"problem in JCL orb instantiateGlobalVar(String varName, File[] jars, Object[] defaultVarValue)");
			e.printStackTrace();
			return false;
		}
	}

	@Override
	public synchronized boolean instantiateGlobalVar(Object key, String nickName, Object[] defaultVarValue) {
		try {
			if (globalVars.containsKey(key)) {
				return false;
			} else {
				if (defaultVarValue == null) {
					Object var = Class.forName(nickName).newInstance();
					globalVars.put(key, var);
					return true;
				} else {
					Constructor[] cs = Class.forName(nickName).getConstructors();
					for (Constructor c : cs) {
						if (c.getParameterTypes() != null) {
							boolean flag = true;
							if (c.getParameterTypes().length == defaultVarValue.length){
								for (int i = 0; i < c.getParameterTypes().length; i++) {
									Class<?> aClass = c.getParameterTypes()[i];
									if (aClass.isPrimitive()) aClass = Primitives.wrap(aClass);
									if (!aClass.equals(defaultVarValue[i].getClass())) {
										flag = false;
									}

								}
							}else{
								flag = false;
							}
							
							if (flag) {
								Object var = c.newInstance(defaultVarValue);
								globalVars.put(key, var);
								return true;

							}
						}
					}
				}

				return false;
			}

		} catch (Exception e) {
			System.err.println(
					"problem in JCL orb instantiateGlobalVar(String varName, File[] jars, Object[] defaultVarValue)");
			e.printStackTrace();
			return false;
		}
	}

	@Override
	public Object getValue(Object key) {
		try {
			Object obj = globalVars.get(key);
			if (obj == null) {
				return new String("No value found!");
			} else {
				return obj;
			}
		} catch (Exception e) {
			System.err.println("problem in JCL orb getValue(String varName)");

			return e.getMessage();
		}
	}

	@Override
	public Object getValueLocking(Object key) {
		try {
			Object obj = globalVars.get(key);
			if (obj != null) {
				synchronized (locks) {
					// no wait and notify
					if (locks.contains(key))
						return null;

//					PILHA
					locks.add(key);
					return obj;
				}
			} else {
				return new String("No value found!");
			}
		} catch (Exception e) {
			System.err.println("problem in JCL orb getValueLocking(String varName)");

			return e.getMessage();
		}
	}

	@Override
	public boolean setValueUnlocking(Object key, Object value) {
		try {
			if (globalVars.containsKey(key)) {
				globalVars.put(key, value);
				locks.remove(key);

//				PILHA
				return true;
			} else {
				return false;
			}
		} catch (Exception e) {
			System.err.println("problem in JCL orb setValueUnlocking(String varName)");

			return false;
		}

	}

	private void addURL(URL url) throws Exception {

		Class<URLClassLoader> clazz = URLClassLoader.class;

		// Use reflection
		Method method = clazz.getDeclaredMethod("addURL", new Class[] { URL.class });
		method.setAccessible(true);
		method.invoke(classLoader, new Object[] { url });
	}

	public boolean register(File[] fs, String classToBeExecuted) {
		try {
			for (File f : fs) {
				this.addURL((f.toURI().toURL()));
			}

			JarFile jar = new JarFile(fs[0]);

			for (Enumeration<JarEntry> entries = jar.entries(); entries.hasMoreElements();) {
				JarEntry entry = entries.nextElement();
				String file = entry.getName();

				if (file.endsWith(classToBeExecuted + ".class")) {
					String classname = file.replace('/', '.').substring(0, file.length() - 6);
					return this.register(classname, classToBeExecuted);

				}
			}

			return false;
		} catch (Exception e) {
			e.printStackTrace();
			return false;
		}
	}

	@Override
	public boolean containsTask(String nickName) {
		if (nickName == null)
			return false;
		return this.nameMap.containsKey(nickName);
	}

	@Override
	public boolean containsGlobalVar(Object key) {
		if (key == null)
			return false;
		return this.globalVars.containsKey(key);
	}

	@Override
	public Set<Entry<Object, Object>> getGlobalVarEntrySet() {

		return this.globalVars.entrySet();
	}

	@Override
	public boolean lockGlobalVar(Object key) {
		try {
			if (globalVars.containsKey(key)) {
				if (locks.contains(key)) {
					this.lockGlobalVar(key);
					return false;
				} else {
					synchronized (locks) {
						return locks.add(key);
					}
				}
			} else {
				return false;
			}
		} catch (Exception e) {
			System.err.println("problem in JCL orb lock(Object key)");
			return false;
		}
	}

	@Override
	public boolean unLockGlobalVar(Object key) {
		// TODO Auto-generated method stub
		try {
			if (globalVars.containsKey(key)) {
				synchronized (locks) {
					if (locks.contains(key)) {
						globalVars.remove(key);
						return locks.remove(key);
					}

					return false;
				}
			} else {
				return false;
			}
		} catch (Exception e) {
			System.err.println("problem in JCL orb lock(Object key)");
			return false;
		}
	}

	@SuppressWarnings("rawtypes")
	public static JCL_orb getInstance() {
		return Holder.getInstance();
	}

	@SuppressWarnings("rawtypes")
	public static JCL_orb getInstancePacu() {
		return Holder.getInstancePacu();
	}

	private static class Holder {

		protected static JCL_orb getInstance() {
			if (instance == null) {
				instance = new JCL_orbImpl();
			}
			return instance;
		}

		public static JCL_orb getInstancePacu() {
			if (instancePacu == null) {
				instancePacu = new JCL_orbImpl();
			}
			return instancePacu;
		}
	}

	@Override
	public boolean isLock(Object key) {
		if (key == null)
			return false;
		return locks.contains(key);
	}

	@Override
	public boolean cleanEnvironment() {
		try {
			
			globalVars.clear();
			locks.clear();
			nameMap.clear();
			cache1.clear();
			cache2.clear();

		} catch (Exception e) {
			// TODO: handle exception
			return false;
		}
		return true;
	}

	public Map<Long, T> getResults() {
		return results;
	}

	public void setResults(Map<Long, T> results) {
		this.results = results;
	}

	public static AtomicInteger getRegisterMsg() {
		return RegisterMsg;
	}

	public static void setRegisterMsg(AtomicInteger registerMsg) {
		RegisterMsg = registerMsg;
	}
}
