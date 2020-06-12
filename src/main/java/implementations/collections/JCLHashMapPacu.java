package implementations.collections;

import implementations.dm_kernel.JCLTopic;
import implementations.dm_kernel.user.JCL_FacadeImpl.Holder;
import implementations.util.ObjectWrap;
import interfaces.kernel.JCL_facade;
import interfaces.kernel.JCL_map;
import interfaces.kernel.JCL_message_generic;
import interfaces.kernel.JCL_result;
import io.protostuff.LinkedBuffer;
import io.protostuff.ProtobufIOUtil;
import io.protostuff.Schema;
import io.protostuff.runtime.RuntimeSchema;
import java.io.*;
import implementations.util.ByteBuffer;
import implementations.util.JCLConfigProperties;

import java.util.AbstractCollection;
import java.util.AbstractSet;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Queue;
import java.util.Set;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;

import commom.Constants;
import commom.JCLResultResource;
import commom.JCLResultResourceContainer;
import commom.JCLResultSerializer;
import commom.JCL_resultImpl;

public class JCLHashMapPacu<K,V>
    extends Holder
    implements JCL_map<K,V>, Cloneable, Serializable
{

    /**
	 * 
	 */
	private static final long serialVersionUID = -4532275712761435044L;

	/**
     * Default JCL pacu instance.
     */
	private static JCL_facade DEFAULT_JCL;

    /**
     * The number of key-value mappings contained in this map.
     */
	private Map<String, String> Localize;
	
    /**
     * The number of key-value mappings contained in this map.
     */
	private int idLocalize;
    
    /**
     * The HashMap name in the cluster.
     */
    private String gvName;
   
    /**
     *Class name.
     */
    private String clName="";

    /**
     *Register Class.
     */
    private boolean regClass = false; 

    private int size;
    
    /** begin 3.0 **/
    private Producer<String, JCL_result> kafkaProducer;
    private static JCLResultResourceContainer localResourceMapContainer;
    private String gvNameKafka;
    /** end 3.0 **/
    
    /**
     * Constructs with HashMap name.
     */
    public JCLHashMapPacu(String gvName){
    	this.gvName = gvName;

    	//Get Pacu
    	Properties properties = new Properties();
		try {
			properties.load(new FileInputStream(Constants.Environment.JCLConfig()));
		} catch (IOException e) {
			e.printStackTrace();
		}
    	
		DEFAULT_JCL = super.getInstancePacu(properties);
		
		this.initKafka(gvName);
    }
    
    public JCLHashMapPacu(String gvName, JCLResultResourceContainer localResourceMapContainerParam){
    	this.gvName = gvName;
    	localResourceMapContainer = localResourceMapContainerParam;
    	
    	//Get Pacu
    	Properties properties = new Properties();
		try {
			properties.load(new FileInputStream(Constants.Environment.JCLConfig()));
		} catch (IOException e) {
			e.printStackTrace();
		}
    	
		DEFAULT_JCL = super.getInstancePacu(properties);
    	
		initKafka(gvName);
    }
    
    // internal utilities
    private void initKafka(String gvName){
    	this.gvNameKafka = getKeyNameMapped(gvName);
    	
    	Properties properties = JCLConfigProperties.get(Constants.Environment.JCLKafkaConfig());
    	JCLTopic jclTopic = new JCLTopic();
		
    	properties.put("topic.name", gvNameKafka);
    	properties.put("transactional.id", gvNameKafka);
		
    	kafkaProducer = new KafkaProducer<>(
			properties,
			new StringSerializer(),
			new JCLResultSerializer()
		);
    	
    	kafkaProducer.initTransactions();
		
		boolean existsMap = jclTopic.exists(properties);
		
		if(!existsMap) {
			jclTopic.create(properties);
			
			JCL_result jclResultHeader = new JCL_resultImpl();
			ProducerRecord<String, JCL_result> producedRecord;
			
			Object[] header = {
					0	
			};
			
			jclResultHeader.setCorrectResult(header);
			
			producedRecord = new ProducerRecord<>(
				gvNameKafka,
				Constants.Environment.MAPHEADER,
				jclResultHeader
			);
			
			kafkaProducer
				.send(producedRecord);
		}
    }

    private String getKeyNameMapped(Object key) {
    	String keyMapped = Constants.Environment.MAPPREFIX + key;
    	
    	return keyMapped;
    }
    
    private String getKeyNameMapped(Object mapName, Object key) {
    	String keyMapped = Constants.Environment.MAPPREFIX + key;
    	
    	return keyMapped;
    }
    
    /**
     * Returns the number of key-value mappings in this map.
     *
     * @return the number of key-value mappings in this map
     */
    public int size(){
    	JCLResultResource selfMapResource = null;
    	JCL_result jclResult = null;
    	int size = -1;
    	
		try {
			System.out.println("int size() begin");
			if((localResourceMapContainer.isFinished() == false) || (localResourceMapContainer.getNumOfRegisters() != 0)){
				while ((selfMapResource = localResourceMapContainer.read(gvNameKafka)) == null);
			}
			System.out.println("int size() end");
			if((selfMapResource.isFinished() == false) || (selfMapResource.getNumOfRegisters() != 0)){
				while ((jclResult = selfMapResource.read(Constants.Environment.MAPHEADER)) == null);
			}
			
			ArrayList<Object> header = (ArrayList<Object>) jclResult.getCorrectResult();
			
			size = (int) header.get(0);
		} catch (Exception e) {
			System.err
				.println("problem in JCL_HashMapPacu V get(" + gvNameKafka + ")");
			e.printStackTrace();
		}
    	
        return size;
    }        

    /**
     * Returns <tt>true</tt> if this map contains no key-value mappings.
     *
     * @return <tt>true</tt> if this map contains no key-value mappings
     */
    public boolean isEmpty(){
    	if (size() == 0){
    		return true;
    	}
    	
    	return false;
    }

    /**
     * Returns the value to which the specified key is mapped.
     */
    public V get(Object key){
    	JCL_result jclResult = new JCL_resultImpl();
    	JCLResultResource jclResultResource = null;
		
    	try {
    		if((localResourceMapContainer.isFinished() == false) || (localResourceMapContainer.getNumOfRegisters() != 0)){
				while ((jclResultResource = localResourceMapContainer.read(gvNameKafka)) == null);
			}
    		
			if((jclResultResource.isFinished() == false) || (jclResultResource.getNumOfRegisters() != 0)){
				while ((jclResult = jclResultResource.read(key.toString())) == null);
			}
		} catch (Exception e){
			jclResult.setCorrectResult("no result");
			
			System
				.err
				.println("problem in JCL_HashMapPacu V get(" + key + ")");
			e.printStackTrace();
		}
    	
    	V value = (V) jclResult.getCorrectResult();
    	
    	return value;
    }
        
    /**
     * Returns and lock the value to which the specified key is mapped.
     */
    public V getLock(Object key){
    	V oldValue = null;
    	
        if (key != null) {
        	String keyNameMapped = this.getKeyNameMapped(this.gvName, key);
        	
        	oldValue = (V) JCLHashMapPacu.DEFAULT_JCL.getValueLocking(keyNameMapped).getCorrectResult();
        }else{
        	System.out.println("Can't get<K,V> with null key!");
        }        
        return (oldValue == null ? null : oldValue);
    }

    /**
     * Returns <tt>true</tt> if this map contains a mapping for the
     * specified key.
     *
     * @param   key   The key whose presence in this map is to be tested
     * @return <tt>true</tt> if this map contains a mapping for the specified
     * key.
     */
    public boolean containsKey(Object key){
    	boolean contains = false;
    	
    	try {
			if(localResourceMapContainer.read(gvNameKafka).read(key.toString()) != null) {
				contains = true;
			}
		} catch (Exception e) {
			e.printStackTrace();
		}
		
    	return contains;
    }
    
    final Entry<K,V> getEntry(Object key) {
    		V value = (V) DEFAULT_JCL.getValue(key.toString()+"¬Map¬"+gvName).getCorrectResult();
    		return new implementations.util.Entry(key,value);
    }
 
    /**
     * Associates the specified value with the specified key in this map.
     * If the map previously contained a mapping for the key, the old
     * value is replaced.
     *
     * @param key key with which the specified value is to be associated
     * @param value value to be associated with the specified key
     * @return the previous value associated with <tt>key</tt>, or
     *         <tt>null</tt> if there was no mapping for <tt>key</tt>.
     */
    
    public V put(K key, V value){
    	JCL_result jclResultInstance = new JCL_resultImpl();
    	JCL_result jclResultHeader = new JCL_resultImpl();
		
    	Object[] header = {
			size() + 1
		};
    	
		jclResultInstance.setCorrectResult(value);
		jclResultHeader.setCorrectResult(header);
		
		try {
			kafkaProducer.beginTransaction();
			
			kafkaProducer.send(
				new ProducerRecord<>(
					gvNameKafka,
					key.toString(),
					jclResultInstance
				)
			);
			
			kafkaProducer.send(
				new ProducerRecord<>(
					gvNameKafka,
					Constants.Environment.MAPHEADER,
					jclResultHeader
				)
			);
			
			kafkaProducer.commitTransaction();
		} catch (Exception e) {
			System.err
				.println("problem in JCL_HashMapPacu V put(" + key + ", " + value + ")");
			kafkaProducer.abortTransaction();
		}
        
        return value;
    }
    
    /**
     * Associates the specified value with the specified key in this map.
     * If the map previously contained a mapping for the key, the old
     * value is replaced.
     *
     * @param key key with which the specified value is to be associated
     * @param value value to be associated with the specified key
     * @return the previous value associated with <tt>key</tt>, or
     *         <tt>null</tt> if there was no mapping for <tt>key</tt>.
     */
    
    public V putUnlock(K key, V value){
    	V oldValue = null;
        if (key != null){        	
        	if(DEFAULT_JCL.containsGlobalVar(key.toString()+"¬Map¬"+gvName)){
        		oldValue = (V) DEFAULT_JCL.getValue(key.toString()+"¬Map¬"+gvName).getCorrectResult();
        		DEFAULT_JCL.setValueUnlocking((key.toString()+"¬Map¬"+gvName), value);
        	}else if (DEFAULT_JCL.instantiateGlobalVar((key.toString()+"¬Map¬"+gvName), value)){
    			
    			// ################ Serialization key ########################
    			LinkedBuffer buffer = LinkedBuffer.allocate(1048576);
    			ObjectWrap objW = new ObjectWrap(key);	
    			Schema scow = RuntimeSchema.getSchema(ObjectWrap.class);
    			byte[] k = ProtobufIOUtil.toByteArray(objW,scow, buffer);			
    			// ################ Serialization key ########################

        		super.hashAdd(gvName,ByteBuffer.wrap(k),idLocalize);
    		}
        }else{
       	 System.out.println("Can't put<K,V> with null key!");
        }        
        return (oldValue == null ? null : oldValue);
    }

    /**
     * Copies all of the mappings from the specified map to this map.
     * These mappings will replace any mappings that this map had for
     * any of the keys currently in the specified map.
     *
     * @param m mappings to be stored in this map
     * @throws NullPointerException if the specified map is null
     */
    public void putAll(Map<? extends K, ? extends V> m) {
        int numKeysToBeAdded = m.size();
                
        if (numKeysToBeAdded == 0)
            return;
        super.instantiateBin((Object)m.entrySet(), this.gvName);
                
        List<Object> obj =  new ArrayList<Object>();
		LinkedBuffer buffer = LinkedBuffer.allocate(1048576);

        for(K key:m.keySet()){
        	
			// ################ Serialization key ########################
			buffer.clear();
        	ObjectWrap objW = new ObjectWrap(key);	
			Schema scow = RuntimeSchema.getSchema(ObjectWrap.class);
			byte[] k = ProtobufIOUtil.toByteArray(objW,scow, buffer);			
			// ################ Serialization key ########################

        	obj.add(ByteBuffer.wrap(k));
        }   
        
        super.hashAdd(gvName, obj,idLocalize);                
    }

    /**
     * Removes the mapping for the specified key from this map if present.
     *
     * @param  key key whose mapping is to be removed from the map
     * @return the previous value associated with <tt>key</tt>, or
     *         <tt>null</tt> if there was no mapping for <tt>key</tt>.
     *         (A <tt>null</tt> return can also indicate that the map
     *         previously associated <tt>null</tt> with <tt>key</tt>.)
     */
    public V remove(Object key) {
    	V oldValue = null;
    	
    	 if (key != null){        	
         	if(DEFAULT_JCL.containsGlobalVar(key.toString()+"¬Map¬"+gvName)){
         		oldValue = (V) DEFAULT_JCL.getValue(key.toString()+"¬Map¬"+gvName).getCorrectResult();
         	}
     		if (DEFAULT_JCL.deleteGlobalVar(key.toString()+"¬Map¬"+gvName)){
     			super.hashRemove(gvName,key,idLocalize);
     		}
         }else{
        	 System.out.println("Can't remove null key!");
         }
        return (oldValue == null ? null : oldValue);
    }
    
    protected V removeInt(Object key) {
    	V oldValue = null;
    	
    	 if (key != null){        	
         	if(DEFAULT_JCL.containsGlobalVar(key.toString()+"¬Map¬"+gvName)){
         		oldValue = (V) DEFAULT_JCL.getValue(key.toString()+"¬Map¬"+gvName).getCorrectResult();
         	}
     		if (DEFAULT_JCL.deleteGlobalVar(key.toString()+"¬Map¬"+gvName)){
     			super.hashRemove(gvName,key,idLocalize);
     		}
         }else{
        	 System.out.println("Can't remove null key!");
         }
        return (oldValue == null ? null : oldValue);
    }
    
    /**
     * Removes all of the mappings from this map.
     * The map will be empty after this call returns.
     */
    public void clear() {
    	Set table = super.hashClean(gvName,idLocalize);
       for(Object key:table){    	   
    	   if (DEFAULT_JCL.deleteGlobalVar(key.toString()+"¬Map¬"+gvName)){
    		   table.remove(key);
    	   }
       }
    }

    /**
     * Returns <tt>true</tt> if this map maps one or more keys to the
     * specified value.
     *
     * @param value value whose presence in this map is to be tested
     * @return <tt>true</tt> if this map maps one or more keys to the
     *         specified value
     */
    public boolean containsValue(Object value) {
    	Set table = super.getHashSet(gvName,idLocalize);
    	for(Object k:table){
    		
			Schema<ObjectWrap> scow = RuntimeSchema.getSchema(ObjectWrap.class);
			ObjectWrap obj = scow.newMessage();
			ProtobufIOUtil.mergeFrom(((ByteBuffer)k).getArray(), obj, scow);    		
    		K key = (K)obj.getobj();
    		
    		Object valueGV = DEFAULT_JCL.getValue(key.toString()+"¬Map¬"+gvName).getCorrectResult();
    		if(value.equals(valueGV)){
    			return true;
    		}
    	}
    	return false;
    }
    
    protected Set<K> getHashSet(String gvName){
    	return super.getHashSet(gvName,idLocalize);
    }
    
    private abstract class HashIterator<E> implements Iterator<E> {
 
    	Entry<K,V> current;     // current entry
		Iterator<java.util.Map.Entry<Integer, JCL_message_generic>> intGvList;
        Queue<Entry<K,V>> queue = new ConcurrentLinkedQueue();
        Queue<Future<JCL_result>> ticket = new LinkedList<>();
        
        Map<Integer,JCL_message_generic> gvList;
        int length = 0;
        double size = 0;
        
        HashIterator(){
        	
			Set key = getHashSet(gvName);
			length = key.size();
        	gvList = JCLHashMapPacu.super.getHashQueue(queue,key,gvName);
        	intGvList = gvList.entrySet().iterator();      	
        	
        }

        public final boolean hasNext() {
        	try {
        	
        	if(queue.isEmpty()){
        		if (size==length){
        			return false;
        		}else{        			
        			if(ticket.isEmpty()){
        				
        				System.err.println("FAULT: Can't retrive all datas!!!");
        			} else{
        			
//        				JCLHashMapPacu.super.getResultBlocking(ticket.poll());
        				
							ticket.poll().get();
						
        				while(queue.isEmpty() && (!ticket.isEmpty())){            			
        					
 //       					JCLHashMapPacu.super.getResultBlocking(ticket.poll());
        					ticket.poll().get();
        				}
        			}
                	return true;
        		}
        	} else{
        		return true;
        	}
        } catch (InterruptedException | ExecutionException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
			return false;
		}
        }

        final Entry<K,V> nextEntry() {
        	current = queue.poll();
        	
        	double pag = size/(queue.size()+size);
        	if(((pag>0.4) || (gvList.size()==1)) && (intGvList.hasNext())){
    			java.util.Map.Entry<Integer, JCL_message_generic> entHost = intGvList.next();
    			ticket.add(JCLHashMapPacu.super.getHashValues(queue, entHost.getValue(), entHost.getKey()));        	
        	}
        	
        	
        	size++;
        	        	
			Schema<ObjectWrap> scow = RuntimeSchema.getSchema(ObjectWrap.class);
			ObjectWrap obj = scow.newMessage();
			ProtobufIOUtil.mergeFrom(((ByteBuffer)current.getKey()).getArray(), obj, scow);    		
    		K key = (K)obj.getobj();
 
			ProtobufIOUtil.mergeFrom((byte[])current.getValue(), obj, scow);    		
    		V value = (V)obj.getobj();
        	
        	return new implementations.util.Entry<K, V>(key,value);
//            return current;
        }

        public void remove() {     
        	JCLHashMapPacu.this.remove(current.getKey());
        }

    }

    private final class ValueIterator extends HashIterator<V> {
        public V next() {
            return nextEntry().getValue();
        }
    }

    private final class KeyIterator extends HashIterator<K> {
        public K next() {
            return nextEntry().getKey();
        }
    }

    private final class EntryIterator extends HashIterator<Map.Entry<K,V>> {
        public Map.Entry<K,V> next() {
            return nextEntry();
        }
    }

    // Subclass overrides these to alter behavior of views' iterator() method
    Iterator<K> newKeyIterator(){
        return new KeyIterator();
    }
    Iterator<V> newValueIterator()   {
        return new ValueIterator();
    }
    Iterator<Map.Entry<K,V>> newEntryIterator(){
        return new EntryIterator();
    }


    // Views

    private transient Set<Map.Entry<K,V>> entrySet = null;

    /**
     * Returns a {@link Set} view of the keys contained in this map.
     * The set is backed by the map, so changes to the map are
     * reflected in the set, and vice-versa.  If the map is modified
     * while an iteration over the set is in progress (except through
     * the iterator's own <tt>remove</tt> operation), the results of
     * the iteration are undefined.  The set supports element removal,
     * which removes the corresponding mapping from the map, via the
     * <tt>Iterator.remove</tt>, <tt>Set.remove</tt>,
     * <tt>removeAll</tt>, <tt>retainAll</tt>, and <tt>clear</tt>
     * operations.  It does not support the <tt>add</tt> or <tt>addAll</tt>
     * operations.
     */
    public Set<K> keySet(){
        Set ks = super.getHashSet(gvName,idLocalize);
        
        Set<K> retSet = new HashSet<K>();
		Schema<ObjectWrap> scow = RuntimeSchema.getSchema(ObjectWrap.class);        
		ObjectWrap obj = scow.newMessage();

		for(Object key:ks){
			ProtobufIOUtil.mergeFrom(((ByteBuffer)key).getArray(), obj, scow);    		
            retSet.add((K)obj.getobj());        	
        }
        
        return retSet;
    }

    /**
     * Returns a {@link Collection} view of the values contained in this map.
     * The collection is backed by the map, so changes to the map are
     * reflected in the collection, and vice-versa.  If the map is
     * modified while an iteration over the collection is in progress
     * (except through the iterator's own <tt>remove</tt> operation),
     * the results of the iteration are undefined.  The collection
     * supports element removal, which removes the corresponding
     * mapping from the map, via the <tt>Iterator.remove</tt>,
     * <tt>Collection.remove</tt>, <tt>removeAll</tt>,
     * <tt>retainAll</tt> and <tt>clear</tt> operations.  It does not
     * support the <tt>add</tt> or <tt>addAll</tt> operations.
     */
    public Collection<V> values() {
        Collection<V> vs = new Values();
        return vs;
    }

    private final class Values extends AbstractCollection<V> {
        public Iterator<V> iterator() {
            return newValueIterator();
        }
        public int size(){       	
            return JCLHashMapPacu.this.size();
        }
        public boolean contains(Object o) {
            return containsValue(o);
        }
        public void clear() {
        	JCLHashMapPacu.this.clear();
        }
    }

    /**
     * Returns a {@link Set} view of the mappings contained in this map.
     * The set is backed by the map, so changes to the map are
     * reflected in the set, and vice-versa.  If the map is modified
     * while an iteration over the set is in progress (except through
     * the iterator's own <tt>remove</tt> operation, or through the
     * <tt>setValue</tt> operation on a map entry returned by the
     * iterator) the results of the iteration are undefined.  The set
     * supports element removal, which removes the corresponding
     * mapping from the map, via the <tt>Iterator.remove</tt>,
     * <tt>Set.remove</tt>, <tt>removeAll</tt>, <tt>retainAll</tt> and
     * <tt>clear</tt> operations.  It does not support the
     * <tt>add</tt> or <tt>addAll</tt> operations.
     *
     * @return a set view of the mappings contained in this map
     */
    public Set<Map.Entry<K,V>> entrySet() {
        return entrySet0();
    }

    private Set<Map.Entry<K,V>> entrySet0() {
        Set<Map.Entry<K,V>> es = entrySet;
        return es != null ? es : (entrySet = new EntrySet());
    }

    private final class EntrySet extends AbstractSet<Map.Entry<K,V>> {
        public Iterator<Map.Entry<K,V>> iterator() {
            return newEntryIterator();
        }
        public boolean contains(Object o) {
            if (!(o instanceof Map.Entry))
                return false;
            Map.Entry<K,V> e = (Map.Entry<K,V>) o;
            Entry<K,V> candidate = getEntry(e.getKey());
            return candidate != null && candidate.equals(e);
        }
        public boolean remove(Object o) {
            return (JCLHashMapPacu.this.remove(o)!=null);
        }
        
        public int size() {
            return JCLHashMapPacu.this.size();
        }
        
        public void clear() {
        	JCLHashMapPacu.this.clear();
        }
    }
    
    public static void destroy(){
    	if(DEFAULT_JCL!=null){
    	DEFAULT_JCL.destroy();
    	}else{
    		System.exit(0);
    	}
    }
}

