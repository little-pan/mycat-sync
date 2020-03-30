package org.opencloudb.cache.impl;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;

import org.opencloudb.cache.CachePool;
import org.opencloudb.cache.CacheStatic;
import org.iq80.leveldb.DB;
import org.slf4j.*;


public class LevelDBPool implements CachePool {

	private static final Logger log = LoggerFactory.getLogger(LevelDBPool.class);

	private final DB cache;
	private final CacheStatic cacheStati = new CacheStatic();
    private final String name;
    private final long maxSize;
    
	public LevelDBPool(String name, DB db, long maxSize) {
		this.cache = db;
		this.name = name;
		this.maxSize = maxSize;
		cacheStati.setMaxSize(maxSize);
	}

	@Override
	public void putIfAbsent(Object key, Object value) {
		try {
			cache.put(toByteArray(key), toByteArray(value));
		} catch (IOException e) {
			log.warn("'"+this.name+"' can't add leveldb cache, key: "+key+", value: "+value+"", e);
			return;
		}
		cacheStati.incPutTimes();
		log.debug("'{}' add leveldb cache, key: {}, value: {}", this.name, key, value);
	}

	@Override
	public Object get(Object key) {
		Object ob;

		try {
			byte[] val = cache.get(toByteArray(key));
			ob = toObject(val);
		} catch (Exception e) {
			log.warn("'"+this.name+"' can't hit leveldb cache, key: "+key, e);
			return null;
		}

		if (ob != null) {
			log.debug("'{}' hit leveldb cache, key: {}", this.name, key);
			cacheStati.incHitTimes();
			return ob;
		} else {
			log.debug("'{}' miss leveldb cache, key: {}", this.name, key);
			cacheStati.incAccessTimes();
			return null;
		}
	}

	@Override
	public void clearCache() {
		log.info("clear leveldb cache '{}'", this.name);
		cacheStati.reset();
	}

	@Override
	public CacheStatic getCacheStatic() {
		cacheStati.setItemSize(cacheStati.getPutTimes());
		return cacheStati;
	}

	@Override
	public long getMaxSize() {
		return maxSize;
	}
	
    private byte[] toByteArray (Object obj) throws IOException {
        ByteArrayOutputStream bos = new ByteArrayOutputStream();
		ObjectOutputStream oos = new ObjectOutputStream(bos);
		oos.writeObject(obj);
		oos.flush();
		oos.close();
		byte[] bytes = bos.toByteArray();
		bos.close();

        return bytes;      
    }     
         
        
    private Object toObject (byte[] bytes) throws IOException, ClassNotFoundException {
        Object obj = null;   
        if ((bytes == null) || (bytes.length <= 0)) {
        	return obj;
        }

		ByteArrayInputStream bis = new ByteArrayInputStream (bytes);
		ObjectInputStream ois = new ObjectInputStream(bis);
		obj = ois.readObject();
		ois.close();
		bis.close();

        return obj;      
    } 

}
