/*
 * Copyright (c) 2013, OpenCloudDB/MyCAT and/or its affiliates. All rights reserved.
 * DO NOT ALTER OR REMOVE COPYRIGHT NOTICES OR THIS FILE HEADER.
 *
 * This code is free software;Designed and Developed mainly by many Chinese 
 * opensource volunteers. you can redistribute it and/or modify it under the 
 * terms of the GNU General Public License version 2 only, as published by the
 * Free Software Foundation.
 *
 * This code is distributed in the hope that it will be useful, but WITHOUT
 * ANY WARRANTY; without even the implied warranty of MERCHANTABILITY or
 * FITNESS FOR A PARTICULAR PURPOSE.  See the GNU General Public License
 * version 2 for more details (a copy is included in the LICENSE file that
 * accompanied this code).
 *
 * You should have received a copy of the GNU General Public License version
 * 2 along with this work; if not, write to the Free Software Foundation,
 * Inc., 51 Franklin St, Fifth Floor, Boston, MA 02110-1301 USA.
 * 
 * Any questions about this component can be directed to it's project Web address 
 * https://code.google.com/p/opencloudb/.
 *
 */
package org.opencloudb.route.function;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import java.util.SortedMap;
import java.util.TreeMap;

import org.opencloudb.config.model.rule.RuleAlgorithm;
import org.opencloudb.exception.MurmurHashException;

import com.google.common.hash.HashFunction;
import com.google.common.hash.Hashing;

/**
 * consistancy hash, murmur hash
 * implemented by Guava
 * @author wuzhih
 *
 */
public class PartitionByMurmurHash extends AbstractPartitionAlgorithm implements RuleAlgorithm  {

	private static final int DEFAULT_VIRTUAL_BUCKET_TIMES=160;
	private static final int DEFAULT_WEIGHT=1;
	private static final Charset DEFAULT_CHARSET = StandardCharsets.UTF_8;
	
	private int seed;
	private int count;
	private int virtualBucketTimes=DEFAULT_VIRTUAL_BUCKET_TIMES;
	private Map<Integer,Integer> weightMap=new HashMap<>();
	
	private HashFunction hash;
	
	private SortedMap<Integer,Integer> bucketMap;
	@Override
	public void init()  {
		try{
			bucketMap=new TreeMap<>();
			generateBucketMap();
		}catch(Exception e){
			throw new MurmurHashException(e);
		}
	}

	private void generateBucketMap(){
		hash=Hashing.murmur3_32(seed);//计算一致性哈希的对象
		for(int i=0;i<count;i++){//构造一致性哈希环，用TreeMap表示
			StringBuilder hashName=new StringBuilder("SHARD-").append(i);
			for(int n=0,shard=virtualBucketTimes*getWeight(i);n<shard;n++){
				bucketMap.put(hash.hashUnencodedChars(hashName.append("-NODE-").append(n)).asInt(),i);
			}
		}
		weightMap=null;
	}

	/**
	 * 得到桶的权重，桶就是实际存储数据的DB实例
	 * 从0开始的桶编号为key，权重为值，权重默认为1。
	 * 键值必须都是整数
	 * @param bucket
	 * @return
	 */
	private int getWeight(int bucket){
		Integer w=weightMap.get(bucket);
		if(w==null){
			w=DEFAULT_WEIGHT;
		}
		return w;
	}
	/**
	 * 创建murmur_hash对象的种子，默认0
	 * @param seed
	 */
	public void setSeed(int seed){
		this.seed=seed;
	}
	/**
	 * 节点的数量
	 * @param count
	 */
	public void setCount(int count) {
		this.count = count;
	}
	/**
	 * 虚拟节点倍数，virtualBucketTimes*count就是虚拟结点数量
	 * @param virtualBucketTimes
	 */
	public void setVirtualBucketTimes(int virtualBucketTimes){
		this.virtualBucketTimes=virtualBucketTimes;
	}
	/**
	 * 节点的权重，没有指定权重的节点默认是1。以properties文件的格式填写，以从0开始到count-1的整数值也就是节点索引为key，以节点权重值为值。
	 * 所有权重值必须是正整数，否则以1代替
	 * @param weightMapPath
	 * @throws IOException 
	 * @throws  
	 */
	public void setWeightMapFile(String weightMapPath) throws IOException{
		Properties props=new Properties();
		try(BufferedReader reader=new BufferedReader(new InputStreamReader(this.getClass().getClassLoader().getResourceAsStream(weightMapPath), DEFAULT_CHARSET))){
			props.load(reader);
			for(Map.Entry entry:props.entrySet()){
				int weight=Integer.parseInt(entry.getValue().toString());
				weightMap.put(Integer.parseInt(entry.getKey().toString()), weight>0?weight:1);
			}
		}
	}

	@Override
	public Integer calculate(String columnValue) {
		SortedMap<Integer, Integer> tail = bucketMap.tailMap(hash.hashUnencodedChars(columnValue).asInt());
		if (tail.isEmpty()) {
		    return bucketMap.get(bucketMap.firstKey());
		}
		return tail.get(tail.firstKey());
	}

}
