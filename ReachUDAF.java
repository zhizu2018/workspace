/**
 * 
 */
package com.wafersystems.wificloud.ana.udaf;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.spark.sql.Row;
import org.apache.spark.sql.expressions.MutableAggregationBuffer;
import org.apache.spark.sql.expressions.UserDefinedAggregateFunction;
import org.apache.spark.sql.types.DataType;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;

import com.alibaba.fastjson.JSONObject;

import scala.collection.JavaConverters;

/**
 * @author wisdom
 *
 */
public class ReachUDAF extends UserDefinedAggregateFunction implements Serializable  {
	
    @Override
    public StructType inputSchema() {
    	List<StructField> inputFields = new ArrayList<StructField>();
    	StructField inputStructField1 = DataTypes.createStructField("reach_url",DataTypes.StringType, true);
    	inputFields.add(inputStructField1);
    	return DataTypes.createStructType(inputFields);
    }

    @Override
    public StructType bufferSchema() {
    	List<StructField> bufferFields = new ArrayList<StructField>();
    	StructField bufferStructField1 = DataTypes.createStructField("reach_url",DataTypes.StringType, true);
    	bufferFields.add(bufferStructField1);
    	StructField bufferStructField5 = DataTypes.createStructField("adreach",DataTypes.StringType, true);
    	bufferFields.add(bufferStructField5);
    	return DataTypes.createStructType(bufferFields);
    }

    @Override
    public DataType dataType() {
		return DataTypes.StringType;
    }

    @Override
    public boolean deterministic() {
        return true;
    }

    @Override
    public void initialize(MutableAggregationBuffer buffer) {
    	buffer.update(0,"");
    	buffer.update(1,"");
    }

    @Override
    public void update(MutableAggregationBuffer buffer, Row input) {
    	Map<String,String> map = new HashMap<String,String>();
    	map.put("reach_url", input.get(0).toString());
    	map.put("count", "1");
    	List<Map<String,String>> list = new ArrayList<Map<String,String>>();
       if (buffer.getString(1).isEmpty()){
    	   list.add(map);
    	   buffer.update(1,JSONObject.toJSONString(list));
    	   
       }else{
    	   List<HashMap> bufferList =  JSONObject.parseArray(buffer.getString(1), HashMap.class);
    	   List<Map<String,String>> newlist = new ArrayList<Map<String,String>>();
    	   int i=0;
    		for (Map<String, String> map1 : bufferList){
    		   Map<String,String> maptem = map1;
    		   if (map.get("reach_url").equals(maptem.get("reach_url"))){
        		   Map<String,String> mapNew = new HashMap<String,String>();
    			   mapNew.put("count",String.valueOf(Integer.valueOf(map.get("count"))+Integer.valueOf(maptem.get("count"))) );
    			   mapNew.put("reach_url", input.get(0).toString());
    			   newlist.add(mapNew);
    		   }else {
    			   i++;
    			   newlist.add(maptem);
    		   }
    	   }
    	if(i==bufferList.size()){
     	   newlist.add(map);	
    	}
    	buffer.update(1, JSONObject.toJSONString(newlist));
       } 
    }

    @Override
    public void merge(MutableAggregationBuffer buffer1, Row buffer2) {
    	 List<HashMap> bufferList1 =  JSONObject.parseArray(buffer1.getString(1), HashMap.class);
    	 List<HashMap> bufferList2 =  JSONObject.parseArray(buffer2.getString(1), HashMap.class);
    	 if (bufferList1==null||bufferList1.isEmpty()) {
    		 buffer1.update(1,buffer2.getString(1)); 
    	 }else {
    		 List<Map<String,String>> newlist = new ArrayList<Map<String,String>>();
    		 Map<String,Integer> map1 = new HashMap<>();
    		 for (Map<String, String> map : bufferList1){
    			 Map<String,String> maptem = map;
    			 map1.put(maptem.get("reach_url"), Integer.valueOf(maptem.get("count")));
    		 }
    		 Map<String,Integer> map2 = new HashMap<>();
    		 for(Map<String, String> map : bufferList2) {
    			 Map<String,String> maptem = map;
    			 map2.put(maptem.get("reach_url"), Integer.valueOf(maptem.get("count")));
    		 }
    		 for(Map<String, String> map : bufferList1) {
    			 Map<String,String> maptem = map;
    			if (map2.get(maptem.get("reach_url"))!=null) {
    				Map<String,String> mapNew = new HashMap<String,String>();
	    			   mapNew.put("count",String.valueOf(Integer.valueOf(map2.get(maptem.get("reach_url"))+Integer.valueOf(maptem.get("count")))));
	    			   mapNew.put("reach_url", maptem.get("reach_url"));
	    			   newlist.add(mapNew);
    			}else {
    				newlist.add(maptem);
    			}
    		 }
    		 for(Map<String, String> map : bufferList2) {
    			 Map<String,String> maptem = map;
    			if (map1.get(maptem.get("reach_url"))!=null) {
    				
    			}else {
    				newlist.add(maptem);
    			}
    		 }
    		 
    		 
    		 buffer1.update(1,JSONObject.toJSONString(newlist)); 
    		 
    		 
    	 }
    	 
    	
       
    }
    @Override
    public Object evaluate(Row buffer) {
        return buffer.getString(1);
    }
}
