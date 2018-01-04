package func;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;

import java.util.ArrayList;
import java.util.List;

import net.sf.json.JSONArray;
import net.sf.json.JSONObject;
import struct.ChunkInfo;

public class Slave {

	
	public  List<ChunkInfo> chunkInfoList=new ArrayList<ChunkInfo>();
	public static final String CHUNK_LOG = "E:\\testlog.txt"; // *1024;
	public static final int CHUNK_SIZE = 64*1024; // *1024;
	public static final int UPLOAD_BUFFER_SIZE = 8*1024;
	public static final int DOWNLOAD_BUFFER_SIZE = 8*1024;
	
	public Slave(){};
	public void IniSalve() throws IOException{
		String result=null;
		String filePath="E:\\testlog.txt";	
		File fileName = new File(filePath);
		if(fileName.exists()){  
		      FileReader fileReader=null;  
		      BufferedReader bufferedReader=null;  
		      try{  
		          fileReader=new FileReader(fileName);  
		          bufferedReader=new BufferedReader(fileReader);  
		          try{  
		              String read="";  
		              while((read=bufferedReader.readLine())!=null){  
		              System.out.println("line£º"+"\r\n"+read);  
		              String[] Info=read.split(" ");
		              ChunkInfo chunkInfo=new ChunkInfo(Integer.valueOf(Info[0]),Info[1],Integer.valueOf(Info[2]),Integer.valueOf(Info[3]),Integer.valueOf(Info[4]));
		             chunkInfoList.add(chunkInfo);
		            }
		              }catch(Exception e){  
		           e.printStackTrace();  
		          }
		          }catch(Exception e){  
		         e.printStackTrace();  
		         }finally{  
		           if(bufferedReader!=null){  
		              bufferedReader.close();  
		         }  
		          if(fileReader!=null){  
		             fileReader.close();  
		        }  
		     }  	  
		  }
	}

	
	public void CreateChunk(JSONObject chunk) throws Exception{
		
		ChunkInfo chunkInfo=new ChunkInfo(chunk.getInt("chunk_id"), chunk.getString("slave_ip"),
				chunk.getInt("port"), chunk.getInt("file_index"), chunk.getInt("chunk_left"));
		chunkInfoList.add(chunkInfo);
		this.writeChunkLog(chunkInfo);
	}
	
	
	public JSONArray getChunkList(){
		JSONArray chunkArray = new JSONArray();
		for(int i=0;i<chunkInfoList.size();i++){
			
		    ChunkInfo chunkInfo=chunkInfoList.get(i);
			JSONObject obj = new JSONObject();
			obj.put("chunk_id", chunkInfo.chunkId);
			obj.put("slave_ip", chunkInfo.slaveIP);
			obj.put("port", chunkInfo.port);
			obj.put("file_index", chunkInfo.fileIndex);
			obj.put("chunk_left", chunkInfo.chunkLeft);
			chunkArray.add(obj);
			
			//String Info=chunkInfo.chunkId+" "+chunkInfo.slaveIP+" "+chunkInfo.port+" "+chunkInfo.fileIndex+" "+chunkInfo.chunkLeft+"\n";
			//System.out.println(Info);
		}
		return chunkArray;
	}
	
	
	public void deleteChunk(int chunkid) throws Exception{
		for(int i=0;i<chunkInfoList.size();i++){
			ChunkInfo chunkInfo=chunkInfoList.get(i);
			if (chunkInfo.chunkId==chunkid){
				chunkInfoList.remove(i);
				break;//////////////////////
			}
		}
		for(int i=0;i<chunkInfoList.size();i++){
			ChunkInfo chunkInfo=chunkInfoList.get(i);			
			 File fileName = new File(CHUNK_LOG);
			   if(fileName.exists()){ 
				   fileName.delete();
			   }
			this.writeChunkLog(chunkInfo);
		}
		
	}
	public byte[] readChunk(int chunkid,int offset,int readLen) throws IOException{
		byte[] buffer = new byte[(int) DOWNLOAD_BUFFER_SIZE]; 
		for(int i=0;i<chunkInfoList.size();i++){
			ChunkInfo chunkInfo=chunkInfoList.get(i);
			if (chunkInfo.chunkId==chunkid){
				String result=null;
				String contentPath="E:\\content"+Integer.toString(chunkid);
				File fileName = new File(contentPath);
				if(fileName.exists()){  
					FileInputStream file = new FileInputStream(contentPath);  
			         			        
			        int numRead = 0; 
			        	        
			        
			        while (offset < buffer.length  
			        && (numRead = file.read(buffer, readLen, buffer.length - readLen)) >= 0) {  
			            offset += numRead;  
			        }  		         
			          
			        file.close();  
			        
				}
				break;
			}
		}
		return buffer;  
	}
	
	
	public  boolean writeChunk(int chunkid,int offset,int writeLen,String content){
		
		String contentPath="E:\\content"+Integer.toString(chunkid);
		try{  
			  File fileName = new File(contentPath);
		   if(!fileName.exists()){  
		    fileName.createNewFile();  
		    System.out.println("CreateFile"+contentPath);
		   }  
		   
		   BufferedWriter output = new BufferedWriter(new FileWriter(fileName,true));  
         		   
		   output.write(content);  
           output.close(); 
		   
		   
		  }catch(Exception e){  
		   e.printStackTrace();  
		  }  
		for(int i=0;i<chunkInfoList.size();i++){
			ChunkInfo chunkInfo=chunkInfoList.get(i);
			if (chunkInfo.chunkId==chunkid){
				chunkInfo.fileIndex=CHUNK_SIZE-chunkInfo.chunkLeft;
				chunkInfo.chunkLeft-=writeLen;
				
				return true;
			}
			return false;
		}
		return false;
	}
	
	public  void writeChunkLog(ChunkInfo chunkInfo)throws Exception{  
		 
		  try{  
			  File fileName = new File(CHUNK_LOG);
		   if(!fileName.exists()){  
		    fileName.createNewFile();  
		    System.out.println("CreateFile"+filePath);
		   }  
		   
		   BufferedWriter output = new BufferedWriter(new FileWriter(fileName,true));  
           
		   String Info=chunkInfo.chunkId+" "+chunkInfo.slaveIP+" "+chunkInfo.port+" "+chunkInfo.fileIndex+" "+chunkInfo.chunkLeft+"\n";
		   
		   output.write(Info);  
           output.close(); 
		   
		   
		  }catch(Exception e){  
		   e.printStackTrace();  
		  }  
		  
		  
		 }   
	
	
	
	
	
}
