package socket;

import java.io.BufferedReader;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.net.ServerSocket;
import java.net.Socket;

import func.Slave;
import net.sf.json.JSONArray;
import net.sf.json.JSONObject;
import struct.VSFProtocols;

public class SlaveServer {
	private static ServerSocket server;
	
	/**
	* 初始化socket服务
	*/
	public static String signObj=null;
	private static Slave slave=new Slave();
	public static void main(String[] args) throws IOException {    
		SlaveServer slaveServer=new SlaveServer();
		slave.IniSalve();
		PushBlockQueue.getInstance().start();
		slaveServer.initServer();
	}
	
	
	public void initServer() {
	try {
	// 创建一个ServerSocket在端口8080监听客户请求
	server = new ServerSocket(8888);
	createMessage();
	} catch (IOException e) {
	// TODO Auto-generated catch block
	e.printStackTrace();
	}
	}
	/**
	* 创建消息管理，一直接收消息
	*/
	private void createMessage() {
	try {
		System.out.println("waiting for client : ");
		// 使用accept()阻塞等待客户请求
		Socket socket = server.accept();
		System.out.println("client socketPort : " + socket.getPort());
		// 开启一个子线程来等待另外的socket加入
		new Thread(new Runnable() {
			public void run() {
			    createMessage();
		}
	}).start();
	// 向客户端发送信息
	OutputStream output = socket.getOutputStream();
	// 从客户端获取信息
	
	BufferedReader bff = new BufferedReader(new InputStreamReader(socket.getInputStream())); 
    	// Scanner scanner = new Scanner(socket.getInputStream());

		String line = "";
		// accept this socket message
		while (true) {
			//Thread.sleep(500);
			
			while ((line = bff.readLine()) != null) {		
			  
			   if(line.equals(String.valueOf(VSFProtocols.WRITE_CHUNK))||line.equals(String.valueOf(VSFProtocols.READ_CHUNK))){
				   System.out.println("put");
				   PushBlockQueue.getInstance().put(line+":"+socket.getPort());
			   }else{
				   
				   if(line.equals(String.valueOf(1002))){ //master   
					  
					   switch (Integer.valueOf(line)) {
		                case 1002://chunkinfo
		                    getChunkInfo(socket);
		                    break;

		                case 1003://createchunk
		                    createChunk(socket);
		                    break;	
		                case 1004:
		                	deleteChunk(socket);
		                	break;
		                }					   
					   
				   }else{
				   
					   responesClient(socket,"Please wait...");
				   }
			   }
			   Thread.sleep(500);
			   System.out.println("context signObj :"+line+" "+signObj);
			   if(signObj.equals(line+":"+socket.getPort())){
				    System.out.println("to do obj");
				   
				    responesClient(socket,"OK");
	        		String[] Info=signObj.split(":");
	        		switch (Integer.valueOf(Info[0])) {
	                case VSFProtocols.WRITE_CHUNK:
	                    writeChunk(socket);
	                    break;

	                case VSFProtocols.READ_CHUNK:
	                    readChunk(socket);
	                    break;
	                
	                }
				   
			   }
			   
			}
		}
		// server.close();
		} catch (Exception e) {
		// TODO Auto-generated catch block
		e.printStackTrace();
		System.out.println("error : " + e.getMessage());
		}
		}
	
	public void writeChunk(Socket socket) throws IOException{
		  
            
            DataInputStream input = new DataInputStream(socket.getInputStream());  
            
            
            	  System.out.println("write Chunk"); 
            	  byte[] chunkids = new byte[64];
            	  input.read(chunkids, 0, 64);
            	  String chunkid = new String(chunkids);//chunk_id
            	  
            	  byte[] offsetBuffs = new byte[64];
            	  input.read(offsetBuffs, 0, 64);
            	  String offsetBuff = new String(offsetBuffs);//offset
            	  
            	  byte[] lenBuffs = new byte[64];
            	  input.read(lenBuffs, 0, 64);
            	  String lenBuff = new String(lenBuffs);//writelen
            	  String contentBuff="";
            	 
            	  //content
            	  int contentCount = 0;
            	  byte[] tempBuf = new byte[Slave.UPLOAD_BUFFER_SIZE];
              	
              	  while(true){
              		if(contentCount >= Integer.valueOf(lenBuff)){
              			break;
              		}
              		int cRead = Math.min(Integer.valueOf(lenBuff) - contentCount, tempBuf.length);
              		int aRead = input.read(tempBuf, 0, cRead);
              		
    	
              			contentBuff+= new String(tempBuf);
              		
              		contentCount += aRead;
              	}
            	
            	  
                  boolean stateWruteChunk=slave.writeChunk(Integer.valueOf(chunkid),Integer.valueOf(offsetBuff),Integer.valueOf(lenBuff),contentBuff);
                  DataOutputStream out = new DataOutputStream(socket.getOutputStream());    
                  
                  if(stateWruteChunk){
                	  out.writeUTF("write Chunk successfully"); 
                     
                  }else{
                	  out.writeUTF("write Chunk error"); 
                  }
                  out.close();
	
	}

    public void readChunk(Socket socket) throws NumberFormatException, IOException{
    	  DataInputStream input = new DataInputStream(socket.getInputStream());  
          
    	  System.out.println("read chunk");               	  
          String chunkid = input.readUTF();//chunk_id
	      String offsetBuff = input.readUTF();//offset
	      String lenBuff = input.readUTF();//readlen
	      byte[] content=slave.readChunk(Integer.valueOf(chunkid),Integer.valueOf(offsetBuff),Integer.valueOf(lenBuff));
	    
	      
	      responesClient(socket,"OK");
        
            OutputStream out = socket.getOutputStream();
			int bufferSize = Slave.UPLOAD_BUFFER_SIZE;
			byte[] contentBuff = new byte[bufferSize];
			int contentCount = 0;
			while(contentCount < Integer.valueOf(lenBuff)){
				int writeNum = Math.min(bufferSize, Integer.valueOf(lenBuff)-contentCount);
				for(int i = 0; i < bufferSize; ++i){
					contentBuff[i] = content[contentCount+i];
				}
				out.write(contentBuff, 0, writeNum);
				out.flush();
				
				contentCount += writeNum;
			}   
             
	     
	        out.close(); 
    }
    
    public void getChunkInfo(Socket socket) throws IOException{
    	OutputStream out = socket.getOutputStream();
    	JSONArray chunkarray=slave.getChunkList();
    	String string=chunkarray.toString();
        
        byte[] bytes=string.getBytes();
       
        out.write(bytes);
        out.flush();
        out.close();
    }
    
    public void deleteChunk(Socket socket) throws NumberFormatException, Exception{
    	//doing
    	BufferedReader in = new BufferedReader(new InputStreamReader(socket.getInputStream()));
    	slave.deleteChunk(Integer.valueOf(in.readLine()));
    	responesClient(socket,"1004");
    }
    
    public void createChunk(Socket socket) throws IOException{
    	OutputStream out = socket.getOutputStream();
    	BufferedReader in = new BufferedReader(new InputStreamReader(socket.getInputStream()));
		JSONObject chunk = new JSONObject(in.readLine());////================error
		slave.CreateChunk(chunk);
		
        String string=chunk.toString();
        
        byte[] bytes=string.getBytes();
       
        out.write(bytes);
        out.flush();
        out.close();
    }
    
    public void responesClient(Socket socket,String content) throws IOException{
    	 OutputStream out = socket.getOutputStream();
 		
 		// response to client	        		
 		byte[] response = ("content".getBytes());	        		
 		out.write(response, 0, response.length);
 		out.flush();
 		System.out.println("response to client: " + response);
 		out.close();
    }
}
