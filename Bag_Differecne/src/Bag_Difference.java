import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.CharBuffer;
import java.nio.channels.FileChannel;
import java.nio.charset.Charset;
import java.nio.charset.CharsetDecoder;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.EnumSet;
import java.util.List;
import java.util.Map.Entry;
import java.util.Scanner;

public class Bag_Difference {
	static int  Block_Size=4096;
    static int  Relations_Size=0;
    static float  MainMemory_Size=0;
    static int  Tuple_Size=100;
    static int  subListSize=(int)(MainMemory_Size/Tuple_Size);
    static int  runTime=(int)Math.ceil(((float)Relations_Size/(float)subListSize));
    static int  outputBufferSize=8192;
	public static void main(String[] args) throws IOException {
		int IOS1=0,IOS2=0,IOD=0,size1,size2,flag=1;
		float temp;
		System.out.println("please choose main memory: 0--5M, 1--10M, default main memory is 10M");
		Scanner sc=new Scanner(System.in);
		flag=sc.nextInt();
		sc.nextInt();
		if(flag==0){
			temp=(float)1.1;
			System.out.println("choose main memory 5M, the sub-Relations size is 1.1M");
		}
		else{
			temp=(float)2.1;
			System.out.println("choose main memory 10M, the sub-Relations size is 2.1M");
		}
		/*System.out.println("please enter the sub-Relations size(M): ");
		
		temp=sc.nextFloat();
		sc.nextLine();*/
		MainMemory_Size=1024*1024*temp;
		subListSize=(int)(MainMemory_Size/Tuple_Size);
		
		long currenTime1, currenTime2,currenTime3;
		int Total_Number=0;
		String name1="//T1.txt", name2="//T2.txt";
		System.out.println("doing phase1 for T1, please wait");
		currenTime1=System.currentTimeMillis();
	    Phase1(name1);
	    size1=Relations_Size;
	    currenTime2=System.currentTimeMillis();
	    System.out.println("Sorting time of T1 phase1(sec):"+(currenTime2-currenTime1)/1000); 
	    System.out.println("doing phase2 for T1, please wait");
	    Phase2("//outputT1.txt");
	    currenTime3=System.currentTimeMillis();
		System.out.println("merge time of T1 phase2(sec):"+(currenTime3-currenTime2)/1000); 
		System.out.println("doing phase1 for T2, please wait");
		Phase1(name2);
		size2=Relations_Size;
	    currenTime1=System.currentTimeMillis();
	    System.out.println("Sorting time of T2 phase1(sec):"+(currenTime1-currenTime3)/1000); 
	    System.out.println("doing phase2 for T2, please wait");
	    Phase2("//outputT2.txt");
	    currenTime2=System.currentTimeMillis();
		System.out.println("merge time of T2 phase2(sec):"+(currenTime2-currenTime1)/1000); 
		System.out.println("doing bag difference for T1 and T2, please wait");
		Total_Number=dealBD("//mergeoutputT1.txt","//mergeoutputT2.txt");
	    currenTime3=System.currentTimeMillis();
	    System.out.println("compare time of bd(T1,T2)(sec):"+(currenTime3-currenTime2)/1000);
	    System.out.println("the total number of tuples is : "+Total_Number);
	    System.out.println("the number of block used to store output on disk: "+(int)Math.ceil(Total_Number/40));
	    IOD=(int)Math.ceil(Total_Number/40+size1/40+size2/40);
	    IOS1=(int)Math.ceil(size1/10);
	    IOS2=(int)Math.ceil(size2/10);
	    System.out.println("the IO operation in sorting of T1 is: "+IOS1);
	    System.out.println("the IO operation in sorting of T2 is:" +IOS2);
	    System.out.println("the total IO opertion in sorting is: "+(IOS1+IOS2+IOD));
	    
	}
	
	
	public static void Phase1(String textName ) throws IOException{
		
		BufferedReader reader = new BufferedReader(new FileReader(System.getProperty("user.dir")+textName)); 
		String outputName="//output"+textName.substring(2);
		BufferedWriter bw=new BufferedWriter(new FileWriter(System.getProperty("user.dir")+outputName)); 
		String line;
		
		Relations_Size=0;
	     //int TN=MainMemory_Size/Tuple_Size,RT=Relations_Size/TN+1;
	     
	    
	    //ArrayList<String> t=new ArrayList<String>();
	    while(true){
	    	ArrayList<String> tupleList=new ArrayList<String>();
	    	
	    	while((line=reader.readLine())!=null){
            tupleList.add(String.format("%-100s", line.trim()));
            if(tupleList.size()>=subListSize)
	    		break;
	    }
	    	
	   
	    	
	    	Collections.sort(tupleList, new Comparator<String>() {

	        @Override
	        public int compare( String o1, String o2) {
	        	return o1.compareTo(o2);
	      
	        }

			
	    });
	    	//System.out.println(tupleList.size());
	    	
	    int l=0;
	    String wl;
	    for(l=0;l<tupleList.size();l++){
	    	wl=tupleList.get(l);
	    	 bw.write(wl);
	    	 Relations_Size++;
			 //bw.newLine();
	    }
	    
	    
	    
	     if(line==null)
	    	 break;
	    }
	    
	    reader.close();
	    bw.close();
	   
}
public static void Phase2(String textName)  throws IOException{
	
	FileChannel fc= FileChannel.open(Paths.get(System.getProperty("user.dir")+"\\"+textName),EnumSet.of(StandardOpenOption.READ));
	String outputName="//merge"+textName.substring(2);
	BufferedWriter bw=new BufferedWriter(new FileWriter(System.getProperty("user.dir")+outputName));
	ByteBuffer rb= ByteBuffer.allocate(Tuple_Size);
	ArrayList<SubRelations> mergeList= new ArrayList<SubRelations>();
	runTime=(int)Math.ceil(((float)Relations_Size/(float)subListSize));
	int i=0;	
	
		for( i=0; i<runTime;i++){ // initialize 
			SubRelations sr=new SubRelations();
			sr.subSize=subListSize;
			sr.Relations_Num=i;
			fc.read(rb, (i*subListSize+sr.First_Link)*Tuple_Size);
			rb.flip();
			
			sr.insideTuple=new String(rb.array(),0,100);
			mergeList.add(sr);
		}
		
		mergeList.get(runTime-1).subSize=Relations_Size-(runTime-1)*subListSize;
		//System.out.println(mergeList.get(8).insideTuple);
		ArrayList<String> buffer=new ArrayList<String>();
		long recordFirst=0;
		int l=0,recordLoc=0;
		String w;
		while(true){
			   if(mergeList.size()==0){
				   if(buffer.size()!=0){
				   for(l=0;l<buffer.size();l++){
		    		   w=buffer.get(l);
		    		   bw.write(w.trim());
		    		   bw.newLine();
		    	   }
		    	   buffer.clear();
		    	 
				   }
				   break;
			   }
			 
		       
			   Collections.sort(mergeList, new Comparator<SubRelations>() {

		        @Override
		        public int compare( SubRelations o1, SubRelations o2) {
		        	//System.out.println(o1.insideTuple);
		        	return o2.insideTuple.compareTo(o1.insideTuple);
		        	
		        }

				
		    });
		       
		       recordLoc=mergeList.get(mergeList.size()-1).Relations_Num;
		       mergeList.get(mergeList.size()-1).First_Link++;
		       recordFirst=mergeList.get(mergeList.size()-1).First_Link;
		       
		       buffer.add(mergeList.get(mergeList.size()-1).insideTuple);
		       if(mergeList.get(mergeList.size()-1).First_Link>=mergeList.get(mergeList.size()-1).subSize){
		    	   
		    	   mergeList.remove(mergeList.size()-1);
		    	 
		    	     continue;
		       }
		       if(buffer.size()==outputBufferSize){
		    	   for(l=0;l<buffer.size();l++){
		    		   w=buffer.get(l);
		    		   bw.write(w.trim());
		    		   bw.newLine();
		    	   }
		    	  
		    	   buffer.clear();
		       }
		       fc.read(rb, (recordLoc*subListSize+recordFirst)*Tuple_Size);
		       mergeList.get(mergeList.size()-1).insideTuple=new String(rb.array(),0,100);
		       rb.flip();
		      
		      // System.out.println("POS="+(recordLoc*subListSize+recordFirst)*Tuple_Size+"       read="+mergeList.get(mergeList.size()-1).insideTuple);
		       
		       
		       
		}
			fc.close();
			bw.close();
			
			}
			

	
public static int dealBD(String name1, String name2) throws IOException{
	
	BufferedReader reader1=new BufferedReader(new FileReader(System.getProperty("user.dir")+name1));
	BufferedReader reader2=new BufferedReader(new FileReader(System.getProperty("user.dir")+name2));
	BufferedWriter  bw=new BufferedWriter(new FileWriter(System.getProperty("user.dir")+"//finaloutput.txt"));
	String line1,line2,preline1,preline2,w;
	int key1=0,key2=0,totalnum=0,l,i=0;
	int count1=1,count2=1;
	line1=reader1.readLine();
	line2=reader2.readLine();
	preline1=line1;
	preline2=line2;
	line1=reader1.readLine();
	key1=Integer.parseInt(preline1.substring(0,8));
	key2=Integer.parseInt(preline2.substring(0,8));
	ArrayList<String> buffer=new ArrayList<String>();
	while(true){
		
		if(buffer.size()==outputBufferSize||line1==null||line2==null){
			 for(l=0;l<buffer.size();l++){
	    		   w=buffer.get(l);
	    		   bw.write(w);
	    		   bw.newLine();
	    		   totalnum++;
	    	   }
	    	   buffer.clear();
	       
		}
		if(preline1!=null&&preline2!=null){
		
			
		if(preline1.equals(line1)){
			line1=reader1.readLine();
			count1++;
			
			continue;
			
		}
		
		else{
			if(key1<key2){
				if(preline1!=null){
				buffer.add(String.format("(%05d), ",count1)+preline1);
				
				
				
			}
			}
			else if(key1==key2){
				
				/*while(true){
					line2=reader2.readLine();
					if(line2!=null&&!line2.equals(preline2))
						break;
					count2++;
						
					
				}*/
				while((line2=reader2.readLine())!=null&&line2.equals(preline2))
					count2++;
				
				if(preline1!=null&&count1>count2){
			    buffer.add(String.format("(%05d), ", count1-count2)+preline1);
					//buffer.add(String.format("(%05d), ", (count1-count2>0? count1-count2:0))+preline1);
			    
			   /* preline1=line1;
				key1=Integer.parseInt(preline1.substring(0,8));
				line1=reader1.readLine();*/
				}
			}
			else {
				while(true){
				    while((line2=reader2.readLine())!=null&&line2.equals(preline2));
					if(preline2!=null){
					 // buffer.add(String.format("(%05d), ",0)+preline2);
					  preline2=line2;
					  
					  /*if(buffer.size()==outputBufferSize||preline2==null){
						 for(l=0;l<buffer.size();l++){
				    		   w=buffer.get(l);
				    		   bw.write(w);
				    		   bw.newLine();
				    	   }
				    	   buffer.clear();
				       
					  }	*/				  
					}
					if(preline2==null)
						break;
					key2=Integer.parseInt(preline2.substring(0,8));
					if(key2>=key1)
						break;
				}
				count2=1;
				continue;
			}
			
			count1=1;
			count2=1;
			preline1=line1;
			line1=reader1.readLine();
			preline2=line2;
			if(preline1!=null)
			key1=Integer.parseInt(preline1.substring(0,8)); 
			if(preline2!=null)
			key2=Integer.parseInt(preline2.substring(0,8));
			
		}
		}
		/*else{
			if(preline1!=null){

				while(true){
					line1=reader1.readLine();
					if(preline1.equals(line1)){
						count1++;
						continue;
					}
					if(preline1!=null)
					buffer.add(String.format("(%05d), ",count1)+preline1);
					if(buffer.size()==outputBufferSize||preline1==null){
						 for(l=0;l<buffer.size();l++){
				    		   w=buffer.get(l);
				    		   bw.write(w.trim());
				    		   bw.newLine();
				    	   }
				    	   buffer.clear();
					}
					preline1=line1;
					if(preline1==null)
						break;
				}
			}
			/*else if(preline2!=null){
				while(true){
					line2=reader2.readLine();
					if(preline2.equals(line2))
						continue;
					if(preline2!=null)
					buffer.add(String.format("(%05d), ",0)+preline2);
					if(buffer.size()==outputBufferSize||preline2==null){
						 for(l=0;l<buffer.size();l++){
				    		   w=buffer.get(l);
				    		   bw.write(w.trim());
				    		   bw.newLine();
				    	   }
				    	   buffer.clear();
					}
					preline2=line2;
					if(preline2==null)
						break;
				}
			}
		}*/
		
		
		if(preline1==null)//&&line2==null)
			break;
				
	}


	//buffer may has data did not write to disk	
	for(l=0;l<buffer.size();l++){
		   w=buffer.get(l);		   
		   bw.write(w);
		   bw.newLine();
		   totalnum++;
	}	
	reader1.close();
	reader2.close();
	bw.close();
	return totalnum;
	
}
public void QuickSort(ArrayList<SubRelations> sortArray,int left, int right){
	int partition;
	if(left<right){
		partition=Partition(sortArray,left,right);
		QuickSort(sortArray,left,partition-1);
		QuickSort(sortArray,partition+1,right);
	}
}
public static int Partition(ArrayList<SubRelations> sortArray,int left, int right){
	int pivot=Integer.parseInt(sortArray.get(right).insideTuple.substring(0, 8));
	int i=left-1;
	SubRelations temp;
	for(int j=left;j<right;j++){
		if(Integer.parseInt(sortArray.get(j).insideTuple.substring(0, 8))>pivot)
			i++;
		temp=sortArray.get(i);
		sortArray.set(i, sortArray.get(j));
		sortArray.set(j,temp);
	}
	temp=sortArray.get(right);
	sortArray.set(right, sortArray.get(i+1));
	sortArray.set(i+1, temp);
	return i+1;
}
}

class SubRelations{
	
	int Relations_Num;
	long First_Link=0;
	long subSize=0;
	String insideTuple;
	
	
}