import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.FileNotFoundException;
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

public class Tpmms_Join {
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
		/*System.out.println("please choose main memory: enter 0 for 5M, enter 1 for 10M, default main memory is 10M");
		Scanner sc=new Scanner(System.in);
		flag=sc.nextInt();
		sc.nextLine();
		if(flag==0){
			temp=(float)0.9;
			System.out.println("choose main memory 5M, the sub-Relations size is 1.1M");
		}
		else{
			temp=(float)2;
			System.out.println("choose main memory 10M, the sub-Relations size is 2M");
		}*/
		temp=(float)0.9;
		/*System.out.println("please enter the sub-Relations size(M): ");
		Scanner sc=new Scanner(System.in);
		temp=sc.nextFloat();
		sc.nextLine();*/
		MainMemory_Size=1024*1024*temp;
		subListSize=(int)(MainMemory_Size/Tuple_Size);
		
		long currenTime1, currenTime2,currenTime3;
		int Total_Number=0;
		String name1="//T1.txt", name2="//T2.txt";
		System.out.println("doing phase1 for T1, please wait");
		currenTime1=System.currentTimeMillis();
	    Phase1(name1,100);
	    size1=Relations_Size;
	    currenTime2=System.currentTimeMillis();
	    System.out.println("Sorting time of T1 phase1(sec):"+(currenTime2-currenTime1)/1000); 
	    System.out.println("doing phase2 for T1, please wait");
	    Phase2("//outputT1.txt",100);
	    currenTime3=System.currentTimeMillis();
		System.out.println("merge time of T1 phase2(sec):"+(currenTime3-currenTime2)/1000); 
		System.out.println("doing phase1 for T2, please wait");
		Phase1(name2,27);
		size2=Relations_Size;
	    currenTime1=System.currentTimeMillis();
	    System.out.println("Sorting time of T2 phase1(sec):"+(currenTime1-currenTime3)/1000); 
	    System.out.println("doing phase2 for T2, please wait");
	    Phase2("//outputT2.txt",27);
	    currenTime2=System.currentTimeMillis();
		System.out.println("merge time of T2 phase2(sec):"+(currenTime2-currenTime1)/1000); 
		System.out.println("doing join for T1 and T2 and compute, please wait");
		join("//mergeoutputT1.txt","//mergeoutputT2.txt");
		currenTime3=System.currentTimeMillis();
		System.out.println("join and compute time is :"+(currenTime3-currenTime2)/1000); 
		/*Total_Number=dealBD("//mergeoutputT1.txt","//mergeoutputT2.txt");
	    currenTime3=System.currentTimeMillis();
	    System.out.println("compare time of bd(T1,T2)(sec):"+(currenTime3-currenTime2)/1000);
	    System.out.println("the total number of tuples is : "+Total_Number);
	    System.out.println("the number of block used to store output on disk: "+(int)Math.ceil(Total_Number/40));
	    IOD=(int)Math.ceil(Total_Number/40+size1/40+size2/40);
	    IOS1=(int)Math.ceil(size1/10);
	    IOS2=(int)Math.ceil(size2/10);
	    System.out.println("the IO operation in sorting of T1 is: "+IOS1);
	    System.out.println("the IO operation in sorting of T2 is:" +IOS2);
	    System.out.println("the total IO opertion in sorting is: "+(IOS1+IOS2+IOD));*/
	    
	}
	
	
public static void Phase1(String textName, int maxLength ) throws IOException{
		
		BufferedReader reader = new BufferedReader(new FileReader(System.getProperty("user.dir")+textName)); 
		String outputName="//output"+textName.substring(2);
		BufferedWriter bw=new BufferedWriter(new FileWriter(System.getProperty("user.dir")+outputName)); 
		String line;
		String formatstr=String.format("%%-%ds", maxLength);
		subListSize=(int)(MainMemory_Size/maxLength);
		Relations_Size=0;
	     //int TN=MainMemory_Size/Tuple_Size,RT=Relations_Size/TN+1;
	     
	    
	    //ArrayList<String> t=new ArrayList<String>();
	    while(true){
	    	ArrayList<String> tupleList=new ArrayList<String>();
	    	
	    	while((line=reader.readLine())!=null){
            tupleList.add(String.format(formatstr, line.trim()));
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
public static void Phase2(String textName,int maxLength)  throws IOException{
	
	FileChannel fc= FileChannel.open(Paths.get(System.getProperty("user.dir")+"\\"+textName),EnumSet.of(StandardOpenOption.READ));
	String outputName="//merge"+textName.substring(2);
	BufferedWriter bw=new BufferedWriter(new FileWriter(System.getProperty("user.dir")+outputName));
	ByteBuffer rb= ByteBuffer.allocate(maxLength);
	ArrayList<SubRelations> mergeList= new ArrayList<SubRelations>();
	subListSize=(int)(MainMemory_Size/maxLength);
	runTime=(int)Math.ceil(((float)Relations_Size/(float)subListSize));
	int i=0;	
	
		for( i=0; i<runTime;i++){ // initialize 
			SubRelations sr=new SubRelations();
			sr.subSize=subListSize;
			sr.Relations_Num=i;
			fc.read(rb, (i*subListSize+sr.First_Link)*maxLength);
			rb.flip();
			
			sr.insideTuple=new String(rb.array(),0,maxLength);
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
		       fc.read(rb, (recordLoc*subListSize+recordFirst)*maxLength);
		       mergeList.get(mergeList.size()-1).insideTuple=new String(rb.array(),0,maxLength);
		       rb.flip();
		      
		     
		       
		       
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
				
				
				while((line2=reader2.readLine())!=null&&line2.equals(preline2))
					count2++;
				
				if(preline1!=null&&count1>count2){
			    buffer.add(String.format("(%05d), ", count1-count2)+preline1);
					
				}
			}
			else {
				while(true){
				    while((line2=reader2.readLine())!=null&&line2.equals(preline2));
					if(preline2!=null){
					 
					  preline2=line2;
					  
					  		  
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
		
		
		
		if(preline1==null)
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
public static void join(String name1, String name2) throws IOException{
	BufferedReader reader1=new BufferedReader(new FileReader(System.getProperty("user.dir")+name1));
	BufferedReader reader2=new BufferedReader(new FileReader(System.getProperty("user.dir")+name2));
	BufferedWriter  bw=new BufferedWriter(new FileWriter(System.getProperty("user.dir")+"//finaloutput.txt"));
	BufferedWriter  bw1=new BufferedWriter(new FileWriter(System.getProperty("user.dir")+"//GPA_TPMMS.txt"));
	ArrayList<String> buffer=new ArrayList<String>();
	String line1,line2,preline1,preline2,write,content;
	int i,key1,key2;
	int creditSum=0;
	double gradeSum=0;
	double gpa=0;
	preline1=reader1.readLine();
	key1=Integer.parseInt(preline1.substring(0, 8));
	line2="";
	preline2=reader2.readLine();
	key2=Integer.parseInt(preline2.substring(0, 8));
	while(true){
		if(preline1==null||preline2==null){
			if(!buffer.isEmpty())
				for(i=0;i<buffer.size();i++){
					write=buffer.get(i);
					bw.write(write);
					bw.newLine();
				}
			buffer.clear();
			if(gradeSum!=0&&creditSum!=0){
				gpa=(double)gradeSum/(double)creditSum; // compute gpa
				write=String.valueOf(String.format("%08d", key1))+", "+String.valueOf(String.format("%1$.2f", gpa));
				bw1.write(write);
				bw1.newLine();
			}
			break;
		}
		line1=reader1.readLine();
		if(line1!=null&&preline1.substring(0,8).equals(line1.substring(0, 8)))
			continue;   // duplicate elimination for T1
		else{
			while(true){
				if(preline2==null)
					break;
				
				if(key1>key2){
					preline2=reader2.readLine();
					key2=Integer.parseInt(preline2.substring(0,8));
					continue;
				}
				else if(key1<key2){
					if(gradeSum!=0&&creditSum!=0){
						gpa=(double)gradeSum/(double)creditSum; // compute gpa
						write=preline1.substring(0, 8)+", "+String.valueOf(String.format("%1$.2f", gpa));
						bw1.write(write);
						bw1.newLine();
						gradeSum=0;
						creditSum=0;// initialize 
					}
					break; 
				}// compare T1 and T2 according to student id, and when key1 <key2 ,which means there isn't same tuple in T2 
				else if(key1==key2){
			             
				              while(true){
					          line2=reader2.readLine();
						      if(line2==null||!preline2.trim().substring(0, 16).equals(line2.trim().substring(0, 16)))
							  break;
					                 }// duplicate elimination for T2 ,after this loop , line2 is not the duplicate tuple as preline2
				
				content=preline1+","+preline2.substring(8);
				creditSum=creditSum+Integer.parseInt(preline2.substring(21, 23).trim());// compute the sum of credit
				gradeSum=gradeSum+Integer.parseInt(preline2.substring(21, 23).trim())*parseGrade((preline2.substring(23).trim()));// compute the sum of credit*grade
				buffer.add(content);// do join
				
				preline2=line2;
				if(preline2!=null)
				key2=Integer.parseInt(preline2.substring(0,8));
				// initialize preline2
				if(buffer.size()==outputBufferSize){// if buffer is full, then write the data to the disk
						for(i=0;i<buffer.size();i++){
							write=buffer.get(i);
							bw.write(write);
							bw.newLine();
						}
						buffer.clear();
					}
				
				}
				
					
			
			}
			
			preline1=line1;
			if(preline1!=null)
			key1=Integer.parseInt(preline1.substring(0, 8));
			
		    }
	           }
             reader1.close();
             reader2.close();
             bw.close();
             bw1.close();
}

public static double parseGrade(String grade){
	if(grade.equals("A+"))
		return 4.3;
	else if(grade.equals("A"))
		return 4.0;
	else if(grade.equals("A-"))
		return 3.7;
	else if(grade.equals("B+"))
		return 3.3;
	else if(grade.equals("B"))
		return 3.0;
	else if(grade.equals("B-"))
		return 2.7;
	else if(grade.equals("C+"))
		return 2.3;
	else if(grade.equals("C"))
		return 2.0;
	else if(grade.equals("C-"))
		return 1.7;
	else if(grade.equals("D+"))
		return 1.3;
	else if(grade.equals("D"))
		return 1.0;
	else if(grade.equals("D-"))
		return 0.7;
	else
		return 0;
     
}




}
class SubRelations{
	
	int Relations_Num;
	long First_Link=0;
	long subSize=0;
	String insideTuple;
	
	
}
