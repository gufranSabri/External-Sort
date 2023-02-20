import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.util.*;
import java.util.concurrent.TimeUnit;

public class Main {
    public static void main(String[] args) throws IOException{
        Test.test("/media/gufran/GsHDD/Work/Projects/Console/ExternalMergeSort","data",100_000_000);
    }
}

class Test{
    static void test(String path, String fileName, long n) throws IOException{
        generateData(n, path, fileName);
        ExternalSort s= new ExternalSort(path,fileName);
        s.sort();
        verifyResults(n, path);
    }
    static void generateData(long n, String path, String fileName) throws IOException{
        Random rd = new Random(); 
        new File(path+fileName+".txt"); 
        FileWriter w = new FileWriter(path+fileName+".txt");

        System.out.println("Generating data...");
        while(n>0){
            w.write(rd.nextInt(1000)+"\n");
            n--;
        }
        w.close();
    }
    
    static void verifyResults(long n, String path) throws IOException{
        if(n>10_000_000)System.out.println("File size too large. Will not verify.");
        else{
            System.out.println("\nVerifying results...");

            BufferedReader br1 = new BufferedReader(new FileReader(new File(path+"data.txt")));
            BufferedReader br2 = new BufferedReader(new FileReader(new File(path+"output.txt")));
            ArrayList<Long> data = new ArrayList<>(), sorted = new ArrayList<>();

            String x;
            while((x=br1.readLine())!=null)data.add(Long.parseLong(x));
            while((x=br2.readLine())!=null)sorted.add(Long.parseLong(x));

            Collections.sort(data);
            boolean failed = false;
            for (int i = 0; i < sorted.size(); i++) {
                if(!sorted.get(i).equals(data.get(i))){
                    System.out.println("\nMismatch at " + i);
                    failed=true;
                    break;
                }
            }
            if(!failed) System.out.println("Sort verified");
        }
    }
}

class ExternalSort{
    private final String EXTENSION = ".txt", OUTPUT_FILE_NAME="output", DUMMY_FILE_NAME="dummy";
    
    private int chunkSize=0;
    private String path, fileName;
    private ArrayList<Chunk> chunkList = new ArrayList<>();

    ExternalSort(String path, String fileName){
        this.path=path;
        this.fileName=fileName;

        boolean multiply = true, add=false;
        int n=10, mulFactor=-1;

        System.out.println("\nCalculating chunk size...");
        while(true){
            try{
                int[] arr = new int[n];
                if(multiply){
                    n*=10;
                    mulFactor++;
                }
                if(add)n+=(Math.pow(10, mulFactor));
            }
            catch(OutOfMemoryError e){
                if(multiply){
                    n/=10;
                    multiply=false;
                    add=true;
                }
                else if(add){
                    chunkSize=n/10;
                    System.out.println("Chunk size = "+ chunkSize+"\n");
                    break;
                }
            }
        }
    }
    public void sort() throws IOException{
        long x = System.currentTimeMillis();

        System.out.println("Sorting data...");
        createChunks();
        
        if(chunkList.size()==1)chunkList.add(new Chunk(path, DUMMY_FILE_NAME, true));

        System.out.println("\nMerging...");
        while(chunkList.size()!=1){
            System.out.println("\n"+(chunkList.size())+" chunks left to merge");

            Chunk c1 = chunkList.remove(chunkList.size()-1);
            Chunk c2 = chunkList.remove(chunkList.size()-1);

            String newFileName =  chunkList.size()==0?OUTPUT_FILE_NAME:(Integer.parseInt(c1.getName())+2)+"";
            chunkList.add(mergeChunks(c1, c2, newFileName));
        }
        System.out.println("\nData from "+fileName+EXTENSION+" sorted into "+OUTPUT_FILE_NAME+EXTENSION);

        String timeTaken = String.format("%02d:%02d:%02d", 
            TimeUnit.MILLISECONDS.toHours(System.currentTimeMillis()-x),
            TimeUnit.MILLISECONDS.toMinutes(System.currentTimeMillis()-x) -  
            TimeUnit.HOURS.toMinutes(TimeUnit.MILLISECONDS.toHours(System.currentTimeMillis()-x)), 
            TimeUnit.MILLISECONDS.toSeconds(System.currentTimeMillis()-x) - 
            TimeUnit.MINUTES.toSeconds(TimeUnit.MILLISECONDS.toMinutes(System.currentTimeMillis()-x)));   
        System.out.println("\nTime taken (hh:mm:ss): "+timeTaken);
    }
    public void createChunks() throws IOException{
        BufferedReader br = new BufferedReader(new FileReader(new File(path+fileName+EXTENSION)));
        FileWriter w=null;
        
        ArrayList<Long> arrData = new ArrayList<>();
        boolean eofReached=false;
        int i=0, fName=1;
        String s;
        
        while(true){
            if(i==0||eofReached){
                if(w!=null){
                    MergeSort.sort(arrData, 0, arrData.size()-1, w);
                    w.close();
                    fName++;
                }
                if(eofReached) {
                    if(arrData.size()==0)chunkList.remove(chunkList.size()-1);
                    w.close();
                    break;
                }

                arrData=new ArrayList<>();
                chunkList.add(new Chunk(path, fName+"", true));
                w = new FileWriter(path+fName+EXTENSION);
                System.out.println("Sorting chunk " + (chunkList.size()));
            }
            else{
                s=br.readLine();
                if(s==null)eofReached=true;
                else arrData.add(Long.parseLong(s));
            }
            i = (i+1)%(chunkSize+1);
        }
        br.close();
    }
    public Chunk mergeChunks(Chunk chunk1 ,Chunk chunk2, String outputFileName) throws IOException{
        Chunk c= new Chunk(path, outputFileName, !outputFileName.equals(OUTPUT_FILE_NAME));

        FileWriter w = new FileWriter(path+outputFileName+EXTENSION);
        BufferedReader br1 = new BufferedReader(new FileReader(new File(path+chunk1.getName()+EXTENSION)));
        BufferedReader br2 = new BufferedReader(new FileReader(new File(path+chunk2.getName()+EXTENSION)));

        String n1=null, n2=null;
        boolean newN1=true, newN2=true;

        while(true){
            n1 = newN1?br1.readLine():n1;
            n2 = newN2?br2.readLine():n2;

            if(n1!=null&&n2!=null){
                if(Long.parseLong(n1)<=Long.parseLong(n2)){
                    newN1=true;
                    newN2=false;
                }
                else{
                    newN1=false;
                    newN2=true;
                }
            }
            else if(n1!=null){
                newN1=true;
                newN2=false;
            }
            else if(n2!=null){
                newN1=false;
                newN2=true;
            }
            else break;

            if(newN1)w.write(n1+"\n");
            else if(newN2)w.write(n2+"\n");
        }
        br1.close();
        br2.close();
        w.close();

        chunk1.delete();
        chunk2.delete();

        return c;
    }
}

class MergeSort{
    public static void sort(ArrayList<Long> arr, int l, int h, FileWriter w) throws IOException{
        if(l<h){
            int mid = (l+h)/2;
            sort(arr, l, mid, w);
            sort(arr, mid+1, h, w);
            merge(arr, l, mid, h, w);
        }
        else if(arr.size()==1)w.write(arr.get(0)+"\n");
    }
	static void merge(ArrayList<Long> arr, int l, int m, int r, FileWriter w) throws IOException{
        long[] temp = new long[r-l+1];
        int i = l, j = m+1, k=0;

        while(i<m+1&&j<r+1){
            if(arr.get(i)<arr.get(j)) temp[k++]=arr.get(i++);
            else temp[k++]=arr.get(j++);
        }

        while(i<m+1)temp[k++]=arr.get(i++);
        while(j<r+1)temp[k++]=arr.get(j++);

        k=l;
        for (int index = 0; index < temp.length; index++){
            arr.set(k++,temp[index]);
            if(l==0&&r==arr.size()-1)w.write(temp[index]+"\n");
        }
	}
}

class Chunk{
    private String name, path;

    Chunk(String path, String name, boolean isTemp){
        this.name=name;
        this.path=path;

        try {
            File f = new File(path+name+".txt");
            if (f.createNewFile()) {
                if(isTemp)f.deleteOnExit();
            }
        } 
        catch (IOException ignored) {System.out.println("An error occurred.");}
    }
    public void delete(){
        File d = new File(path+name+".txt");
        d.delete();
    }
    public String getName() {return name;}
}

// javac --release 8 Main.java && java Main