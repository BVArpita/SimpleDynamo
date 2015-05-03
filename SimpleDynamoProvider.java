package edu.buffalo.cse.cse486586.simpledynamo;

import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.io.PrintWriter;
import java.io.Serializable;
import java.net.InetAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.UnknownHostException;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Formatter;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;

import android.content.ContentProvider;
import android.content.ContentResolver;
import android.content.ContentValues;
import android.content.Context;
import android.database.Cursor;
import android.database.MatrixCursor;
import android.net.Uri;
import android.os.AsyncTask;
import android.telephony.TelephonyManager;
import android.util.Log;

public class SimpleDynamoProvider extends ContentProvider {

    static final String TAG =SimpleDynamoProvider.class.getSimpleName();
    static final int SERVER_PORT = 10000;
    //MatrixCursor cursor=new MatrixCursor(new String[]{"key","value"} );
    String resultans="";
    final String  lowestport="11124";
    final Object lock = new Object();

    List<String> listofhashnodes=new LinkedList<>();
    List<String> listofnodes=new LinkedList<>();
    HashMap<String ,List<String>> hm = new HashMap<>();
    MatrixCursor cursor_star =new MatrixCursor(new String[]{"key","value"} );





    public class Message  {
        String   msg;
        String toport;
        String amport;
        String key;
        String value;

        public Message(String message, String portnumber,String sendtoport,String k,String val){
            msg=message;
            amport=portnumber;
            toport=sendtoport;
            key=k;
            value=val;
        }

        public String toString(){
            String ans=msg+"-"+amport+"-"+toport+"-"+key+"-"+value;
            return ans;

        }
    }

    @Override
	public int delete(Uri uri, String selection, String[] selectionArgs) {

		return deletelocal(selection);
	}

    public int deletelocal(String selection){
        Integer delrows=0;
        boolean isdeleted=false;
        String[] filelist=getContext().fileList();
        for(String f : filelist){
            if(f.equals(selection)){
                isdeleted=getContext().deleteFile(f);
            }

        }
        if(isdeleted == true){
            delrows+=1;
            Log.d(TAG,"deleted key"+" "+selection);
            Log.d(TAG,getContext().fileList().length+" "+"filelist");
        }
        return delrows;
    }

	@Override
	public String getType(Uri uri) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public Uri insert(Uri uri, ContentValues values) {
        ArrayList<String> portstrings= new ArrayList<String>();
        portstrings.add("5554");
        portstrings.add("5556");
        portstrings.add("5558");
        portstrings.add("5560");
        portstrings.add("5562");

        String key= values.getAsString("key");
        String val=values.getAsString("value");
        String myport=getport();
        String avd=String.valueOf(Integer.parseInt(myport)/2);
        String highid=Collections.max(listofhashnodes);
        String lowid=Collections.min(listofhashnodes);
        //final String  lowestport="11124";
        try {
        String successor=hm.get(genHash(avd)).get(0);
        String predecessor=hm.get(genHash(avd)).get(2);
        String keyid=genHash(key);

        //local insert
        if(predecessor.compareTo(keyid)<0 && keyid.compareTo(genHash(avd))<=0){
            Log.d(TAG,key+" "+"in local insert");
            writetofile(uri, values);
            String emu_id=String.valueOf(Integer.parseInt(myport) / 2);
            String myport_suc1=hm.get(genHash(emu_id)).get(0);
            for(int i=0;i<portstrings.size();i++){
                if(myport_suc1.equals(genHash(portstrings.get(i)))){
                    String toport = Integer.toString(Integer.parseInt(portstrings.get(i))*2);
                    Message m1=new Message("repinsert",myport,toport,key,val);
                    String msg=m1.toString();
                    new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, msg,toport);


                }
            }


            String myport_suc2=hm.get(genHash(emu_id)).get(1);
            for(int i=0;i<portstrings.size();i++){
                if(myport_suc2.equals(genHash(portstrings.get(i)))){
                    String toport = Integer.toString(Integer.parseInt(portstrings.get(i))*2);
                    Message m1=new Message("repinsert",myport,toport,key,val);
                    String msg=m1.toString();
                    new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, msg,toport);


                }
            }


        }
        //insert into lowest port
        else if(keyid.compareTo(highid) >0 || keyid.compareTo(lowid) <0){
            Message m1=new Message("insert",myport,lowestport,key,val);
            String msg=m1.toString();
            Log.d(TAG,key+" "+"message being sent to spl partition");
            new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, msg,lowestport);
        }
        else  if(genHash("5554").compareTo(keyid)<0 && keyid.compareTo(genHash("5558"))<=0){
            String toport="11116";
            Log.d(TAG,key+" "+" sent to 5558");
            Message m1=new Message("insert",myport,toport,key,val);
            String msg=m1.toString();
            new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, msg,toport);
        }
        else if(genHash("5558").compareTo(keyid)<0 && keyid.compareTo(genHash("5560"))<=0){
            String toport="11120";
            Log.d(TAG,key+" "+" sent to 5560");
            Message m1=new Message("insert",myport,toport,key,val);
            String msg=m1.toString();
            new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, msg,toport);
        }
        else if(genHash("5560").compareTo(keyid)<0 && keyid.compareTo(genHash("5562"))<=0){
            String toport="11124";
            Log.d(TAG,key+" "+" sent to 5562");
            Message m1=new Message("insert",myport,toport,key,val);
            String msg=m1.toString();
            new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, msg,toport);
        }
        else if(genHash("5562").compareTo(keyid)<0 && keyid.compareTo(genHash("5556"))<=0){
            String toport="11112";
            Log.d(TAG,key+" "+" sent to 5556");
            Message m1=new Message("insert",myport,toport,key,val);
            String msg=m1.toString();
            new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, msg,toport);
        }
        else if(genHash("5556").compareTo(keyid)<0 && keyid.compareTo(genHash("5554"))<=0){
            String toport="11108";
            Log.d(TAG,key+" "+" sent to 5554");
            Message m1=new Message("insert",myport,toport,key,val);
            String msg=m1.toString();
            new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, msg,toport);
        }

        } catch (NoSuchAlgorithmException e) {
            e.printStackTrace();
        }
        return uri;
	}

    public Uri writetofile(Uri uri,ContentValues values){
        String[] filelist=getContext().fileList();
        String key= values.getAsString("key");
        String val=values.getAsString("value");
        //Log.d(TAG,"in insert"+" "+dht+" "+portstring);

        for(String f : filelist) { //to check if file with key name already exists and if so , update value.
            if (f.equals(key)) {
                try {
                    FileOutputStream fos =getContext().openFileOutput(key, Context.MODE_PRIVATE);


                    fos.write(val.getBytes());

                    fos.close();

                    return uri;
                }
                catch(IOException e){
                    Log.e(TAG, "Unable to open file");
                }
            }
        }

        try {
            FileOutputStream fos =getContext().openFileOutput(key, Context.MODE_PRIVATE);


            fos.write(val.getBytes());

            fos.close();

            return uri;
        }
        catch(IOException e){
            Log.e(TAG, "Unable to open file");
        }
        return uri;
        // Log.v("insert", values.toString());
    }

	@Override
	public boolean onCreate() {
        try {
            Log.d(TAG,"in oncreate");
            Log.d(TAG,"number of nodes"+" "+listofhashnodes.size());
            listofhashnodes.add(genHash("5554"));
            listofhashnodes.add(genHash("5556"));
            listofhashnodes.add(genHash("5558"));
            listofhashnodes.add(genHash("5560"));
            listofhashnodes.add(genHash("5562"));
        } catch (NoSuchAlgorithmException e) {
            e.printStackTrace();
        }

        try {

            addMembership(listofhashnodes);
            ServerSocket serverSocket = new ServerSocket(SERVER_PORT);
            new ServerTask().executeOnExecutor(AsyncTask.THREAD_POOL_EXECUTOR, serverSocket);

        } catch (NoSuchAlgorithmException e) {
            e.printStackTrace();
        } catch (IOException e) {

            Log.e(TAG, "Can't create a ServerSocket");
            //return;
        }
        return false;
	}

    public void addMembership(List<String> listofhashnodes) throws NoSuchAlgorithmException {
        listofnodes.add("5554");
        listofnodes.add("5556");
        listofnodes.add("5558");
        listofnodes.add("5560");
        listofnodes.add("5562");
        Collections.sort(listofhashnodes);
        // adding successors
        for(int i=0;i<3;i++){

            String successor = listofhashnodes.get(i+1);
            String successor1=listofhashnodes.get(i+2);
            List<String> pointersu = new LinkedList<>();
            pointersu.add(successor);
            pointersu.add(successor1);
            hm.put(listofhashnodes.get(i),pointersu);
        }
        List<String> pointersu = new LinkedList<>();
        pointersu.add(listofhashnodes.get(4));
        pointersu.add(listofhashnodes.get(0));
        hm.put(listofhashnodes.get(3),pointersu);

        List<String> pointersu1 = new LinkedList<>();
        pointersu1.add(listofhashnodes.get(0));
        pointersu1.add(listofhashnodes.get(1));
        hm.put(listofhashnodes.get(4),pointersu1);


        //adding predecessors
        for(int j=4;j>0;j--){
            String predecessor= listofhashnodes.get(j-1);
            List<String> pointerpr;
            //pointerpr.add(predecessor);
            pointerpr=hm.get(listofhashnodes.get(j));
            pointerpr.add(predecessor);
            hm.put(listofhashnodes.get(j),pointerpr);
        }
        List<String> pointerpr ;
        pointerpr = hm.get(listofhashnodes.get(0));
        pointerpr.add(listofhashnodes.get(4));
        hm.put(listofhashnodes.get(0),pointerpr);

        Log.d(TAG,"5554"+" "+hm.get(genHash("5554")));
        Log.d(TAG,"5556"+" "+hm.get(genHash("5556")));
        Log.d(TAG,"5558"+" "+hm.get(genHash("5558")));
        Log.d(TAG,"5560"+" "+hm.get(genHash("5560")));
        Log.d(TAG,"5562"+" "+hm.get(genHash("5562")));
        String[] filelist=getContext().fileList();
        if(filelist.length>0){
            Log.d(TAG,"revival of node");
            getfromsuccessors();
            getfrompredecessors();
        }


    }

    public void getfromsuccessors(){
        String toport=hm.get(getport()).get(0);
        String amport=getport();
        String message="getdataafterfailure";
        Message m1=new Message(message,amport,toport," "," ");
        String msg=m1.toString();

        new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, msg,toport);

    }

    public void getfrompredecessors(){

    }

	@Override
	public synchronized  Cursor query(Uri uri, String[] projection, String selection,
			String[] selectionArgs, String sortOrder) {

        String myport=getport();
        String avd=String.valueOf(Integer.parseInt(myport)/2);
        MatrixCursor cursor_local=new MatrixCursor(new String[]{"key","value"} );

        try {
           // String successor=hm.get(genHash(avd)).get(0);
            String predecessor=hm.get(genHash(avd)).get(1);
            String keyid=genHash(selection);
            String highid=Collections.max(listofhashnodes);
            String lowid=Collections.min(listofhashnodes);


        if(selection.equals( "\"@\"")){
            String[] filelist=getContext().fileList();
            for(String f : filelist){

                try{                                //Reference from http://stackoverflow.com/questions/14768191/how-do-i-read-the-file-content-from-the-internal-storage-android-app
                    //String path=getContextt
                    FileInputStream fis = getContext().openFileInput(f);
                    InputStreamReader isr = new InputStreamReader(fis);
                    BufferedReader bufferedReader = new BufferedReader(isr);
                    StringBuilder sb = new StringBuilder();
                    String line="";
                    //to debug


                    while ((line = bufferedReader.readLine()) != null) {
                        sb.append(line);
                    }
                    cursor_local.addRow(new Object[] { f, sb.toString() });
                    //return cursor;


                }
                catch(IOException e){
                    Log.e(TAG, "Unable to read from file");
                }


            }
            return cursor_local;
        }

        else if(selection.equals( "\"*\"")){
            for(int i=0;i<5;i++) {
                String toport=Integer.toString(Integer.parseInt(listofnodes.get(i))*2);
                Message m1=new Message("star",myport,toport,"*","");
                String msg=m1.toString();
                AsyncTask c=new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, msg, toport);

                synchronized (c){
                    c.wait(500);
                }
            }

            return cursor_star;
        }
        //local query
        else if(predecessor.compareTo(keyid)<0 && keyid.compareTo(genHash(myport))<=0) {
            String ans = localquery(selection);
            MatrixCursor cursor1=new MatrixCursor(new String[]{"key","value"} );
            cursor1.addRow(new Object[] { selection, ans.toString() });
            return cursor1;

        }

        //query in lowestport
        else if(keyid.compareTo(highid) >0 || keyid.compareTo(lowid) <0){

            Message m1=new Message("query",myport,lowestport,selection,"");
            String msg = m1.toString();
            AsyncTask d=new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, msg,lowestport);
            synchronized (resultans) {
                synchronized (d) {

                    while (resultans.equals("")) {
                        d.wait(100);
                    }

            }

            if(!resultans.equals("")){
                MatrixCursor cursor1=new MatrixCursor(new String[]{"key","value"} );
                cursor1.addRow(new Object[] { selection, resultans });
                Log.d(TAG, "query result returned at" + " " + selection + resultans);
                resultans="";
                return cursor1;
            }}
        }
        else if(genHash("5554").compareTo(keyid)<0 && keyid.compareTo(genHash("5558"))<=0){
            String toport="11116";
            Message m1=new Message("query",myport,toport,selection,"");
            String msg = m1.toString();
            AsyncTask c= new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, msg,toport);
            synchronized (resultans) {
                synchronized (c) {
                    while (resultans.equals("")) {
                        c.wait(100);
                    }
                }

                if (!resultans.equals("")) {
                    MatrixCursor cursor = new MatrixCursor(new String[]{"key", "value"});
                    cursor.addRow(new Object[]{selection, resultans});
                    Log.d(TAG, "query result returned at" + " " + selection + resultans);
                    resultans = "";
                    return cursor;
                }
            }
        }

        else if(genHash("5558").compareTo(keyid)<0 && keyid.compareTo(genHash("5560"))<=0){
            String toport="11120";
            Message m1=new Message("query",myport,toport,selection,"");
            String msg = m1.toString();
            AsyncTask c= new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, msg,toport);
            synchronized (resultans) {
                synchronized (c) {
                    while (resultans.equals("")) {
                        c.wait(100);
                    }
                }

                if (!resultans.equals("")) {
                    MatrixCursor cursor = new MatrixCursor(new String[]{"key", "value"});
                    cursor.addRow(new Object[]{selection, resultans});
                    Log.d(TAG, "query result returned at" + " " + selection + resultans);
                    resultans = "";
                    return cursor;
                }
            }
        }

        else if(genHash("5560").compareTo(keyid)<0 && keyid.compareTo(genHash("5562"))<=0){
            String toport="11124";
            Message m1=new Message("query",myport,toport,selection,"");
            String msg = m1.toString();
            AsyncTask c= new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, msg,toport);
            synchronized (resultans) {
                synchronized (c) {
                    while (resultans.equals("")) {
                        c.wait(100);
                    }
                }

                if (!resultans.equals("")) {
                    MatrixCursor cursor = new MatrixCursor(new String[]{"key", "value"});
                    cursor.addRow(new Object[]{selection, resultans});
                    Log.d(TAG, "query result returned at" + " " + selection + resultans);
                    resultans = "";
                    return cursor;
                }
            }
        }

        else if(genHash("5562").compareTo(keyid)<0 && keyid.compareTo(genHash("5556"))<=0){
            String toport="11112";
            Message m1=new Message("query",myport,toport,selection,"");
            String msg = m1.toString();
            AsyncTask c= new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, msg,toport);
            synchronized (resultans) {
                synchronized (c) {
                    while (resultans.equals("")) {
                        c.wait(100);
                    }
                }

                if (!resultans.equals("")) {
                    MatrixCursor cursor = new MatrixCursor(new String[]{"key", "value"});
                    cursor.addRow(new Object[]{selection, resultans});
                    Log.d(TAG, "query result returned at" + " " + selection + resultans);
                    resultans = "";
                    return cursor;
                }
            }
        }

        else if(genHash("5556").compareTo(keyid)<0 && keyid.compareTo(genHash("5554"))<=0){
            String toport="11108";
            Message m1=new Message("query",myport,toport,selection,"");
            String msg = m1.toString();
            AsyncTask c= new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, msg,toport);
            synchronized (resultans) {
                synchronized (c) {
                    while (resultans.equals("")) {
                        c.wait(100);
                    }
                }

                if (!resultans.equals("")) {
                    MatrixCursor cursor = new MatrixCursor(new String[]{"key", "value"});
                    cursor.addRow(new Object[]{selection, resultans});
                    Log.d(TAG, "query result returned at" + " " + selection + resultans);
                    resultans = "";
                    return cursor;
                }
            }
        }

        } catch (NoSuchAlgorithmException e) {
            e.printStackTrace();
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
        return null;
	}

    public String localquery(String selection){
        String[] filelist=getContext().fileList();
        for(String f : filelist){
            if(f.equals(selection)){
                try{                                //Reference from http://stackoverflow.com/questions/14768191/how-do-i-read-the-file-content-from-the-internal-storage-android-app
                    //String path=getContextt
                    FileInputStream fis = getContext().openFileInput(selection);
                    InputStreamReader isr = new InputStreamReader(fis);
                    BufferedReader bufferedReader = new BufferedReader(isr);
                    StringBuilder sb = new StringBuilder();
                    String line="";
                    //to debug


                    while ((line = bufferedReader.readLine()) != null) {
                        sb.append(line);
                    }
                    //Log.d("Value of line",line);
                    MatrixCursor cursor=new MatrixCursor(new String[]{"key","value"} );
                    cursor.addRow(new Object[] { selection, sb.toString() });
                    //return cursor;
                    return sb.toString();
                }
                catch(IOException e){
                    Log.e(TAG, "Unable to read from file");
                }
            }}

        return null;
    }



   /* public void queryall(){
        String[] filelist=getContext().fileList();

        for(String f : filelist){

            try{                                //Reference from http://stackoverflow.com/questions/14768191/how-do-i-read-the-file-content-from-the-internal-storage-android-app
                //String path=getContextt
                FileInputStream fis = getContext().openFileInput(f);
                InputStreamReader isr = new InputStreamReader(fis);
                BufferedReader bufferedReader = new BufferedReader(isr);
                StringBuilder sb = new StringBuilder();
                String line="";
                //to debug


                while ((line = bufferedReader.readLine()) != null) {
                    sb.append(line);
                }
                cursor.addRow(new Object[] { f, sb.toString() });
                //return cursor;


            }
            catch(IOException e){
                Log.e(TAG, "Unable to read from file");
            }


        }
    }*/

    public void sendtooriginator(String ans,String originator){
        if(ans != null) {
            Log.d(TAG,"at sendtooriginator"+" "+ans);
            Message m1=new Message("ans",getport(),originator,"",ans);
            String msg=m1.toString();
            new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, msg, originator);
        }
    }

    public void sendallkeyvalues(String originator) {
        String[] filelist = getContext().fileList();
        for (String f : filelist) {

            try {

                FileInputStream fis = getContext().openFileInput(f);
                InputStreamReader isr = new InputStreamReader(fis);
                BufferedReader bufferedReader = new BufferedReader(isr);
                StringBuilder sb = new StringBuilder();
                String line = "";
                //to debug


                while ((line = bufferedReader.readLine()) != null) {
                    sb.append(line);
                }
                Message m1=new Message("finalall",getport(),originator,f,sb.toString());
                String msg = m1.toString();
                new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, msg, originator);

            } catch (IOException e) {
                Log.e(TAG, "Unable to read from file");
            }
        }
    }
	@Override
	public int update(Uri uri, ContentValues values, String selection,
			String[] selectionArgs) {
		// TODO Auto-generated method stub
		return 0;
	}

    private class ClientTask extends AsyncTask<String, Void, Void> {
        protected Void doInBackground(String... m1) {
            try {
                String[] msgarr=m1[0].split("-");
                String message=msgarr[0];
                if(message.equals("insert")) {
                    Socket socket0 = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}),
                            Integer.parseInt(m1[1]));
                    socket0.setSoTimeout(2500);
                    Log.d(TAG, "at clienttask for insert");
                    PrintWriter out = new PrintWriter(socket0.getOutputStream(), true);
                    out.println(m1[0]);
                    Log.d(TAG, "msg sent to" + " " + m1[1] + " " + m1[0]);

                    BufferedReader br = new BufferedReader(new InputStreamReader(socket0.getInputStream()));
                    String recieveddata = br.readLine();
                    if(recieveddata == null){
                        Log.d(TAG,"failed port"+" "+m1[1]);
                    }
                    else {
                        // if (recieveddata != null) {
                        String[] msgarray = recieveddata.split("-");
                        String msg = msgarray[0];
                        if (msg.equals("ackforinsert")) {
                            Log.d(TAG, "ack recieved from" + " " + msgarray[1]);
                        }
                    }
                   // }
                    socket0.close();
                }
                else{
                    Socket socket0 = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}),
                            Integer.parseInt(m1[1]));
                    socket0.setSoTimeout(2500);
                   // Log.d(TAG, "at clienttask from sendtooriginator");
                    PrintWriter out = new PrintWriter(socket0.getOutputStream(), true);
                    out.println(m1[0]);
                    Log.d(TAG, "msg sent to" + " " + m1[1] + " " + m1[0]);
                    socket0.close();
                }


            } catch (UnknownHostException e) {
                Log.e(TAG, "ClientTask UnknownHostException");
            } catch (IOException e) {
                Log.e(TAG, "ClientTask socket IOException");
                e.printStackTrace();
            }

            return null;
        }
    }


    private class ServerTask extends AsyncTask<ServerSocket, String, Void> {
       // private final ContentResolver amContentResolver=getContentResolver();
        private  Uri amUri;

        private Uri buildUri(String scheme, String authority) {
            Uri.Builder uriBuilder = new Uri.Builder();
            uriBuilder.authority(authority);
            uriBuilder.scheme(scheme);
            return uriBuilder.build();
        }
        @Override
        protected Void doInBackground(ServerSocket... sockets) {
            ServerSocket serverSocket = sockets[0];
            ArrayList<String> portstrings= new ArrayList<String>();
            portstrings.add("5554");
            portstrings.add("5556");
            portstrings.add("5558");
            portstrings.add("5560");
            portstrings.add("5562");
            //Integer key=0;
            //String value;
            amUri = buildUri("content", "edu.buffalo.cse.cse486586.simpledynamo.provider");

            while(true){
                try {
                    Socket clientSocket = serverSocket.accept();
                    BufferedReader br = new BufferedReader(new InputStreamReader(clientSocket.getInputStream()));
                    String recieveddata = br.readLine();
                    String[] msgarray=recieveddata.split("-");
                    String msg=msgarray[0];

                    if (msg.equals("insert")){
                        String key=msgarray[3];
                        String value=msgarray[4];
                        ContentValues val =new ContentValues();
                        val.put("key",key);
                        val.put("value", value);

                        writetofile(amUri, val);
                        PrintWriter out = new PrintWriter(clientSocket.getOutputStream(), true);
                        out.println("ackforinsert-"+getport());
                        out.close();

                        String myport=getport();
                        String emu_id=String.valueOf(Integer.parseInt(myport) / 2);
                        String myport_suc1=hm.get(genHash(emu_id)).get(0);
                        for(int i=0;i<portstrings.size();i++){
                            if(myport_suc1.equals(genHash(portstrings.get(i)))){
                                String toport = Integer.toString(Integer.parseInt(portstrings.get(i))*2);

                                Message m1=new Message("repinsert",myport,toport,key,value);
                                String message=m1.toString();
                                new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, message,toport);


                            }
                        }


                        String myport_suc2=hm.get(genHash(emu_id)).get(1);
                        for(int i=0;i<portstrings.size();i++){
                            if(myport_suc2.equals(genHash(portstrings.get(i)))){
                                String toport = Integer.toString(Integer.parseInt(portstrings.get(i))*2);
                                Message m1=new Message("repinsert",myport,toport,key,value);
                                String message=m1.toString();
                                new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, message,toport);


                            }
                        }
                    }

                    else if(msg.equals("repinsert")){
                        Log.d(TAG,"in server for repinsert"+" "+recieveddata);
                        ContentValues val =new ContentValues();
                        String key=msgarray[3];
                        String value=msgarray[4];
                        val.put("key",key);
                        val.put("value", value);
                        writetofile(amUri,val);
                    }
                    else if(msg.equals("query")){
                        Log.d(TAG,"in query of server"+getport());
                        String selection=msgarray[3];
                        String ans=localquery(selection);
                        String originator=msgarray[1];
                        Log.d(TAG,"query result found at"+getport()+" "+ans);
                        sendtooriginator(ans,originator);
                    }
                    else if(msg.equals("ans")){
                        //final Object lock = new Object();
                        synchronized (lock) {
                            resultans = msgarray[4];
                        }
                        Log.d(TAG,"final query result recived at"+" "+getport()+" "+resultans);
                    }
                    else if(msg.equals("star")){
                        String originator=msgarray[1];
                        sendallkeyvalues(originator);
                    }
                    else if(msg.equals("finalall")){
                        String key=msgarray[3];
                        String value=msgarray[4];
                        cursor_star.addRow(new Object[]{key, value});
                    }
                    else if(msg.equals("ackforinsert")){
                        Log.d(TAG,"ack recieved at"+" "+getport()+"from "+" "+msgarray[1]);
                    }



                }
                catch(IOException e){
                    Log.e(TAG, "Accept Failed");
                } catch (NoSuchAlgorithmException e) {
                    e.printStackTrace();
                }
            }

        }
    }


    private String genHash(String input) throws NoSuchAlgorithmException {
        MessageDigest sha1 = MessageDigest.getInstance("SHA-1");
        byte[] sha1Hash = sha1.digest(input.getBytes());
        Formatter formatter = new Formatter();
        for (byte b : sha1Hash) {
            formatter.format("%02x", b);
        }
        return formatter.toString();
    }

    public  String getport(){
        TelephonyManager tel = (TelephonyManager) getContext().getSystemService(Context.TELEPHONY_SERVICE);
        String portStr = tel.getLine1Number().substring(tel.getLine1Number().length() - 4);
        final  String myPort = String.valueOf((Integer.parseInt(portStr) * 2));
        return myPort;
    }
}
