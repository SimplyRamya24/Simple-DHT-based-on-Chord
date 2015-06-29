package edu.buffalo.cse.cse486586.simpledht;

import android.content.ContentProvider;
import android.content.ContentValues;
import android.content.Context;
import android.database.Cursor;
import android.database.MatrixCursor;
import android.database.MergeCursor;
import android.database.sqlite.SQLiteDatabase;
import android.net.Uri;
import android.os.AsyncTask;
import android.telephony.TelephonyManager;
import android.util.Log;
import android.widget.Switch;
import android.widget.TextView;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.PrintWriter;
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

public class SimpleDhtProvider extends ContentProvider {

    static final String REMOTE_PORT0 = "11108";
//    static final String REMOTE_PORT1 = "11112";
//    static final String REMOTE_PORT2 = "11116";
//    static final String REMOTE_PORT3 = "11120";
//    static final String REMOTE_PORT4 = "11124";
    static final int SERVER_PORT = 10000;
    private ArrayList<String[]> joinedList = new ArrayList<String[]>();
    private String successorNode;
    private String predecessorNode;
    private HashMap<String,String> queryMap = new HashMap();
    private HashMap<String,String> queryAll = new HashMap();
    private String myNode;
    private Boolean isQueryComplete = false;
    private final Uri mUri = buildUri("content", "edu.buffalo.cse.cse486586.simpledht.provider");
    private SimpleDHTOpenHelper db;

    @Override
    public int delete(Uri uri, String selection, String[] selectionArgs) {

        MessageObject delMsg = new MessageObject();
        delMsg.mType = "D";
        delMsg.mHashValue = "Dummy";
        delMsg.mKey = selection;

        Boolean result = checkMyPartition(delMsg.mKey);
        if (!result)
        {
            new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR,
                    delMsg.toString(), successorNode);
            return 0;
        }
        Log.d("Delete", "Deleting msg " + delMsg.toString());
        SQLiteDatabase database = db.getReadableDatabase();
        String[] sArgs = {selection};
        database.delete("simple_dht", "key = ?", sArgs);
        return 0;
    }

    @Override
    public String getType(Uri uri) {

        return null;
    }

    private Uri buildUri(String scheme, String authority) {
        Uri.Builder uriBuilder = new Uri.Builder();
        uriBuilder.authority(authority);
        uriBuilder.scheme(scheme);
        return uriBuilder.build();
    }

    @Override
    public Uri insert(Uri uri, ContentValues values) {

        MessageObject regMsg = new MessageObject();
        regMsg.mMsg = values.getAsString("value");
        regMsg.mType = "I";
        regMsg.mHashValue = "Dummy";
        regMsg.mKey = values.getAsString("key");

        Boolean result = checkMyPartition(regMsg.mKey);
        if (!result)
        {
            new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR,
                    regMsg.toString(), successorNode);
            return uri;
        }
        Log.d("Insert", "Inserting msg " + regMsg.toString());
        SQLiteDatabase database = db.getWritableDatabase();
        String[] selectionArgs = {values.getAsString("key")};
        Cursor cursor = database.query("simple_dht", null, "key = ?", selectionArgs, null, null, null);
        if (cursor.getCount() < 1) {
            database.insert("simple_dht", null, values);
        }
        else
        {
            database.update("simple_dht", values, "key = ?", selectionArgs);
        }
        Log.d("Insert", values.toString());
        return uri;

    }

    @Override
    public boolean onCreate() {

        db = new SimpleDHTOpenHelper(getContext(), null, null, 1);

        // Logic to get the current instance's node
        TelephonyManager tel = (TelephonyManager) getContext().getSystemService
                (Context.TELEPHONY_SERVICE);
        String portStr = tel.getLine1Number().substring(tel.getLine1Number().length() - 4);
        final String myPort = String.valueOf((Integer.parseInt(portStr) * 2));

        //Create a server task to listen to incoming msgs
        try {
            ServerSocket serverSocket = new ServerSocket(SERVER_PORT);
            new ServerTask().executeOnExecutor(AsyncTask.THREAD_POOL_EXECUTOR, serverSocket);
        } catch (IOException e) {
            Log.d("On Create", "Can't create a ServerSocket");
            return false;
        }

        //Send join msg to 54
        MessageObject joinMsg = new MessageObject("J", myPort);
        try {
            joinMsg.mHashValue = genHash(portStr);
        } catch (NoSuchAlgorithmException e) {
            Log.e("On Create", "No Such Algorithm Exception");
            return false;
        }
        new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR,joinMsg.toString(),REMOTE_PORT0);
        Log.d("On Create", "Sent join msg from " + myPort);
        Log.d("On Create", "Sent msg is " + joinMsg.toString());
        myNode = myPort;
        return true;
    }

    @Override
    public Cursor query(Uri uri, String[] projection, String selection, String[] selectionArgs,
                        String sortOrder) {
        SQLiteDatabase database = db.getReadableDatabase();
        switch (selection)
        {
            case "\"*\"":
                Cursor cursor = database.rawQuery("SELECT * from simple_dht",null);
                if(successorNode==null) {
                    return cursor;
                }
                //send message to successor
                MessageObject qAllmsg = new MessageObject();
                qAllmsg.mType = "QALL";
                qAllmsg.mOriginNode = myNode;
                qAllmsg.mHashValue = "dummy";
                qAllmsg.mKey = "QAllkey";
                qAllmsg.mMsg = "dummy";
                sendMessageToClient(qAllmsg.toString(),successorNode);
                while(!isQueryComplete) {
                    try {
                        Thread.sleep(10);
                    } catch (InterruptedException e) {
                        e.printStackTrace();
                    }
                }
                //add everything to cursor
                String [] columns = {"key","value"};
                MatrixCursor qAllCursor = new MatrixCursor(columns);
                for (String key:queryAll.keySet())
                {
                    String value = queryAll.get(key);
                    String [] row = {key,value};
                    qAllCursor.addRow(row);
                 }

                MergeCursor mergeCursor = new MergeCursor(new Cursor[]{qAllCursor,cursor});
                return mergeCursor;
            case "\"@\"":
                return database.rawQuery("SELECT * from simple_dht",null);
            default :
                Boolean isInPartition = checkMyPartition(selection);
                if(!isInPartition)
                {
                    //check in other db and wait for response
                    return querySuccessor(selection);
                }
                return queryDB(selection);
        }

    }

    public Cursor queryDB(String selection) {
        SQLiteDatabase database = db.getReadableDatabase();
        String[] sArgs = {selection};
        Cursor cursor = database.query("simple_dht", null, "key = ?", sArgs, null, null, null);
        Log.v("query", selection);
        return cursor;
    }

    public Cursor querySuccessor(String selection) {
        MessageObject queryMsg = new MessageObject();
        queryMsg.mType = "Q";
        queryMsg.mHashValue = "dummy";
        queryMsg.mKey = selection;
        queryMsg.mOriginNode = myNode;
        sendMessageToClient(queryMsg.toString(),successorNode);
        while(queryMap.get(selection) == null)
        {
            try {
                Thread.sleep(10);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }
        String [] curArray = {"key","value"};
        MatrixCursor matCursor = new MatrixCursor(curArray);
        String [] currentQuery = {selection,queryMap.get(selection)};
        matCursor.addRow(currentQuery);
        Log.v("query", selection);
        return matCursor;
    }

    @Override
    public int update(Uri uri, ContentValues values, String selection, String[] selectionArgs) {

        return 0;
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


    private class ServerTask extends AsyncTask<ServerSocket, String, Void> {

        @Override
        protected Void doInBackground(ServerSocket... sockets) {

            ServerSocket serverSocket = sockets[0];
            //logic to receive the messages and pass them to onProgressUpdate()
            Socket clientSocket = null;
            BufferedReader inMsg = null;
            assert clientSocket != null;
            try {
                while (true) {
                    clientSocket = serverSocket.accept();
                    inMsg = new BufferedReader(new InputStreamReader(clientSocket.getInputStream()));
                    String rMsg = inMsg.readLine();
                    Log.d("Raw","rMsg ->"+rMsg);
                    //process different message types
                    String[] rMsgParts = rMsg.split("::");
                    MessageObject msgObj = new MessageObject();
                    msgObj.mType = rMsgParts[0];
                    msgObj.mMsg = rMsgParts[1];
                    msgObj.mHashValue = rMsgParts[2];
                    msgObj.mKey = rMsgParts[3];

                    switch (msgObj.mType) {

                        case "J":
                            //for a join msg, recompute successor and predecessor nodes
                            handleJoinMessage(rMsgParts, msgObj);
                            break;

                        case "S":
                            //update succ and pre node info upon signal
                            handleSignal(msgObj);
                            break;

                        case "I":
                            //logic for insert messages
                            Boolean result = checkMyPartition(msgObj.mKey);
                            if (result) {
                                //insert into my db
                                Log.d("Server Task", "Redirecting to insert fun-> " + msgObj.toString());
                                ContentValues cv = new ContentValues();
                                cv.put("key", msgObj.mKey);
                                cv.put("value", msgObj.mMsg);
                                getContext().getContentResolver().insert(mUri, cv);
                            } else {
                                sendMessageToClient(msgObj.toString(),successorNode);
                            }
                            break;
                        case "Q":
                            //check in partition else forward
                            String search = msgObj.mKey;
                            msgObj.mOriginNode = rMsgParts[4];
                            Boolean isInPartition = checkMyPartition(search);
                            if (!isInPartition) {
                                sendMessageToClient(msgObj.toString(), successorNode);
                            }
                            else
                            {
                                MessageObject replyMsg = new MessageObject();
                                Cursor qCursor = queryDB(search);
                                qCursor.moveToFirst();
                                replyMsg.mMsg = qCursor.getString(1);
                                replyMsg.mType = "QR";
                                replyMsg.mOriginNode = msgObj.mOriginNode;
                                replyMsg.mKey = msgObj.mKey;
                                sendMessageToClient(replyMsg.toString(),msgObj.mOriginNode);
                            }
                            break;
                        case "QR":
                            queryMap.put(msgObj.mKey,msgObj.mMsg);
                            break;
                        case "QALL":
                            msgObj.mOriginNode = rMsgParts[4];
                            if (msgObj.mOriginNode.equals(myNode)) {
                                isQueryComplete = true;
                                break;
                            }
                            SQLiteDatabase database = db.getReadableDatabase();
                            Cursor lcursor = database.rawQuery("SELECT * from simple_dht",null);
                            MessageObject qAllRObj = new MessageObject();
                            qAllRObj.mType = "QALLR";
                            qAllRObj.mOriginNode = myNode;
                            qAllRObj.mHashValue = "dummy";
                            qAllRObj.mKey = "QAllkey";
                            String msgAll = "";
                            while(lcursor.moveToNext()) {
                               String key = lcursor.getString(0);
                               String value = lcursor.getString(1);
                               msgAll += key+"-"+value+"&";
                            }
                            qAllRObj.mMsg = msgAll;
                            sendMessageToClient(qAllRObj.toString(),msgObj.mOriginNode);
                            sendMessageToClient(msgObj.toString(),successorNode);
                            break;
                        case "QALLR":
                            if(msgObj.mMsg.length()<2) break;
                            StringBuilder processStr = new StringBuilder(msgObj.mMsg);
                            processStr.deleteCharAt(msgObj.mMsg.length()-1);
                            String [] kvRows = processStr.toString().split("&");
                            for(String row:kvRows) {
                                String [] kvpair = row.split("-");
                                queryAll.put(kvpair[0],kvpair[1]);
                            }
                            break;
                        case "D":
                            //check my partition
                            Boolean dresult = checkMyPartition(msgObj.mKey);
                            if (dresult) {
                                //insert into my db
                                Log.d("Server Task", "Redirecting to delete fun-> " + msgObj.toString());
                                String selection = msgObj.mKey;
                                getContext().getContentResolver().delete(mUri,selection,null);
                            } else {
                                sendMessageToClient(msgObj.toString(),successorNode);
                            }
                            break;

                    }
                }
            } catch (IOException e) {
                Log.d("Server Task", "Exception in Server Task");
            }
            return null;
        }

        public void handleSignal(MessageObject msgObj) {
            //for a signal msg, set successor or predecessor node
            String[] strPart = msgObj.mMsg.split("-");
            if (strPart[0].equals("setS")) {
                successorNode = strPart[1];
                Log.d("DHT", "Updated S->" + successorNode);
            } else if (strPart[0].equals("setP")) {
                predecessorNode = strPart[1];
                Log.d("DHT", "Updated P->" + predecessorNode);
            } else {
                predecessorNode = strPart[1];
                successorNode = strPart[2];
                Log.d("DHT", "Updated P->" + predecessorNode + " S->" + successorNode);
            }
        }

        public void handleJoinMessage(String[] rMsgParts, MessageObject msgObj) {
            msgObj.mHashValue = rMsgParts[2];
            publishProgress(msgObj.toString());
            String[] nodeValue = new String[2];
            nodeValue[0] = msgObj.mMsg;
            nodeValue[1] = msgObj.mHashValue;
            joinedList.add(nodeValue);
            Log.d("Server Task", "msg object is " + msgObj.toString());
            Log.d("Server Task", "added object to 54 list");
            int listSize = joinedList.size();
            if (listSize > 1) {
                Collections.sort(joinedList, new CompareHash());

                String[] ring = new String[joinedList.size()];
                String[] tempStr;
                publishProgress("/nSorted List now ");
                for (int k = 0; k < listSize; k++) {
                    tempStr = joinedList.get(k);
                    ring[k] = tempStr[0];
                    publishProgress(" " + ring[k]);
                    Log.d("Server Task", "ring[k]=" + ring[k]);
                }

                String sPreNode = null, sPostNode = null;
                for (int l = 0; l < listSize; l++) {
                    if ((nodeValue[0].equals(ring[l])) && (listSize != 1)) {
                        if (l == 0) {
                            sPreNode = ring[listSize - 1];
                            sPostNode = ring[l + 1];
                            Log.d("DHT", nodeValue[0] + " P->" + sPreNode + " S->" + sPostNode);
                        } else if (l == listSize - 1) {
                            sPreNode = ring[l - 1];
                            sPostNode = ring[0];
                            Log.d("DHT", nodeValue[0] + " P->" + sPreNode + " S->" + sPostNode);
                        } else {
                            sPreNode = ring[l - 1];
                            sPostNode = ring[l + 1];
                            Log.d("DHT", nodeValue[0] + " P->" + sPreNode + " S->" + sPostNode);
                        }
                        break;
                    }
                }
                //logic to create signal msg and send it to pre ,succ and new nodes respectively.
                MessageObject sigMsgP = new MessageObject();
                sigMsgP.mType = "S";
                sigMsgP.mMsg = "setS-" + nodeValue[0];
                new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR,
                        sigMsgP.toString(), sPreNode);

                MessageObject sigMsgS = new MessageObject();
                sigMsgS.mType = "S";
                sigMsgS.mMsg = "setP-" + nodeValue[0];
                new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR,
                        sigMsgS.toString(), sPostNode);

                MessageObject sigMsgN = new MessageObject();
                sigMsgN.mType = "S";
                sigMsgN.mMsg = "setN-" + sPreNode + "-" + sPostNode;
                new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR,
                        sigMsgN.toString(), nodeValue[0]);

                Log.d("Server Task", "Sent signal msgs from " + nodeValue[0]);
                Log.d("Server Task", "Signal msg are " + sigMsgP + " and " + sigMsgS);
            }
        }

        protected void onProgressUpdate(String... strings) {

            String strReceived = strings[0].trim();
            SimpleDhtActivity activityContext = SimpleDhtActivity.activity;
            TextView remoteTextView = (TextView) activityContext.findViewById(R.id.textView1);
            remoteTextView.append(strReceived + "\t\n");
        }
    }

    public boolean checkMyPartition(String checkString)   {

        try {
            if ((predecessorNode == null) || (successorNode == null))
            {
                return true;
            }
            Log.d("CheckPartition","Checking "+checkString);
            String keyHash = genHash(checkString);
            String preHash = genHash(String.valueOf((Integer.parseInt(predecessorNode) / 2)));
            String myNodeHash = genHash(String.valueOf((Integer.parseInt(myNode) / 2)));
            if(preHash.compareTo(myNodeHash) > 0) {
                if (keyHash.compareTo(preHash) > 0 || keyHash.compareTo(myNodeHash) <= 0) {
                    return true;
                }else {
                    return false;
                }
            } else if (keyHash.compareTo(preHash) > 0 && keyHash.compareTo(myNodeHash) <= 0) {
                return true;
            } else {
                return false;
            }
        } catch (NoSuchAlgorithmException e) {
            e.printStackTrace();
            return  true;

        }

    }

    private class ClientTask extends AsyncTask<String, Void, Void> {
        @Override
        protected Void doInBackground(String... msgs) {
            String msgToSend = msgs[0];
            String remotePort = msgs[1];
            sendMessageToClient(msgToSend, remotePort);
            Log.d("Client Task", "sending message");
            return null;
        }
    }

    private void sendMessageToClient(String msgToSend, String remotePort) {

        try {

            Socket socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}),
                    Integer.parseInt(remotePort));
            PrintWriter out = new PrintWriter(socket.getOutputStream(), true);
            out.println(msgToSend + "\r\n");
            socket.close();
        } catch (UnknownHostException e) {
            Log.d("SendMessage", "ClientTask UnknownHostException");

        } catch (IOException e) {
            Log.d("SendMessage", "ClientTask socket IOException");
        }
    }


}
