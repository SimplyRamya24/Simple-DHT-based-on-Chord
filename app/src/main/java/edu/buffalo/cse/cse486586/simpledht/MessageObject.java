package edu.buffalo.cse.cse486586.simpledht;

import java.util.ArrayList;
import java.util.HashMap;

/**
 * Created by ramya on 3/31/15.
 */
public class MessageObject {

    public String mType;
    public String mMsg;
    public String mHashValue;
    public String mKey;
    public String mOriginNode;


    public MessageObject(String mType, String mMsg, String mHashValue) {
        this.mType = mType;
        this.mMsg = mMsg;
        this.mHashValue = mHashValue;
    }

    public MessageObject(String mType, String mMsg) {
        this.mType = mType;
        this.mMsg = mMsg;
    }

    public MessageObject() {

    }

    @Override
    public String toString() {
        return  mType + "::"+
                mMsg + "::"+
                mHashValue +"::"+
                mKey +"::"+
                mOriginNode;

    }


}

