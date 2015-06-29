package edu.buffalo.cse.cse486586.simpledht;

import java.util.ArrayList;
import java.util.Comparator;

/**
 * Created by ramya on 4/1/15.
 */
public class CompareHash implements Comparator <String[]> {

    public int compare(String[] S1,String[] S2) {
        return S1[1].compareTo(S2[1]);
    }
}
