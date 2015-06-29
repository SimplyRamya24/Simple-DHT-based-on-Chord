package edu.buffalo.cse.cse486586.simpledht;

import android.content.Context;
import android.database.sqlite.SQLiteDatabase;
import android.database.sqlite.SQLiteOpenHelper;
import android.util.Log;

/**
 * Created by Ramya on 3/31/15.
 */

public class SimpleDHTOpenHelper extends SQLiteOpenHelper {

    private static final int DATABASE_VERSION = 1;
    private static final String SIMPLE_DHT_TABLE_CREATE =
            "CREATE TABLE simple_dht (key TEXT, value TEXT);";

    public SimpleDHTOpenHelper(Context context, String name,
                                    SQLiteDatabase.CursorFactory factory, int version) {
        super(context, "simple_dht", factory, DATABASE_VERSION);
    }

    @Override
    public void onUpgrade(SQLiteDatabase db,int oldVersion, int newVersion) {
        // dropping an older version of the table , if it exists
        Log.v("db onUpgrade", "simple_dht");
        db.execSQL("DROP TABLE IF EXISTS simple_dht");
        onCreate(db);
    }

    @Override
    public void onCreate(SQLiteDatabase db) {
        db.execSQL("DROP TABLE IF EXISTS simple_dht");
        db.execSQL(SIMPLE_DHT_TABLE_CREATE);
        Log.v("db onCreate","simple_dht");
    }


}
