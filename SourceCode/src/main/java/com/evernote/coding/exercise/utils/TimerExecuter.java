package com.evernote.coding.exercise.utils;

import java.util.Timer;
import java.util.TimerTask;

/**
 * Created by SuryaSelvaraj on 6/19/17.
 */
public class TimerExecuter {
    public static void main (String[] args) {
        TimerTask task = new TimerTaskExample();
        Timer timer = new Timer();
        timer.scheduleAtFixedRate(task, 0L, 10000L);
    }
}
