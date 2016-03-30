package com.flipkart.aesop.bootstrap.mysql;


import com.flipkart.aesop.event.AbstractEvent;

import com.github.shyiko.mysql.binlog.event.Event;

/* This is a wrapper class for Abstract Event and SCN. This is to track SCNs with each
 * generated event .
 */
public class MysqlEvent
{
    /* Associated SCN of the Transformed Source Event */
    private long scn;
    /* Abstract Event */
    private AbstractEvent abstractEvent;
    // event 
    private Event event;


    public MysqlEvent(long scn,Event event) {
        this.scn = scn;
        this.event = event;
    }

    public long getScn() {
        return scn;
    }

    public Event getAbstractEvent() {
        return event;
    }


    // public MysqlEvent(long scn,AbstractEvent abstractEvent) {
    //     this.scn = scn;
    //     this.abstractEvent = abstractEvent;
    // }

    // public long getScn() {
    //     return scn;
    // }

    // public AbstractEvent getAbstractEvent() {
    //     return abstractEvent;
    // }
}
