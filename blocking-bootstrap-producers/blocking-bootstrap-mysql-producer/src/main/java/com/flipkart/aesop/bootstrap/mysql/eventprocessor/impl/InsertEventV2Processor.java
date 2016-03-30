package com.flipkart.aesop.bootstrap.mysql.eventprocessor.impl;

import com.flipkart.aesop.bootstrap.mysql.eventlistener.OpenReplicationListener;
import com.flipkart.aesop.bootstrap.mysql.eventprocessor.BinLogEventProcessor;
import com.github.shyiko.mysql.binlog.event.WriteRowsEventData;
import com.google.code.or.binlog.BinlogEventV4;
import com.google.code.or.binlog.impl.event.WriteRowsEventV2;
import com.linkedin.databus.core.DbusOpcode;
import org.trpr.platform.core.impl.logging.LogFactory;
import org.trpr.platform.core.spi.logging.Logger;
import com.github.shyiko.mysql.binlog.event.Event;

/**
 * The <code>InsertEvent2Processor</code> processes WriteRowsEventV2 from source. This event is received if there is any
 * insert operation on the source.
 * @author jagadeesh.huliyar
 */
public class InsertEventV2Processor implements BinLogEventProcessor
{
	/** Logger for this class */
	private static final Logger LOGGER = LogFactory.getLogger(InsertEventV2Processor.class);

	@Override
	public void process(Event event, OpenReplicationListener listener) throws Exception
	{
		if (!listener.getMysqlTransactionManager().isBeginTxnSeen())
		{
			LOGGER.warn("Skipping event (" + event + ") as this is before the start of first transaction");
			return;
		}
		LOGGER.debug("Insert Event Received : " + event);
		WriteRowsEventData wre =  event.getData();
		listener.getMysqlTransactionManager().performChanges(wre.getTableId(), event.getHeader(), wre.getRows(),
				DbusOpcode.UPSERT);
		LOGGER.debug("Insertion Successful for  " + event.getHeader().getHeaderLength() + " . Data inserted : "
				+ wre.getRows());
	}

}
