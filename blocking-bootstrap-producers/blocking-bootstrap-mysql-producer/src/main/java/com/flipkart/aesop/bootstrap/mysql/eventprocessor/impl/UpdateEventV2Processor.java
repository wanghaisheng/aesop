package com.flipkart.aesop.bootstrap.mysql.eventprocessor.impl;

import com.flipkart.aesop.bootstrap.mysql.eventlistener.OpenReplicationListener;
import com.flipkart.aesop.bootstrap.mysql.eventprocessor.BinLogEventProcessor;
import com.flipkart.aesop.bootstrap.mysql.txnprocessor.MysqlTransactionManager;
import com.github.shyiko.mysql.binlog.event.UpdateRowsEventData;
import com.google.code.or.binlog.BinlogEventV4;
import com.google.code.or.binlog.impl.event.UpdateRowsEventV2;
import com.google.code.or.common.glossary.Pair;
import com.google.code.or.common.glossary.Row;
import com.linkedin.databus.core.DbusOpcode;
import org.trpr.platform.core.impl.logging.LogFactory;
import org.trpr.platform.core.spi.logging.Logger;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import com.github.shyiko.mysql.binlog.event.Event;

/**
 * The <code>UpdateEvent2Processor</code> processes UpdateRowsEventV2 from source. This event is received if there is
 * any
 * update operation on the source.
 * @author jagadeesh.huliyar
 */
public class UpdateEventV2Processor implements BinLogEventProcessor
{
	/** Logger for this class */
	private static final Logger LOGGER = LogFactory.getLogger(UpdateEventV2Processor.class);

	@Override
	public void process(Event event, OpenReplicationListener listener) throws Exception
	{
		MysqlTransactionManager manager = listener.getMysqlTransactionManager();
		if (!manager.isBeginTxnSeen())
		{
			LOGGER.warn("Skipping event (" + event + ") as this is before the start of first transaction");
			return;
		}
		LOGGER.debug("Update Event Received : " + event);

		UpdateRowsEventData updateRowsEventData = event.getData();
		List<Map.Entry<Serializable[], Serializable[]>> listOfPairs = updateRowsEventData.getRows();

//		List<Pair<Row>> listOfPairs = updateRowsEvent.getRows();
//		List<Row> rowList = new ArrayList<Row>(listOfPairs.size());
//		for (Pair<Row> pair : listOfPairs)
//		{
//			Row row = pair.getAfter();
//			rowList.add(row);
//		}

		List<Map.Entry<Serializable[], Serializable[]>> rows = updateRowsEventData.getRows();


			List<Serializable[]> inserValues = new ArrayList<Serializable[]>();
			for (Map.Entry<Serializable[], Serializable[]> rowItem : rows) {
				inserValues.add(rowItem.getValue());
			}
//			Object[][] params = inserValues.toArray(new Object[inserValues.size()][]);
		manager.performChanges(updateRowsEventData.getTableId(), event.getHeader(), inserValues, DbusOpcode.UPSERT);
		LOGGER.debug("Update Successful for  " + event.getHeader().getHeaderLength() + " . Data updated : " + inserValues);
	}
}
