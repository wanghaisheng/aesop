/*
 * Copyright 2012-2015, the original author or authors.
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * http://www.apache.org/licenses/LICENSE-2.0
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.flipkart.aesop.bootstrap.mysql.eventprocessor.impl;

import com.flipkart.aesop.bootstrap.mysql.eventlistener.OpenReplicationListener;
import com.flipkart.aesop.bootstrap.mysql.eventprocessor.BinLogEventProcessor;
import com.flipkart.aesop.bootstrap.mysql.txnprocessor.MysqlTransactionManager;
import com.github.shyiko.mysql.binlog.event.TableMapEventData;
import com.google.code.or.binlog.BinlogEventV4;
import com.google.code.or.binlog.impl.event.TableMapEvent;
import org.trpr.platform.core.impl.logging.LogFactory;
import org.trpr.platform.core.spi.logging.Logger;
import com.github.shyiko.mysql.binlog.event.Event;

/**
 * The <code>TableMapEventProcessor</code> processes TableMapEvent from source. This event gives table related details
 * for the started transaction.
 * @author Shoury B
 * @version 1.0, 07 Mar 2014
 */
public class TableMapEventProcessor implements BinLogEventProcessor
{
	/** Logger for this class */
	private static final Logger LOGGER = LogFactory.getLogger(TableMapEventProcessor.class);

	@Override
	public void process(Event event, OpenReplicationListener listener) throws Exception
	{
		MysqlTransactionManager manager = listener.getMysqlTransactionManager();
		if (!manager.isBeginTxnSeen())
		{
			LOGGER.warn("Skipping event (" + event + ") as this is before the start of first transaction");
			return;
		}
		TableMapEventData tableMapEventData =  event.getData();
		String newTableName =
				tableMapEventData.getDatabase().toLowerCase() + "."
		                + tableMapEventData.getTable().toLowerCase();
		Long newTableId = tableMapEventData.getTableId();

		// storing the tableId to tableName mapping
		manager.getMysqlTableIdToTableNameMap().put(newTableId, newTableName);

	}
}
