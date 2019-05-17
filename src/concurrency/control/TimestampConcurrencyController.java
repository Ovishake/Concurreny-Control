package concurrency.control;

import java.util.HashMap;
import java.util.Date;
//import java.sql.Timestamp;

import concurrency.StorageManager;

/**
 * The {@code TimestampConcurrencyController} class implements timestamp-based concurrency control.
 * 
 * @author Jeong-Hyon Hwang (jhh@cs.albany.edu)
 *
 * @param <V>
 *            the type of data items
 */
public class TimestampConcurrencyController<V> extends ConcurrencyController<V> {

	/**
	 * A map that associates {@code Transaction} IDs with timestamps.
	 */
	HashMap<Integer, Integer> tID2timestamp = new HashMap<Integer, Integer>();
	
	HashMap<Integer, Integer> dID2readtimestamp = new HashMap<Integer, Integer>();
	
	HashMap<Integer, Integer> dID2writetimestamp = new HashMap<Integer, Integer>();
	
	/**
	 * A MAP THAT ASSOCIATES {@code DATAITEM} IDs with Timestamps.
	 * 
	 * Write timestamps are specific to data fields
	 */
//	HashMap<Integer, Integer> write_time_stamp = new HashMap<Integer, Integer>();

	/**
	 * The number of {@code Transaction}s that have been registered so far.
	 */
	int count = 0;

	/**
	 * Constructs a {@code TimestampConcurrencyController}.
	 * 
	 * @param storageManager
	 *            a {@code StorageManager}
	 */
	public TimestampConcurrencyController(StorageManager<V> storageManager) {
		super(storageManager);
	}

	/**
	 * Registers a {@code Transaction}.
	 * 
	 * @param tID
	 *            the ID of the {@code Transaction}
	 */
	public void register(int tID) {
		tID2timestamp.put(tID, count++);
	}

	/**
	 * Handles a read request.
	 * 
	 * @param tID
	 *            the ID of the {@code Transaction} that has made the request
	 * @param dID
	 *            the ID of the data item for which the request was made
	 * @throws InvalidTransactionIDException
	 *             if an invalid {@code Transaction} ID is given
	 * @throws AbortException
	 *             if the request cannot be permitted and thus the related {@code Transaction} must be aborted
	 */
	@Override
	public V read(int tID, int dID) throws InvalidTransactionIDException, AbortException {
		
		Integer timestamp = tID2timestamp.get(tID); 
		
		Integer write_time_stamp = dID2writetimestamp.get(dID);
		
		
		if(write_time_stamp == null)
		{
			dID2readtimestamp.put(dID, timestamp);
			register(tID);
			return super.read(tID, dID);
		}
		
		else
		{
			if(timestamp < write_time_stamp)
			{
				throw new AbortException();
			}
			
			else if(timestamp > write_time_stamp)
			{
				register(tID);
				dID2readtimestamp.put(dID, timestamp);
			}
		}
		
		return super.read(tID, dID);
		
	}

	/**
	 * Handles a write request.
	 * 
	 * @param tID
	 *            the ID of the {@code Transaction} that has made the request
	 * @param dID
	 *            the ID of the data item for which the request was made
	 * @param dValue
	 *            the value of the data item for which the request was made
	 * @throws InvalidTransactionIDException
	 *             if an invalid {@code Transaction} ID is given
	 * @throws AbortException
	 *             if the request cannot be permitted and thus the related {@code Transaction} must be aborted
	 */
	@Override
	public void write(int tID, int dID, V dValue) throws InvalidTransactionIDException, AbortException
	{
		Integer timestamp = tID2timestamp.get(tID); 
		
		Integer write_time_stamp = dID2writetimestamp.get(dID);
		
		Integer read_time_stamp = dID2readtimestamp.get(dID);
		
		if(write_time_stamp == null || read_time_stamp == null)
		{
			dID2writetimestamp.put(dID, timestamp);
			super.write(tID, dID, dValue);
			super.commit(tID);
			register(tID);
		}
		
		else if(read_time_stamp > timestamp)
		{
			super.rollback(tID);
			throw new AbortException();
		}
		
		else if(write_time_stamp > timestamp)
		{
			super.rollback(tID);
			throw new AbortException();
		}
		else
		{
			dID2writetimestamp.put(dID, timestamp);
			super.write(tID, dID, dValue);
			super.commit(tID);
			register(tID);
		}
		
	}

}
