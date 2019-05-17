package concurrency.control;

import java.util.Date;
import concurrency.StorageManager;
import concurrency.control.ConcurrencyController.AbortException;

/**
 * The {@code TimestampConcurrencyControllerTWR} class implements timestamp-based concurrency control with Thomas' write rule.
 * 
 * @author Jeong-Hyon Hwang (jhh@cs.albany.edu)
 *
 * @param <V>
 *            the type of data items
 */
public class TimestampConcurrencyControllerTWR<V> extends TimestampConcurrencyController<V> {

	/**
	 * Constructs a {@code TimestampConcurrencyControllerTWR}.
	 * 
	 * @param storageManager
	 *            a {@code StorageManager}
	 */
	public TimestampConcurrencyControllerTWR(StorageManager<V> storageManager) {
		super(storageManager);
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
	public void write(int tID, int dID, V dValue) throws InvalidTransactionIDException, AbortException {
//		Integer timestamp = tID2timestamp.get(tID); // the timestamp of the transaction specified by tID
Integer timestamp = tID2timestamp.get(tID); 
		
		Integer write_time_stamp = dID2writetimestamp.get(dID);
		
		Integer read_time_stamp = dID2readtimestamp.get(dID);
		
		if(write_time_stamp == null || read_time_stamp == null)
		{
			dID2writetimestamp.put(dID, timestamp);
			super.write(tID, dID, dValue);
		}
		
		else if(read_time_stamp > timestamp)
		{
			super.rollback(tID);
			throw new AbortException();
		}
		
		else if(write_time_stamp > timestamp)
		{
			/**
			 * Thomas Write Rule;
			 * 
			 * Skip;
			 * 
			 * Psuedo-code in Wikipedia
			 */
		}
		else
		{
			dID2writetimestamp.put(dID, timestamp);
			super.write(tID, dID, dValue);
			super.commit(tID);
		}
		
	}

}
