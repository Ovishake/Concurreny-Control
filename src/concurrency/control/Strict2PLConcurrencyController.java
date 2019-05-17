package concurrency.control;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import concurrency.StorageManager;

/**
 * The {@code Strict2PLConcurrencyController} class implements the strict 2 phase-locking protocol.
 * 
 * @author Jeong-Hyon Hwang (jhh@cs.albany.edu)
 *
 * @param <V>
 *            the type of data items
 */
public class Strict2PLConcurrencyController<V> extends ConcurrencyController<V> {

	/**
	 * A map that associates data IDs with locks.
	 */
	HashMap<Integer, ReentrantReadWriteLock> dID2lock = new HashMap<Integer, ReentrantReadWriteLock>();
	
	ArrayList<ReentrantReadWriteLock> all_readlocks = new ArrayList<ReentrantReadWriteLock>();
	
	ArrayList<ReentrantReadWriteLock> all_writelocks = new ArrayList<ReentrantReadWriteLock>();

	/**
	 * Constructs a {@code Strict2PLConcurrencyController}.
	 * 
	 * @param storageManager
	 *            a {@code StorageManager}
	 */
	public Strict2PLConcurrencyController(StorageManager<V> storageManager) {
		super(storageManager);
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
		/**
		 * https://howtodoinjava.com/java/multi-threading/how-to-use-locks-in-java-java-util-concurrent-locks-lock-tutorial-and-example/
		 * 
		 */
		/**
		 * import import java.util.concurrent.locks.Lock;
		 */
		
		
		ReentrantReadWriteLock lock = dID2lock.get(dID);
		
		lock.readLock().lock();
		
		all_readlocks.add(lock);
		
		V read_data = super.read(tID, dID);
		
		return read_data;
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
		ReentrantReadWriteLock lock = dID2lock.get(dID);

		all_writelocks.add(lock);
		
		super.write(tID, dID, dValue);
		
	}

	/**
	 * Rolls back the specified {@code Transaction}.
	 * 
	 * @param tID
	 *            the ID of the {@code Transaction} to roll back.
	 */
	@Override
	public void rollback(int tID) {
		super.rollback(tID);
		releaseAllRemainingLocks(tID);
	}

	/**
	 * Commits the specified {@code Transaction}.
	 * 
	 * @param tID
	 *            the ID of the {@code Transaction} to commit
	 */
	@Override
	public void commit(int tID) {
		super.commit(tID);
		releaseAllRemainingLocks(tID);
	}

	/**
	 * Releases all remaining {@code Lock}s granted to the specified {@code Transaction}.
	 * 
	 * @param tID
	 *            the ID of the {@code Transaction}
	 */
	protected void releaseAllRemainingLocks(int tID) {
		// use a member variable you added
		//call unlock on all the lock that I used.
		//dont release the lock in the read or write
		//unlock it in release all locks
		//in deadlocks you kill the transactions
		//if it is not safe you just block the operation
		
		Iterator<ReentrantReadWriteLock> itr1 = all_readlocks.iterator();
		Iterator<ReentrantReadWriteLock> itr2 = all_readlocks.iterator();
		
		while(itr1.hasNext())
		{
			ReentrantReadWriteLock locker = itr1.next();
			
			locker.readLock().unlock();
		}
		
		while(itr2.hasNext())
		{
			ReentrantReadWriteLock locker = itr2.next();
			
			locker.writeLock().unlock();
		}
		
	}

	/**
	 * Returns the {@code Lock} associated with the specified data item.
	 * 
	 * @param dID
	 *            the ID of the data item
	 * @return the {@code Lock} associated with the specified data item
	 */
	protected ReentrantReadWriteLock getLock(int dID) {
		// use dID2lock
		return dID2lock.get(dID);
	}
}
