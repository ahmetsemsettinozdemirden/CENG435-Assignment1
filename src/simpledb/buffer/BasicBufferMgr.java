/*
 * Mehmet Arda Aksoydan - 230201029
 * Ahmet Þemsettin Özdemirden - 230201043
 */

package simpledb.buffer;

import java.util.HashMap;
import java.util.LinkedList;
import java.util.Map;
import java.util.Queue;
import java.util.Set;

import simpledb.file.*;

/**
 * Manages the pinning and unpinning of buffers to blocks.
 * @author Edward Sciore
 *
 */
class BasicBufferMgr {
   private Queue<Buffer> unpinnedBuffers;
   private Map<Block, Buffer> allocatedBuffers;
   private int numAvailable;
   
   /**
    * Creates a buffer manager.
    * This constructor depends on both the {@link FileMgr} and
    * {@link simpledb.log.LogMgr LogMgr} objects 
    * that it gets from the class
    * {@link simpledb.server.SimpleDB}.
    * Those objects are created during system initialization.
    * Thus this constructor cannot be called until 
    * {@link simpledb.server.SimpleDB#initFileAndLogMgr(String)} or
    * is called first.
    */
   BasicBufferMgr() {
	   this.unpinnedBuffers = new LinkedList<Buffer>();
	   this.allocatedBuffers = new HashMap<Block, Buffer>();
	   this.numAvailable = 8;
   }
   
   /**
    * Returns unpinned buffers queue with current data.
    */
   public Queue<Buffer> getUnpinnedBuffers() {
	   return unpinnedBuffers;
   }
   
   /**
    * Returns allocated buffers map with current data.
    */
   public Map<Block, Buffer> getAllocatedBuffers() {
	   return allocatedBuffers;
   }

/**
    * Flushes the dirty buffers modified by the specified transaction.
    * @param txnum the transaction's id number
    */
   synchronized void flushAll(int txnum) {
	   Set<Map.Entry<Block, Buffer>> viewMap = this.allocatedBuffers.entrySet();
		 
	   for (Map.Entry<Block, Buffer> me : viewMap) {
			  Buffer buffer = me.getValue();
			  if (buffer.isModifiedBy(txnum)){ buffer.flush(); }
	   }	  
   }
   
   /**
    * Pins a buffer to the specified block. 
    * If there is already a buffer assigned to that block
    * then that buffer is used;  
    * otherwise, an unpinned buffer from the pool is chosen.
    * Creates a new buffer if unpinned buffer queue is empty.
    * @param blk a reference to a disk block
    * @return the pinned buffer
    */
   synchronized Buffer pin(Block blk) {
      Buffer buffer = findExistingBuffer(blk);
      if (buffer == null) {
         buffer = chooseUnpinnedBuffer();
         if (buffer == null) {
            buffer = new Buffer(blk.number());
            buffer.assignToBlock(blk);
            this.allocatedBuffers.put(blk, buffer);
         }else {
        	 this.allocatedBuffers.remove(buffer.block());
        	 buffer.assignToBlock(blk);
        	 this.allocatedBuffers.put(blk, buffer);	 
         }
      }
      if(!buffer.isPinned()) {
    	  buffer.pin();
    	  this.numAvailable--;
      }  
      return buffer;
   }
   
   /**
    * Allocates a new block in the specified file, and
    * pins a buffer to it. 
    * Returns null (without allocating the block) if 
    * there are no available buffers.
    * @param filename the name of the file
    * @param fmtr a pageformatter object, used to format the new block
    * @return the pinned buffer
    */
   synchronized Buffer pinNew(String filename, PageFormatter fmtr) {
      Buffer buff = chooseUnpinnedBuffer();
      if (buff == null)
         return null;
      buff.assignToNew(filename, fmtr);
      buff.pin();
      return buff;
   }
   
   /**
    * Unpins the specified buffer and adds to unpinned buffers queue.
    * @param buff the buffer to be unpinned
    */
   synchronized void unpin(Buffer buff) {
	   if(buff.isPinned()) {
		   buff.unpin();
		   this.numAvailable++;
		   this.unpinnedBuffers.add(buff);
	   }
   }
   
   /**
    * Returns the number of available (i.e. unpinned) buffers.
    * @return the number of available buffers
    */
   int available() {
      return numAvailable;
   }
   
   /**
    * Returns existing buffer with given block value if it exists.
    * @param blk
    */
   private Buffer findExistingBuffer(Block blk) {
	  Buffer existingBuffer = this.allocatedBuffers.get(blk);
	  
	  return existingBuffer;
   }
   
   /**
    * Returns the head of unpinned buffer queue after its removal from queue.
    */
   private Buffer chooseUnpinnedBuffer() {
      Buffer replacementBuffer = null;
      if(this.unpinnedBuffers.size() > 0)  
    	  replacementBuffer = this.unpinnedBuffers.remove();
      
      return replacementBuffer;
   }
   
}