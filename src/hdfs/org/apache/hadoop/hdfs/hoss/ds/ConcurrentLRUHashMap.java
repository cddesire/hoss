package org.apache.hadoop.hdfs.hoss.ds;
import java.io.IOException;
import java.io.Serializable;
import java.util.*;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.locks.ReentrantLock;

/**
 *
 * @param <K>
 * @param <V>
 */
public class ConcurrentLRUHashMap<K, V> extends AbstractMap<K, V> implements
        ConcurrentMap<K, V>, Serializable {

       
    private static final long serialVersionUID = -5031526786765467550L;

    static final int DEFAULT_SEGEMENT_MAX_CAPACITY = 100;

    /**
     * The default load factor for this table, used when not otherwise specified
     * in a constructor.
     */
    static final float DEFAULT_LOAD_FACTOR = 0.75f;

    /**
     * The default concurrency level for this table, used when not otherwise
     * specified in a constructor.
     */
    static final int DEFAULT_CONCURRENCY_LEVEL = 16;

    /**
     * The maximum capacity, used if a higher value is implicitly specified by
     * either of the constructors with arguments. MUST be a power of two <=
     * 1<<30 to ensure that entries are indexable using ints.
     */
    static final int MAXIMUM_CAPACITY = 1 << 30;

    /**
     * The maximum number of segments to allow; used to bound constructor
     * arguments.
     */
    static final int MAX_SEGMENTS = 1 << 16; // slightly conservative

   
    static final int RETRIES_BEFORE_LOCK = 2;

        /* ---------------- Fields -------------- */

    /**
     * Mask value for indexing into segments. The upper bits of a key's hash
     * code are used to choose the segment.
     */
    final int segmentMask;

    /**
     * Shift value for indexing within segments.
     */
    final int segmentShift;

    /**
     * The segments, each of which is a specialized hash table
     */
    final Segment<K, V>[] segments;

    transient Set<K> keySet;
    transient Set<Map.Entry<K, V>> entrySet;
    transient Collection<V> values;

       
    private static int hash(int h) {
        // Spread bits to regularize both segment and index locations,
        // using variant of single-word Wang/Jenkins hash.
        h += (h << 15) ^ 0xffffcd7d;
        h ^= (h >>> 10);
        h += (h << 3);
        h ^= (h >>> 6);
        h += (h << 2) + (h << 14);
        return h ^ (h >>> 16);
    }

    /**
     * Returns the segment that should be used for key with given hash
     *
     * @param hash
     *            the hash code for the key
     * @return the segment
     */
    final Segment<K, V> segmentFor(int hash) {
        return segments[(hash >>> segmentShift) & segmentMask];
    }


    static final class HashEntry<K, V> {
        final K key;

        final int hash;

        volatile V value;

        final HashEntry<K, V> next;

        HashEntry<K, V> linkNext;

        HashEntry<K, V> linkPrev;

        AtomicBoolean dead;

        long overtime;

        HashEntry(K key, int hash, HashEntry<K, V> next, V value,long t) {
            this.key = key;
            this.hash = hash;
            this.next = next;
            this.value = value;
            this.overtime=t;
            dead = new AtomicBoolean(false);
        }

        @SuppressWarnings("unchecked")
        static final <K, V> HashEntry<K, V>[] newArray(int i) {
            return new HashEntry[i];
        }
    }

    /**
     * @param <K>
     * @param <V>
     */
    static final class Segment<K, V> extends ReentrantLock implements Serializable {

        private static final long serialVersionUID = 2249069246763182397L;

        /**
         * The number of elements in this segment's region.
         */
        transient volatile int count;

        /**
         * Number of updates that alter the size of the table. This is used
         * during bulk-read methods to make sure they see a consistent snapshot:
         * If modCounts change during a traversal of segments computing size or
         * checking containsValue, then we might have an inconsistent view of
         * state so (usually) must retry.
         */
        transient int modCount;

        /**
         * The table is rehashed when its size exceeds this threshold. (The
         * value of this field is always <tt>(int)(capacity *
         * loadFactor)</tt>.)
         */
        transient int threshold;

        /**
         * The per-segment table.
         */
        transient volatile HashEntry<K, V>[] table;

        /**
         * The load factor for the hash table. Even though this value is same
         * for all segments, it is replicated to avoid needing links to outer
         * object.
         *
         * @serial
         */
        final float loadFactor;

        transient final HashEntry<K, V> header;

        final int maxCapacity;

        Segment(int maxCapacity, float lf, ConcurrentLRUHashMap<K, V> lruMap) {
            this.maxCapacity = maxCapacity;
            loadFactor = lf;
            setTable(HashEntry.<K, V> newArray(maxCapacity));
            header = new HashEntry<K, V>(null, -1, null, null,0);
            header.linkNext = header;
            header.linkPrev = header;
        }

        @SuppressWarnings("unchecked")
        static final <K, V> Segment<K, V>[] newArray(int i) {
            return new Segment[i];
        }

        /**
         * Sets table to new HashEntry array. Call only while holding lock or in
         * constructor.
         */
        void setTable(HashEntry<K, V>[] newTable) {
            threshold = (int) (newTable.length * loadFactor);
            table = newTable;
        }

        /**
         * Returns properly casted first entry of bin for given hash.
         */
        HashEntry<K, V> getFirst(int hash) {
            HashEntry<K, V>[] tab = table;
            return tab[hash & (tab.length - 1)];
        }

        /**
         * Reads value field of an entry under lock. Called if value field ever
         * appears to be null. This is possible only if a compiler happens to
         * reorder a HashEntry initialization with its table assignment, which
         * is legal under memory model but is not known to ever occur.
         */
        V readValueUnderLock(HashEntry<K, V> e) {
            lock();
            try {
                return e.value;
            } finally {
                unlock();
            }
        }

         /* Specialized implementations of map methods */

        V get(Object key, int hash) {
            lock();
            try {
                if (count != 0) { // read-volatile
                    HashEntry<K, V> e = getFirst(hash);
                    while (e != null) {
                        if (e.hash == hash && key.equals(e.key)) {
                            long t=System.currentTimeMillis();
                            if(e.overtime!=0)
                                if(t>=e.overtime){removeNode(e);return null;}

                            V v = e.value;
                            moveNodeToHeader(e);
                            if (v != null)
                                return v;
                            return readValueUnderLock(e); // recheck
                        }
                        e = e.next;
                    }
                }
                return null;
            } finally {
                unlock();
            }
        }

        /**
         *
         * @param entry
         */
        void moveNodeToHeader(HashEntry<K, V> entry) {
            removeNode(entry);
            addBefore(entry, header);
        }

        /**
         *
         * @param newEntry
         * @param entry
         */
        void addBefore(HashEntry<K, V> newEntry, HashEntry<K, V> entry) {
            newEntry.linkNext = entry;
            newEntry.linkPrev = entry.linkPrev;
            entry.linkPrev.linkNext = newEntry;
            entry.linkPrev = newEntry;
        }

        /**
         *
         * @param entry
         */
        void removeNode(HashEntry<K, V> entry) {
            entry.linkPrev.linkNext = entry.linkNext;
            entry.linkNext.linkPrev = entry.linkPrev;
        }

        boolean containsKey(Object key, int hash) {
            lock();
            try {
                if (count != 0) { // read-volatile
                    HashEntry<K, V> e = getFirst(hash);
                    while (e != null) {
                        if (e.hash == hash && key.equals(e.key)) {
                            moveNodeToHeader(e);
                            return true;
                        }

                        e = e.next;
                    }
                }
                return false;
            } finally {
                unlock();
            }
        }

        boolean containsValue(Object value) {
            lock();
            try {
                if (count != 0) { // read-volatile
                    HashEntry<K, V>[] tab = table;
                    int len = tab.length;
                    for (int i = 0; i < len; i++) {
                        for (HashEntry<K, V> e = tab[i]; e != null; e = e.next) {
                            V v = e.value;
                            if (v == null) // recheck
                                v = readValueUnderLock(e);
                            if (value.equals(v)) {
                                moveNodeToHeader(e);
                                return true;
                            }

                        }
                    }
                }
                return false;
            } finally {
                unlock();
            }
        }

        boolean replace(K key, int hash, V oldValue, V newValue) {
            lock();
            try {
                HashEntry<K, V> e = getFirst(hash);
                while (e != null && (e.hash != hash || !key.equals(e.key)))
                    e = e.next;

                boolean replaced = false;
                if (e != null && oldValue.equals(e.value)) {
                    replaced = true;
                    e.value = newValue;
                    moveNodeToHeader(e);
                }
                return replaced;
            } finally {
                unlock();
            }
        }

        V replace(K key, int hash, V newValue) {
            lock();
            try {
                HashEntry<K, V> e = getFirst(hash);
                while (e != null && (e.hash != hash || !key.equals(e.key)))
                    e = e.next;

                V oldValue = null;
                if (e != null) {
                    oldValue = e.value;
                    e.value = newValue;
                    // 移动到头部
                    moveNodeToHeader(e);
                }
                return oldValue;
            } finally {
                unlock();
            }
        }

        V put(K key, int hash, V value, boolean onlyIfAbsent,long t) {
            lock();
            try {
                int c = count;
                if (c++ > threshold) // ensure capacity
                    rehash();
                HashEntry<K, V>[] tab = table;
                int index = hash & (tab.length - 1);
                HashEntry<K, V> first = tab[index];
                HashEntry<K, V> e = first;
                while (e != null && (e.hash != hash || !key.equals(e.key)))
                    e = e.next;

                V oldValue = null;
                if (e != null) {
                    oldValue = e.value;
                    if (!onlyIfAbsent) {
                        e.value = value;
                        e.overtime=t;
                        moveNodeToHeader(e);
                    }
                } else {
                    oldValue = null;
                    ++modCount;
                    HashEntry<K, V> newEntry = new HashEntry<K, V>(key, hash, first, value,t);
                    tab[index] = newEntry;
                    count = c; // write-volatile
                    addBefore(newEntry, header);
                    removeEldestEntry();
                }
                return oldValue;
            } finally {
                unlock();
            }
        }

        void rehash() {
            HashEntry<K, V>[] oldTable = table;
            int oldCapacity = oldTable.length;
            if (oldCapacity >= MAXIMUM_CAPACITY)
                return;


            HashEntry<K, V>[] newTable = HashEntry.newArray(oldCapacity << 1);
            threshold = (int) (newTable.length * loadFactor);
            int sizeMask = newTable.length - 1;
            for (int i = 0; i < oldCapacity; i++) {
                // We need to guarantee that any existing reads of old Map can
                // proceed. So we cannot yet null out each bin.
                HashEntry<K, V> e = oldTable[i];

                if (e != null) {
                    HashEntry<K, V> next = e.next;
                    int idx = e.hash & sizeMask;

                    // Single node on list
                    if (next == null)
                        newTable[idx] = e;

                    else {
                        // Reuse trailing consecutive sequence at same slot
                        HashEntry<K, V> lastRun = e;
                        int lastIdx = idx;
                        for (HashEntry<K, V> last = next; last != null; last = last.next) {
                            int k = last.hash & sizeMask;
                            if (k != lastIdx) {
                                lastIdx = k;
                                lastRun = last;
                            }
                        }
                        newTable[lastIdx] = lastRun;

                        // Clone all remaining nodes
                        for (HashEntry<K, V> p = e; p != lastRun; p = p.next) {
                            int k = p.hash & sizeMask;
                            HashEntry<K, V> n = newTable[k];
                            HashEntry<K, V> newEntry = new HashEntry<K, V>(
                                    p.key, p.hash, n, p.value,p.overtime);
                            // update by Noah
                            newEntry.linkNext = p.linkNext;
                            newEntry.linkPrev = p.linkPrev;
                            newTable[k] = newEntry;
                        }
                    }
                }
            }
            table = newTable;
        }

        /**
         * Remove; match on key only if value null, else match both.
         */
        V remove(Object key, int hash, Object value) {
            lock();
            try {
                int c = count - 1;
                HashEntry<K, V>[] tab = table;
                int index = hash & (tab.length - 1);
                HashEntry<K, V> first = tab[index];
                HashEntry<K, V> e = first;
                while (e != null && (e.hash != hash || !key.equals(e.key)))
                    e = e.next;

                V oldValue = null;
                if (e != null) {
                    V v = e.value;
                    if (value == null || value.equals(v)) {
                        oldValue = v;
                        // All entries following removed node can stay
                        // in list, but all preceding ones need to be
                        // cloned.
                        ++modCount;
                        HashEntry<K, V> newFirst = e.next;
                        for (HashEntry<K, V> p = first; p != e; p = p.next) {
                            newFirst = new HashEntry<K, V>(p.key, p.hash,
                                    newFirst, p.value,p.overtime);
                            newFirst.linkNext = p.linkNext;
                            newFirst.linkPrev = p.linkPrev;
                        }
                        tab[index] = newFirst;
                        count = c; // write-volatile
                        // 移除节点
                        removeNode(e);
                    }
                }
                return oldValue;
            } finally {
                unlock();
            }
        }

        void removeEldestEntry() {
            if (count > this.maxCapacity) {
                HashEntry<K, V> eldest = header.linkNext;
                remove(eldest.key, eldest.hash, null);
            }
        }

        void clear() {
            if (count != 0) {
                lock();
                try {
                    HashEntry<K, V>[] tab = table;
                    for (int i = 0; i < tab.length; i++)
                        tab[i] = null;
                    ++modCount;
                    count = 0; // write-volatile
                } finally {
                    unlock();
                }
            }
        }
    }

    /**
     *
     * @param segementCapacity
     * @param loadFactor
     * @param concurrencyLevel
     */
    public ConcurrentLRUHashMap(int segementCapacity, float loadFactor,
                                int concurrencyLevel) {
        if (!(loadFactor > 0) || segementCapacity < 0 || concurrencyLevel <= 0)
            throw new IllegalArgumentException();

        if (concurrencyLevel > MAX_SEGMENTS)
            concurrencyLevel = MAX_SEGMENTS;

        // Find power-of-two sizes best matching arguments
        int sshift = 0;
        int ssize = 1;
        while (ssize < concurrencyLevel) {
            ++sshift;
            ssize <<= 1;
        }
        segmentShift = 32 - sshift;
        segmentMask = ssize - 1;
        this.segments = Segment.newArray(ssize);

        for (int i = 0; i < this.segments.length; ++i)
            this.segments[i] = new Segment<K, V>(segementCapacity, loadFactor, this);
    }

    /**
     * @param segementCapacity
     *            Segement最大容量
     * @param loadFactor
     */
    public ConcurrentLRUHashMap(int segementCapacity, float loadFactor) {
        this(segementCapacity, loadFactor, DEFAULT_CONCURRENCY_LEVEL);
    }

    /**
     * @param segementCapacity
     *            Segement最大容量
     */
    public ConcurrentLRUHashMap(int segementCapacity) {
        this(segementCapacity, DEFAULT_LOAD_FACTOR, DEFAULT_CONCURRENCY_LEVEL);
    }

    public ConcurrentLRUHashMap() {
        this(DEFAULT_SEGEMENT_MAX_CAPACITY, DEFAULT_LOAD_FACTOR,
                DEFAULT_CONCURRENCY_LEVEL);
    }

    /**
     *
     * @return <tt>true</tt> if this map contains no key-value mappings
     */
    public boolean isEmpty() {
        final Segment<K, V>[] segments = this.segments;
              
        int[] mc = new int[segments.length];
        int mcsum = 0;
        for (int i = 0; i < segments.length; ++i) {
            if (segments[i].count != 0)
                return false;
            else
                mcsum += mc[i] = segments[i].modCount;
        }
        // If mcsum happens to be zero, then we know we got a snapshot
        // before any modifications at all were made. This is
        // probably common enough to bother tracking.
        if (mcsum != 0) {
            for (int i = 0; i < segments.length; ++i) {
                if (segments[i].count != 0 || mc[i] != segments[i].modCount)
                    return false;
            }
        }
        return true;
    }

    /**
     * @return the number of key-value mappings in this map
     */
    public int size() {
        final Segment<K, V>[] segments = this.segments;
        long sum = 0;
        long check = 0;
        int[] mc = new int[segments.length];
        // Try a few times to get accurate count. On failure due to
        // continuous async changes in table, resort to locking.
        for (int k = 0; k < RETRIES_BEFORE_LOCK; ++k) {
            check = 0;
            sum = 0;
            int mcsum = 0;
            for (int i = 0; i < segments.length; ++i) {
                sum += segments[i].count;
                mcsum += mc[i] = segments[i].modCount;
            }
            if (mcsum != 0) {
                for (int i = 0; i < segments.length; ++i) {
                    check += segments[i].count;
                    if (mc[i] != segments[i].modCount) {
                        check = -1; // force retry
                        break;
                    }
                }
            }
            if (check == sum)
                break;
        }
        if (check != sum) { // Resort to locking all segments
            sum = 0;
            for (int i = 0; i < segments.length; ++i)
                segments[i].lock();
            for (int i = 0; i < segments.length; ++i)
                sum += segments[i].count;
            for (int i = 0; i < segments.length; ++i)
                segments[i].unlock();
        }
        if (sum > Integer.MAX_VALUE)
            return Integer.MAX_VALUE;
        else
            return (int) sum;

    }

    
    public V get(Object key) {
        int hash = hash(key.hashCode());
        return segmentFor(hash).get(key, hash);
    }

    
    public boolean containsKey(Object key) {
        int hash = hash(key.hashCode());
        return segmentFor(hash).containsKey(key, hash);
    }

    
    public boolean containsValue(Object value) {
        if (value == null)
            throw new NullPointerException();

        // See explanation of modCount use above

        final Segment<K, V>[] segments = this.segments;
        int[] mc = new int[segments.length];

        // Try a few times without locking
        for (int k = 0; k < RETRIES_BEFORE_LOCK; ++k) {
            int mcsum = 0;
            for (int i = 0; i < segments.length; ++i) {
                mcsum += mc[i] = segments[i].modCount;
                if (segments[i].containsValue(value))
                    return true;
            }
            boolean cleanSweep = true;
            if (mcsum != 0) {
                for (int i = 0; i < segments.length; ++i) {
                    if (mc[i] != segments[i].modCount) {
                        cleanSweep = false;
                        break;
                    }
                }
            }
            if (cleanSweep)
                return false;
        }
        // Resort to locking all segments
        for (int i = 0; i < segments.length; ++i)
            segments[i].lock();
        boolean found = false;
        try {
            for (int i = 0; i < segments.length; ++i) {
                if (segments[i].containsValue(value)) {
                    found = true;
                    break;
                }
            }
        } finally {
            for (int i = 0; i < segments.length; ++i)
                segments[i].unlock();
        }
        return found;
    }

    

    public boolean contains(Object value) {
        return containsValue(value);
    }

   

    public V put(K key, V value,int t) {
        if (value == null)
            throw new NullPointerException();
        int hash = hash(key.hashCode());
        if(t<0)
            throw new IllegalArgumentException();
        if(t>2592000||t==0){ t=0;return segmentFor(hash).put(key, hash, value, false,0);}
        long nowt=System.currentTimeMillis();
        long overt=nowt+1000*t;
        return segmentFor(hash).put(key, hash, value, false,overt);
    }
    
    public V put(K key, V value) {
        if (value == null)
            throw new NullPointerException();
        int hash = hash(key.hashCode());
        return segmentFor(hash).put(key, hash, value, false,0);
    }


    public V putIfAbsent(K key, V value) {
        if (value == null)
            throw new NullPointerException();
        int hash = hash(key.hashCode());
        return segmentFor(hash).put(key, hash, value, true,0);
    }

    /**
     * Copies all of the mappings from the specified map to this one. These
     * mappings replace any mappings that this map had for any of the keys
     * currently in the specified map.
     *
     * @param m
     *            mappings to be stored in this map
     */
    public void putAll(Map<? extends K, ? extends V> m) {
        for (Map.Entry<? extends K, ? extends V> e : m.entrySet())
            put(e.getKey(), e.getValue());
    }

    /**
     * Removes the key (and its corresponding value) from this map. This method
     * does nothing if the key is not in the map.
     *
     * @param key
     *            the key that needs to be removed
     * @return the previous value associated with <tt>key</tt>, or <tt>null</tt>
     *         if there was no mapping for <tt>key</tt>
     * @throws NullPointerException
     *             if the specified key is null
     */
    public V remove(Object key) {
        int hash = hash(key.hashCode());
        return segmentFor(hash).remove(key, hash, null);
    }

    /**
     * {@inheritDoc}
     *
     * @throws NullPointerException
     *             if the specified key is null
     */
    public boolean remove(Object key, Object value) {
        int hash = hash(key.hashCode());
        if (value == null)
            return false;
        return segmentFor(hash).remove(key, hash, value) != null;
    }

    /**
     * {@inheritDoc}
     *
     * @throws NullPointerException
     *             if any of the arguments are null
     */
    public boolean replace(K key, V oldValue, V newValue) {
        if (oldValue == null || newValue == null)
            throw new NullPointerException();
        int hash = hash(key.hashCode());
        return segmentFor(hash).replace(key, hash, oldValue, newValue);
    }

    /**
     * {@inheritDoc}
     *
     * @return the previous value associated with the specified key, or
     *         <tt>null</tt> if there was no mapping for the key
     * @throws NullPointerException
     *             if the specified key or value is null
     */
    public V replace(K key, V value) {
        if (value == null)
            throw new NullPointerException();
        int hash = hash(key.hashCode());
        return segmentFor(hash).replace(key, hash, value);
    }

    /**
     * Removes all of the mappings from this map.
     */
    public void clear() {
        for (int i = 0; i < segments.length; ++i)
            segments[i].clear();
    }

    /**
     * Returns a {@link Set} view of the keys contained in this map. The set is
     * backed by the map, so changes to the map are reflected in the set, and
     * vice-versa. The set supports element removal, which removes the
     * corresponding mapping from this map, via the <tt>Iterator.remove</tt>,
     * <tt>Set.remove</tt>, <tt>removeAll</tt>, <tt>retainAll</tt>, and
     * <tt>clear</tt> operations. It does not support the <tt>add</tt> or
     * <tt>addAll</tt> operations.
     *
     * <p>
     * The view's <tt>iterator</tt> is a "weakly consistent" iterator that will
     * never throw {@link ConcurrentModificationException}, and guarantees to
     * traverse elements as they existed upon construction of the iterator, and
     * may (but is not guaranteed to) reflect any modifications subsequent to
     * construction.
     */
    public Set<K> keySet() {
        Set<K> ks = keySet;
        return (ks != null) ? ks : (keySet = new KeySet());
    }

    /**
     * Returns a {@link Collection} view of the values contained in this map.
     * The collection is backed by the map, so changes to the map are reflected
     * in the collection, and vice-versa. The collection supports element
     * removal, which removes the corresponding mapping from this map, via the
     * <tt>Iterator.remove</tt>, <tt>Collection.remove</tt>, <tt>removeAll</tt>,
     * <tt>retainAll</tt>, and <tt>clear</tt> operations. It does not support
     * the <tt>add</tt> or <tt>addAll</tt> operations.
     *
     * <p>
     * The view's <tt>iterator</tt> is a "weakly consistent" iterator that will
     * never throw {@link ConcurrentModificationException}, and guarantees to
     * traverse elements as they existed upon construction of the iterator, and
     * may (but is not guaranteed to) reflect any modifications subsequent to
     * construction.
     */
    public Collection<V> values() {
        Collection<V> vs = values;
        return (vs != null) ? vs : (values = new Values());
    }

    /**
     * Returns a {@link Set} view of the mappings contained in this map. The set
     * is backed by the map, so changes to the map are reflected in the set, and
     * vice-versa. The set supports element removal, which removes the
     * corresponding mapping from the map, via the <tt>Iterator.remove</tt>,
     * <tt>Set.remove</tt>, <tt>removeAll</tt>, <tt>retainAll</tt>, and
     * <tt>clear</tt> operations. It does not support the <tt>add</tt> or
     * <tt>addAll</tt> operations.
     *
     * <p>
     * The view's <tt>iterator</tt> is a "weakly consistent" iterator that will
     * never throw {@link ConcurrentModificationException}, and guarantees to
     * traverse elements as they existed upon construction of the iterator, and
     * may (but is not guaranteed to) reflect any modifications subsequent to
     * construction.
     */
    public Set<Map.Entry<K, V>> entrySet() {
        Set<Map.Entry<K, V>> es = entrySet;
        return (es != null) ? es : (entrySet = new EntrySet());
    }

    /**
     * Returns an enumeration of the keys in this table.
     *
     * @return an enumeration of the keys in this table
     * @see #keySet()
     */
    public Enumeration<K> keys() {
        return new KeyIterator();
    }

    /**
     * Returns an enumeration of the values in this table.
     *
     * @return an enumeration of the values in this table
     * @see #values()
     */
    public Enumeration<V> elements() {
        return new ValueIterator();
    }

        /* ---------------- Iterator Support -------------- */

    abstract class HashIterator {
        int nextSegmentIndex;
        int nextTableIndex;
        HashEntry<K, V>[] currentTable;
        HashEntry<K, V> nextEntry;
        HashEntry<K, V> lastReturned;

        HashIterator() {
            nextSegmentIndex = segments.length - 1;
            nextTableIndex = -1;
            advance();
        }

        public boolean hasMoreElements() {
            return hasNext();
        }

        final void advance() {
            if (nextEntry != null && (nextEntry = nextEntry.next) != null)
                return;

            while (nextTableIndex >= 0) {
                if ((nextEntry = currentTable[nextTableIndex--]) != null)
                    return;
            }

            while (nextSegmentIndex >= 0) {
                Segment<K, V> seg = segments[nextSegmentIndex--];
                if (seg.count != 0) {
                    currentTable = seg.table;
                    for (int j = currentTable.length - 1; j >= 0; --j) {
                        if ((nextEntry = currentTable[j]) != null) {
                            nextTableIndex = j - 1;
                            return;
                        }
                    }
                }
            }
        }

        public boolean hasNext() {
            return nextEntry != null;
        }

        HashEntry<K, V> nextEntry() {
            if (nextEntry == null)
                throw new NoSuchElementException();
            lastReturned = nextEntry;
            advance();
            return lastReturned;
        }

        public void remove() {
            if (lastReturned == null)
                throw new IllegalStateException();
            ConcurrentLRUHashMap.this.remove(lastReturned.key);
            lastReturned = null;
        }
    }

    final class KeyIterator extends HashIterator implements Iterator<K>,
            Enumeration<K> {
        public K next() {
            return super.nextEntry().key;
        }

        public K nextElement() {
            return super.nextEntry().key;
        }
    }

    final class ValueIterator extends HashIterator implements Iterator<V>,
            Enumeration<V> {
        public V next() {
            return super.nextEntry().value;
        }

        public V nextElement() {
            return super.nextEntry().value;
        }
    }

    /**
     * Custom Entry class used by EntryIterator.next(), that relays setValue
     * changes to the underlying map.
     */
    final class WriteThroughEntry extends AbstractMap.SimpleEntry<K, V> {
        /**
         *
         */
        private static final long serialVersionUID = -2545938966452012894L;

        WriteThroughEntry(K k, V v) {
            super(k, v);
        }

        /**
         * Set our entry's value and write through to the map. The value to
         * return is somewhat arbitrary here. Since a WriteThroughEntry does not
         * necessarily track asynchronous changes, the most recent "previous"
         * value could be different from what we return (or could even have been
         * removed in which case the put will re-establish). We do not and
         * cannot guarantee more.
         */
        public V setValue(V value) {
            if (value == null)
                throw new NullPointerException();
            V v = super.setValue(value);
            ConcurrentLRUHashMap.this.put(getKey(), value);
            return v;
        }
    }

    final class EntryIterator extends HashIterator implements
            Iterator<Entry<K, V>> {
        public Map.Entry<K, V> next() {
            HashEntry<K, V> e = super.nextEntry();
            return new WriteThroughEntry(e.key, e.value);
        }
    }

    final class KeySet extends AbstractSet<K> {
        public Iterator<K> iterator() {
            return new KeyIterator();
        }

        public int size() {
            return ConcurrentLRUHashMap.this.size();
        }

        public boolean contains(Object o) {
            return ConcurrentLRUHashMap.this.containsKey(o);
        }

        public boolean remove(Object o) {
            return ConcurrentLRUHashMap.this.remove(o) != null;
        }

        public void clear() {
            ConcurrentLRUHashMap.this.clear();
        }
    }

    final class Values extends AbstractCollection<V> {
        public Iterator<V> iterator() {
            return new ValueIterator();
        }

        public int size() {
            return ConcurrentLRUHashMap.this.size();
        }

        public boolean contains(Object o) {
            return ConcurrentLRUHashMap.this.containsValue(o);
        }

        public void clear() {
            ConcurrentLRUHashMap.this.clear();
        }
    }

    final class EntrySet extends AbstractSet<Map.Entry<K, V>> {
        public Iterator<Map.Entry<K, V>> iterator() {
            return new EntryIterator();
        }

        public boolean contains(Object o) {
            if (!(o instanceof Map.Entry))
                return false;
            Map.Entry<?, ?> e = (Map.Entry<?, ?>) o;
            V v = ConcurrentLRUHashMap.this.get(e.getKey());
            return v != null && v.equals(e.getValue());
        }

        public boolean remove(Object o) {
            if (!(o instanceof Map.Entry))
                return false;
            Map.Entry<?, ?> e = (Map.Entry<?, ?>) o;
            return ConcurrentLRUHashMap.this.remove(e.getKey(), e.getValue());
        }

        public int size() {
            return ConcurrentLRUHashMap.this.size();
        }

        public void clear() {
            ConcurrentLRUHashMap.this.clear();
        }
    }

        /* ---------------- Serialization Support -------------- */

    /**
     * Save the state of the <tt>ConcurrentHashMap</tt> instance to a stream
     * (i.e., serialize it).
     *
     * @param s
     *            the stream
     * @serialData the key (Object) and value (Object) for each key-value
     *             mapping, followed by a null pair. The key-value mappings are
     *             emitted in no particular order.
     */
    private void writeObject(java.io.ObjectOutputStream s) throws IOException {
        s.defaultWriteObject();

        for (int k = 0; k < segments.length; ++k) {
            Segment<K, V> seg = segments[k];
            seg.lock();
            try {
                HashEntry<K, V>[] tab = seg.table;
                for (int i = 0; i < tab.length; ++i) {
                    for (HashEntry<K, V> e = tab[i]; e != null; e = e.next) {
                        s.writeObject(e.key);
                        s.writeObject(e.value);
                    }
                }
            } finally {
                seg.unlock();
            }
        }
        s.writeObject(null);
        s.writeObject(null);
    }

    /**
     * Reconstitute the <tt>ConcurrentHashMap</tt> instance from a stream (i.e.,
     * deserialize it).
     *
     * @param s
     *            the stream
     */
    @SuppressWarnings("unchecked")
    private void readObject(java.io.ObjectInputStream s) throws IOException,
            ClassNotFoundException {
        s.defaultReadObject();

        // Initialize each segment to be minimally sized, and let grow.
        for (int i = 0; i < segments.length; ++i) {
            segments[i].setTable(new HashEntry[1]);
        }

        // Read the keys and values, and put the mappings in the table
        for (;;) {
            K key = (K) s.readObject();
            V value = (V) s.readObject();
            if (key == null)
                break;
            put(key, value);
        }
    }
}