package org.janelia.saalfeldlab.labels.blocks.mongo

import com.mongodb.MongoClient
import com.mongodb.client.FindIterable
import com.mongodb.client.MongoCollection
import com.mongodb.client.MongoDatabase
import net.imglib2.FinalInterval
import net.imglib2.Interval
import net.imglib2.util.Intervals
import org.bson.Document
import org.bson.codecs.configuration.CodecRegistries
import org.bson.conversions.Bson
import org.janelia.saalfeldlab.labels.blocks.LabelBlockLookup
import org.slf4j.LoggerFactory
import java.lang.invoke.MethodHandles
import java.util.Arrays
import java.util.stream.Collectors
import java.util.stream.LongStream
import java.util.stream.Stream
import java.util.stream.StreamSupport


private val LOG = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass())

private fun longArrayToIntervalArray(longArray: LongArray): Array<Interval> {
    assert(longArray.size % LONGS_PER_INTERVAL == 0)
    val intervals = mutableListOf<Interval>()
    for (index in 0 until longArray.size step LONGS_PER_INTERVAL) {
        val min = longArrayOf(longArray[index + 0], longArray[index + 1], longArray[index + 2])
        val max = longArrayOf(longArray[index + 3], longArray[index + 4], longArray[index + 5])
        intervals.add(FinalInterval(min, max))
    }
    return intervals.toTypedArray()
}

private fun intervalAsLongArray(interval: Interval): LongArray {
    return LongStream.concat(Arrays.stream(Intervals.minAsLongArray(interval)), Arrays.stream(Intervals.maxAsLongArray(interval))).toArray()
}

private fun asLongArray(vararg intervals: Interval): LongArray {
    return Stream
            .of(*intervals)
            .map { intervalAsLongArray(it) }
            .flatMapToLong { Arrays.stream(it) }
            .toArray()
}

private const val LOCALHOST = "localhost"

private const val DEFAULT_PORT = 27017

private const val DEFAULT_DATABASE_NAME = "label-block-lookup"

private const val DEFAULT_COLLECTION_PATTERN = "%d"

private const val N_DIM = 3

// 3 for min, 3 for max
private const val LONGS_PER_INTERVAL = N_DIM * 2

@LabelBlockLookup.LookupType("mongodb")
class LabelBlockLookupFromMongo(
        @LabelBlockLookup.Parameter private val hostname: String = LOCALHOST,
        @LabelBlockLookup.Parameter private val port: Int = DEFAULT_PORT,
        @LabelBlockLookup.Parameter private val databaseName: String = DEFAULT_DATABASE_NAME,
        @LabelBlockLookup.Parameter private val collectionPattern: String = DEFAULT_COLLECTION_PATTERN,
        @LabelBlockLookup.Parameter private val idKey: String = IntervalArrayStoreDocumentDocumentCodec.DEFAULT_ID_KEY,
        @LabelBlockLookup.Parameter private val intervalsKey: String = IntervalArrayStoreDocumentDocumentCodec.DEFAULT_INTERVALS_KEY) : LabelBlockLookup {

    private var client: MongoClient? = null

    private var database: MongoDatabase? = null

    companion object {

    }

    override fun read(level: Int, id: Long): Array<Interval> {
        LOG.debug("Reading id {} for level={}", id, level);
        val iterable = findMatchesForId(level, id)
        val list = StreamSupport
                .stream(iterable.spliterator(), false)
                .map { it.store.intervals }
                .flatMap { Stream.of(*it) }
//                .map { it[intervalsKey, IntervalArrayStore::class.java] }
//                .map { it?.intervals ?: emptyArray() }
//                .flatMap { Stream.of(*it) }
//                .map { it[intervalsKey, List::class.java] as List<Interval>? ?: listOf() }
//                .map { LOG.info("List is {}", it);it }
//                .flatMap { it.stream() }
                .collect(Collectors.toList())
        LOG.info("Got intervals {}", list)
        return list.toTypedArray()
    }


    override fun write(level: Int, id: Long, vararg intervals: Interval) {
        val collection = getCollectionForLevel(level)
        val matches = collection.find(searchQueryForId(id))
        val store = IntervalArrayStore(intervals)
        val doc = IntervalArrayStoreDocument(id, store)
        if (!matches.iterator().hasNext()) {
//            val doc = Document(idKey, id).append(intervalsKey, store)
            collection.insertOne(doc)
        } else {
            collection.updateMany(
                    Document(idKey, id),
                    Document("\$set", Document(intervalsKey, doc)));
        }
    }

    private fun getOrIinitializeClient(): MongoClient {
        if (client == null) client = MongoClient(hostname, port)
        return client!!
    }

    private fun getOrInitializeDatabase(): MongoDatabase {
        if (database == null) {
            val db = getOrIinitializeClient().getDatabase(databaseName)
            val codecs = db.codecRegistry
            val codec = CodecRegistries.fromCodecs(IntervalArrayStoreDocumentDocumentCodec(
                    idKey = idKey,
                    intervalsKey = intervalsKey))
            database = db.withCodecRegistry(CodecRegistries.fromRegistries(codecs, codec))
        }
        return database!!
    }

    private fun getCollectionForLevel(level: Int): MongoCollection<IntervalArrayStoreDocument> {
        val collectionName = String.format(collectionPattern, level)
        val database = getOrInitializeDatabase()
        return database.getCollection(collectionName, IntervalArrayStoreDocument::class.java)
    }

    private fun findMatchesForId(level: Int, id: Long): FindIterable<IntervalArrayStoreDocument> {
        val collection = getCollectionForLevel(level)
        val searchQuery = searchQueryForId(id)
        return collection.find(searchQuery)
    }

    private fun searchQueryForId(id: Long): Bson {
        return Document(idKey, id)
    }

}

fun main(args: Array<String>) {
    val level = 1
    val id = 8L
    val lookup = LabelBlockLookupFromMongo()
    LOG.info("Read intervals: {}", lookup.read(level, id))
    Arrays.stream(lookup.read(level, id)).map {  }

    lookup.write(level, id, FinalInterval.createMinMax(3, 3, 3, 5, 5, 6), FinalInterval.createMinMax(0, 0, 0, 2, 2, 2))

    LOG.info("Read intervals: {}", lookup.read(level, id))
    Arrays.stream(lookup.read(level, id)).map { }

}
