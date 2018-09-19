package org.janelia.saalfeldlab.labels.blocks.mongo

import net.imglib2.FinalInterval
import net.imglib2.Interval
import net.imglib2.util.Intervals
import org.bson.BsonReader
import org.bson.BsonWriter
import org.bson.Document
import org.bson.codecs.Codec
import org.bson.codecs.DecoderContext
import org.bson.codecs.DocumentCodec
import org.bson.codecs.EncoderContext
import org.slf4j.LoggerFactory
import java.lang.invoke.MethodHandles
import java.util.Arrays
import java.util.stream.Collectors
import java.util.stream.LongStream
import java.util.stream.Stream

class IntervalArrayStoreDocumentDocumentCodec(
        val documentCodec: DocumentCodec = DocumentCodec(),
        val idKey: String = DEFAULT_ID_KEY,
        val intervalsKey: String = DEFAULT_INTERVALS_KEY) : Codec<IntervalArrayStoreDocument> {

    companion object {

        private val LOG = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass())

        // TODO should "_id" be used here (reserved mongo primary key)?
        const val DEFAULT_ID_KEY = "_id"

        const val DEFAULT_INTERVALS_KEY = "intervals"

        private const val N_DIM = 3

        // 3 for min, 3 for max
        private const val LONGS_PER_INTERVAL = N_DIM * 2

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
    }
    override fun getEncoderClass(): Class<IntervalArrayStoreDocument> {
        return IntervalArrayStoreDocument::class.java;
    }

    override fun encode(writer: BsonWriter, intervalStore: IntervalArrayStoreDocument, context: EncoderContext) {
        val document = Document()
                .append(idKey, intervalStore.id)
                .append(
                        intervalsKey,
                        Stream
                                .of(intervalStore.store.intervals)
                                .map{ asLongArray(*it) }
                                .flatMapToLong { LongStream.of(*it) }
                                .boxed()
                                .collect(Collectors.toList()))
        LOG.info("Encoding document {}", document)
        documentCodec.encode(writer, document, context)
//        writer.writeStartDocument()
//        writer.writeInt64(idKey, intervalStore.id)
//        intervalStore.store.intervals.run {
//            writer.writeName(intervalsKey)
//            writer.writeStartArray()
//            writer.writeInt32(size)
//            forEach {
//                writer.writeInt64(it.min(0))
//                writer.writeInt64(it.min(1))
//                writer.writeInt64(it.min(2))
//                writer.writeInt64(it.max(0))
//                writer.writeInt64(it.max(1))
//                writer.writeInt64(it.max(2))
//            }
//            writer.writeEndArray()
//        }
//        writer.writeEndDocument()
    }

    override fun decode(reader: BsonReader, context: DecoderContext): IntervalArrayStoreDocument {
//        reader.readStartDocument()
//        val id = reader.readInt64(idKey)
//        val intervalsKey = reader.readName()
//        reader.readStartArray()
//        assert(this.intervalsKey.equals(intervalsKey))
//        val intervals = mutableListOf<Interval>()
//        reader.run {
//            for (index in 0 until readInt32()) {
//                intervals.add(FinalInterval.createMinMax(readInt64(), readInt64(), readInt64(), readInt64(), readInt64(), readInt64()))
//            }
//        }
//        reader.readEndArray()
//        reader.readEndDocument()
        val doc = documentCodec.decode(reader, context)
        LOG.info("Decoded document {}", doc)
        val id = doc[idKey, java.lang.Long::class.java] as Long
        LOG.info("Got id {}", id)
        val data = doc[intervalsKey]//, java.util.List::class.java]
        LOG.info("Got data {} at {}", data, intervalsKey)
        val numbers = if (data is List<*>) data as List<Number> else listOf<Number>()
        val intervals = longArrayToIntervalArray(numbers.stream().mapToLong { it.toLong() }.toArray())
        return IntervalArrayStoreDocument(id, IntervalArrayStore(intervals))
    }



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
}