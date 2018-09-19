package org.janelia.saalfeldlab.labels.blocks.mongo

import net.imglib2.FinalInterval
import net.imglib2.Interval
import org.bson.BsonReader
import org.bson.BsonWriter
import org.bson.codecs.Codec
import org.bson.codecs.DecoderContext
import org.bson.codecs.EncoderContext

class IntervalArrayStoreCodec : Codec<IntervalArrayStore> {
    override fun getEncoderClass(): Class<IntervalArrayStore> {
        return IntervalArrayStore::class.java;
    }

    override fun encode(writer: BsonWriter, intervalStore: IntervalArrayStore, context: EncoderContext) {
        intervalStore.intervals.run {
            writer.writeStartArray()
//            writer.writeInt32(size)
            forEach {
                writer.writeInt64(it.min(0))
                writer.writeInt64(it.min(1))
                writer.writeInt64(it.min(2))
                writer.writeInt64(it.max(0))
                writer.writeInt64(it.max(1))
                writer.writeInt64(it.max(2))
            }
            writer.writeEndArray()
        }
    }

    override fun decode(reader: BsonReader, context: DecoderContext): IntervalArrayStore {
        reader.readStartArray()
        val intervals = mutableListOf<Interval>()
        reader.run {
            for (index in 0 until readInt32()) {
                intervals.add(FinalInterval.createMinMax(readInt64(), readInt64(), readInt64(), readInt64(), readInt64(), readInt64()))
            }
        }
        reader.readEndArray()
        return IntervalArrayStore(intervals.toTypedArray())
    }
}