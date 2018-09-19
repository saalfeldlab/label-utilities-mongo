package org.janelia.saalfeldlab.labels.blocks.mongo

import net.imglib2.FinalInterval
import net.imglib2.Interval
import org.bson.BsonReader
import org.bson.BsonWriter
import org.bson.codecs.Codec
import org.bson.codecs.DecoderContext
import org.bson.codecs.EncoderContext

class IntervalArrayCodec : Codec<Array<Interval>> {
    override fun getEncoderClass(): Class<Array<Interval>> {
        return Array<Interval>::class.java;
    }

    override fun encode(writer: BsonWriter, intervals: Array<Interval>, context: EncoderContext) {
        writer.writeInt32(intervals.size)
        intervals.forEach {
            writer.writeInt64(it.min(0))
            writer.writeInt64(it.min(1))
            writer.writeInt64(it.min(2))
            writer.writeInt64(it.max(0))
            writer.writeInt64(it.max(1))
            writer.writeInt64(it.max(2))
        }
    }

    override fun decode(reader: BsonReader, context: DecoderContext): Array<Interval> {
        val intervals = mutableListOf<Interval>()
        val next = { reader.readInt64() }
        for (index in 0 until reader.readInt32()) {
            intervals.add(FinalInterval.createMinMax(next(), next(), next(), next(), next(), next()))
        }
        return intervals.toTypedArray()
    }
}