package org.janelia.saalfeldlab.labels.blocks.mongo

import net.imglib2.Interval
import org.bson.BsonDocument
import org.bson.Document
import org.bson.codecs.configuration.CodecRegistry
import org.bson.conversions.Bson

class IntervalArrayStoreDocument(
        val id: Long,
        val store: IntervalArrayStore
)