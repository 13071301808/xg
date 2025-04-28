package com.yishou.bigdata.realtime.dw.common.utils;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.DatumWriter;
import org.apache.avro.io.Encoder;
import org.apache.avro.io.EncoderFactory;
import org.apache.avro.reflect.ReflectData;

import java.io.ByteArrayOutputStream;
import java.io.IOException;

public class AvroUtils {


    private Schema schema;

    private DatumWriter<GenericRecord> datumWriter;

    public AvroUtils() {
    }

    public String schemaStr(Class clazz){
        return ReflectData.get().getSchema(clazz).toString(true);
    }

    public AvroUtils(String schemaJson) {
        this.schema = new Schema.Parser().parse(schemaJson);
        this.datumWriter = new GenericDatumWriter<>(schema);
    }

    public GenericRecord genericRecord(){
        return new GenericData.Record(this.schema);
    }

    public byte[] serialize(GenericRecord record) throws IOException {
        ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
        Encoder encoder = EncoderFactory.get().binaryEncoder(outputStream, null);
        this.datumWriter.write(record, encoder);
        encoder.flush();  // 确保所有数据都被写入
        return outputStream.toByteArray();
    }

    public Schema schema() {
        return schema;
    }

    public void setSchema(Schema schema) {
        this.schema = schema;
    }

    public DatumWriter<GenericRecord> getDatumWriter() {
        return datumWriter;
    }

    public void setDatumWriter(DatumWriter<GenericRecord> datumWriter) {
        this.datumWriter = datumWriter;
    }

}
