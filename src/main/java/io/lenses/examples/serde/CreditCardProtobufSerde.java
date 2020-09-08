package io.lenses.examples.serde;

import com.landoop.lenses.lsql.serde.Deserializer;
import com.landoop.lenses.lsql.serde.Serde;
import com.landoop.lenses.lsql.serde.Serializer;
import io.lenses.examples.serde.protobuf.generated.CCData;
import org.apache.avro.Schema;
import org.apache.avro.SchemaBuilder;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;

import java.io.IOException;
import java.util.Properties;

public class CreditCardProtobufSerde implements Serde {

    private Schema schema = SchemaBuilder.builder()
            .record("credit_card_data_proto")
            .fields()
            .requiredString("number")
            .requiredString("customerFirstName")
            .requiredString("customerLastName")
            .requiredString("country")
            .requiredString("currency")
            .requiredBoolean("blocked")
            .endRecord();

    @Override
    public Serializer serializer(Properties properties) {

        return new Serializer() {
            @Override
            public byte[] serialize(GenericRecord record) throws IOException {
                CCData.CreditCard card = CCData.CreditCard.newBuilder()
                        .setNumber((String) record.get("number"))
                        .setCustomerFirstName((String) record.get("customerFirstName"))
                        .setCustomerLastName((String) record.get("customerLastName"))
                        .setCountry((String) record.get("country"))
                        .setCurrency((String) record.get("currency"))
                        .setBlocked((boolean) record.get("blocked"))
                        .build();
                return card.toByteArray();
            }

            @Override
            public void close() throws IOException {
            }
        };
    }

    @Override
    public Deserializer deserializer(Properties properties) {
        return new Deserializer() {
            @Override
            public GenericRecord deserialize(byte[] bytes) throws IOException {

                CCData.CreditCard card = CCData.CreditCard.parseFrom(bytes);

                GenericRecord record = new GenericData.Record(schema);
                record.put("number", card.getNumber());
                record.put("customerFirstName", card.getCustomerFirstName());
                record.put("customerLastName", card.getCustomerLastName());
                record.put("country", card.getCountry());
                record.put("currency", card.getCurrency());
                record.put("blocked", card.getBlocked());
                return record;
            }

            @Override
            public void close() throws IOException {
            }
        };
    }

    @Override
    public Schema getSchema() {
        return schema;
    }
}
