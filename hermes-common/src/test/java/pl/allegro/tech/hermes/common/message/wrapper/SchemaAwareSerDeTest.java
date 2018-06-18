package pl.allegro.tech.hermes.common.message.wrapper;

import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.BinaryDecoder;
import org.apache.avro.io.DatumReader;
import org.apache.avro.io.DecoderFactory;
import org.testng.annotations.Test;
import pl.allegro.tech.hermes.schema.SchemaVersion;
import pl.allegro.tech.hermes.test.helper.avro.AvroUser;

import java.io.IOException;

import static org.assertj.core.api.Assertions.assertThat;

public class SchemaAwareSerDeTest {
    static final AvroUser avro = new AvroUser("bob", 10, "red");

    @Test
    public void shouldSerialize() {
        // given
        SchemaVersion version = SchemaVersion.valueOf(8);

        // when
        byte[] serialized = SchemaAwareSerDe.serialize(version, avro.asBytes());

        // then
        assertThat(serialized).startsWith((byte)0);
    }

    @Test
    public void shouldDeserialize() {
        // given
        byte[] serialized = SchemaAwareSerDe.serialize(SchemaVersion.valueOf(8), avro.asBytes());

        // when
        SchemaAwarePayload deserialized = SchemaAwareSerDe.deserialize(serialized);

        // then
        assertThat(deserialized.getSchemaVersion().value()).isEqualTo(8);
        assertThat(deserialized.getPayload()).isEqualTo(avro.asBytes());
    }

    @Test(expectedExceptions = { DeserializationException.class })
    public void shouldThrowExceptionWhenDeserializingWithoutMagicByte() throws IOException {
        // when
        SchemaAwareSerDe.deserialize(new byte[]{1,2,3});

        // then exception is thrown
    }

    @Test
    public void xWingSerializerCompatibilityTest() throws IOException {
        // given
        SchemaVersion version = SchemaVersion.valueOf(8);

        // when
        byte[] serialized = SchemaAwareSerDe.serialize(version, avro.asBytes());

        // then
        AvroDeserializer deserializer = new AvroDeserializer(serialized);
        DatumReader<GenericRecord> datumReader = new GenericDatumReader<>(avro.getSchema());
        BinaryDecoder binaryDecoder = new DecoderFactory().binaryDecoder(
                deserializer.getPayload(),
                deserializer.getAvroBinaryOffset(),
                deserializer.getAvroBinaryLength(),
                null);

        GenericRecord record = datumReader.read(null, binaryDecoder);

        assertThat(record.get("name").toString()).isEqualTo("bob");
        assertThat(deserializer.getWriterSchemaVersion()).isEqualTo(8);
    }
}
