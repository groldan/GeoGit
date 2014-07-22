package org.locationtech.geogig.storage.fs.mapped;

import java.io.IOException;
import java.io.InputStream;

import org.locationtech.geogig.api.ObjectId;
import org.locationtech.geogig.api.RevObject;
import org.locationtech.geogig.storage.ObjectReader;
import org.locationtech.geogig.storage.ObjectSerializingFactory;
import org.locationtech.geogig.storage.ObjectWriter;

import com.google.common.base.Throwables;

public class Marshaller {

    private ObjectSerializingFactory factory;

    public Marshaller(ObjectSerializingFactory factory) {
        this.factory = factory;
    }

    public void marshal(RevObject o, ByteData target) {
        target.reset();
        ObjectWriter<RevObject> writer = factory.createObjectWriter(o.getType());
        try {
            writer.write(o, target);
        } catch (IOException e) {
            throw Throwables.propagate(e);
        }
    }

    public RevObject unmarshal(ObjectId id, ByteData source) {
        ObjectReader<RevObject> reader = factory.createObjectReader();
        InputStream in = source.asInputStream();
        RevObject object = reader.read(id, in);
        return object;
    }
}
