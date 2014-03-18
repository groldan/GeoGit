/* Copyright (c) 2013 OpenPlans. All rights reserved.
 * This code is licensed under the BSD New License, available at the root
 * application directory.
 */
package org.geogit.storage.protobuf;

import org.geogit.storage.RevCommitSerializationTest;

public class ProtobufCommitSerializationTest extends RevCommitSerializationTest {

    @Override
    protected ProtobufSerializationFactory getObjectSerializingFactory() {
        return new ProtobufSerializationFactory();
    }

}
