@0xc2a14174aa42a12a;

struct ObjectInstructionHeader {
    objectId @0: ObjectId; # 32 bytes
    payloadLength @1: UInt64; # 8 bytes
    instruction @2: ObjectInstructionType; # 2 bytes

    enum ObjectInstructionType {
        setObjectContentById @0;
        setObjectNameById @1;
        getObjectContentById @2;
        getObjectNameById @3;
        delObjectByName @4;
        delObjectById @5;
    }

    struct ObjectId {
        field0 @0: UInt64;
        field1 @1: UInt64;
        field2 @2: UInt64;
        field3 @3: UInt64;
    }
}
