// Wire-compatible with Mononoke's packblob schema. Field IDs and union
// alternatives are part of the persisted format; do not renumber them.
namespace go thrift

union SingleValue {
  1: binary Raw;
  2: binary Zstd;
}

struct ZstdFromDictValue {
  1: string dict_key;
  2: binary zstd;
}

union PackedValue {
  1: SingleValue Single;
  2: ZstdFromDictValue ZstdFromDict;
}

struct PackedEntry {
  1: string key;
  2: PackedValue data;
}

struct PackedFormat {
  1: string key;
  2: list<PackedEntry> entries;
}

union StorageFormat {
  1: SingleValue Single;
  2: PackedFormat Packed;
}

struct StorageEnvelope {
  1: StorageFormat storage;
}
