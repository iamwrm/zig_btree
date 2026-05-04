# zig_phmap

Production-oriented Zig port work for `parallel-hashmap` v2.0.0.

## Implemented

- `FlatHashMap`
- `FlatHashSet`
- `AutoFlatHashMap`
- `AutoFlatHashSet`
- `NodeHashMap`
- `NodeHashSet`
- `AutoNodeHashMap`
- `AutoNodeHashSet`

The flat containers use contiguous entry storage, one control byte per slot, 7-bit hash fingerprints, linear probing, tombstones, high-load-factor growth, explicit reserve/rehash, and validation hooks.

The node containers use the same hash index with separately allocated nodes, so value and entry pointers stay stable across table rehashes.

## Layout

- `src/phmap.zig` contains the public map/set APIs and container logic.
- `src/defaults.zig` contains the default hash and equality policies used by the `Auto*` aliases.
- `test/phmap_basic.zig` covers public API behavior; `test/phmap_stress.zig` covers randomized and allocation-stress scenarios.

## In Progress

- `Parallel*` sharded containers: not yet implemented. The planned design is fixed shard count selected from high hash bits, one flat or node container per shard, and per-shard locks for mutating operations.

## Intentional API Differences

- Zig APIs are allocator-explicit and return error unions for allocation.
- Iterators are invalidated by mutation. The first flat implementation does not expose C++-style iterator erase.
- Keys and values are stored by value. Callers that store owning resources must release those resources before removing entries or deinitializing the table.
