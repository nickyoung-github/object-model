# object-model: Define and persist python objects with minimal boilerplate

Any object that can be handled by `pydantic.TypeAdaptor` will work (pydantic models, dataclasses etc.)
Objects can be persisted as JSON or parquet (with some caveats).


## Defining persistable objects

### `TypeRegistry`
Persistable types must be registered with `TypeRegistry`. This can be done explicitly or using entry points.
While we could have avoided a registry, the alternative is persisting implementation-specific info,
such as the module and class names, making code refactoring very difficult.
You can optionally supply a `root` parameter to group types, e.g., you might specify a root of `Animals`
for classes `Dog`, `Cat`, `Bunyip` etc. See [DiscriminatedUnion](#discriminatedunion) for another use of root.

### `Persistable`
`Persistable` is the abstract base class for persistable objects. All it does it provide an `object_id`
for serialisation and ways to form that id for retrieval. There are two species of `Persistable`:

- `Unique` - As the name suggests, this is the base for types where all fields form the id. E.g.,
```python
class Address(Unique):
    street_number: str  # Could be 221B, for example
    street_name: str
    town: str
    postcode: str
    country: Country
```
- `Identifiable` - A base class for objects where a subset of fields form their id. Such fields
are declared using `IdField`. E.g.,
```python
class Customer(Identifiable):
    name: IdField[str]
    address: Address
```

### `Reference`
In classes which have user-defined types as attributes, do we want those nested objects to be persisted
by value in the parent object's payload or saved separately, with the parent persisting a reference?
The answer is very much, "it depends". If you definitely want the nested payload to be embedded, that is
easy: simply declare your type like `Customer` above. However, you could instead re-write it as:

```python
class Customer(Identifiable):
    name: IdField[str]
    address: Reference[Address]
```

This means that if we had multiple customers (e.g. family members) living at the same address,
when persisting those customers, the `Address` would be persisted once and shared, with a reference being
saved in the `Customer` payload.

You can force references to be serialised by value via the `by_reference` paramter on `ObjectStore.write`.
`ObjectStore.read` will always check for persisted references.

### `DiscriminatedUnion`
Polymorphism in persisted classes tends to work quite badly and should be avoided. Persistable objects
often also get used in contexts where we need to know a schema, such as web services.
`fastapi` based services, using pydantic models is a good example.

A better approach is to declare the valid types using a python `Union`. pydantic can try and infer the
actual type on deserialisation but it can be preferable to indicate that explicitly.
`DiscriminatedUnion` does this for you by:
- Including a type specifier in the JSON schema 
- Adding that type specifier (which uses `TypeRegistry.registered_name`) to the serialisation payload 
- Extracting and removing the type specifier from the deserialisation payload and selecting the appropriate type

`DiscriminatedUnion` can be combined with `__subclasses__` with one major caveat:
you need to ensure that all subclasses of your type have been imported or you will get inconsistent results.
`TypeRegistry.types_for_root` can help you here, e.g.,

```python
Animals = DiscriminatedUnion[TypeRegistry.types_for_root("Animals")]
```

Stack Overflow and its ilk are littered with mediocre implementations of this pattern, so if you want
to use `DiscriminatedUnion` standalone, you can supply your own `Discriminator` and use it without
`TypeRegistry`:

```python
Animals = DiscriminatedUnion[
    Animal.__subclasses__(),
    Discriminator(
        field="_type",
        discriminator=lambda t: t.__qualname__
    )
]
```

## Storing persistable objects

`ObjectStore` is an abstract base class for implementing an object store, such as a database.
A file system-based store `FSStore` is provided. Objects are written into directories, formed
from the store's root, the `TypeRegistry` root and registered name.
Each write creates a new file, whose name is the timestamp of the write, which corresponds to
`MetaData.entry_time` in the persisted record.

Reading uses `duckdb` to read the stored parquet/json files. Currently only querying by an object's id
is supported, but this approach also allows for querying by attributes of the object in future.

A nightly job to compact the timestamped files into a single file will improve read performance.

`FSStore` uses [fsspec](https://pypi.org/project/fsspec/), which supports a wide range of file systems.
For parquet schema generation it uses [anyschema](https://pypi.org/project/anyschema/), extended with
some handling for dataclasses and references.

> [Note]
> Parquet cannot handle union types natively. Currently they are only support via a `Reference`, e.g.:

```python
class Foo:
    animal: Reference[DiscriminatedUnion[Dog, Cat, Wildebeast]]
```

### Writing

```python
store = FSStore(r"file:///users/hades/ObjectStore", format=FSStoreFormat.Parquet)
best_boy = Dog(name="Cerberus")
store.write([best_boy])
```

Will result in:

```
/users/hades/ObjectStore                               # FSStore root
    /Animals                                           # TypeRegistry root
        /Dog                                           # TypeRegistry registered_name
            /2026-11-08T13:01:55.249821+00:00.parquet  # File name
```

If you save an object which contains references, multiple objects may be created:

```python
blofeld = Customer(
    name="Ernst Stavro Blofeld",
    address=Address(
        street_numer=1,
        street_name="The Hollowed Out Volcano",
        town="",
        postcode="HV1 1ZZ",
        country=Country.Krakatoa
    )
)
store.write([blofeld])
```

This would create:

```
/users/hades/ObjectStore                           # FSStore root
    /Customer                                      # TypeRegistry registered_name
        /2026-11-08T13:01:55.249821+00:00.parquet  # File name
    /Address                                       # TypeRegistry registered_name
        /2026-11-08T13:01:55.249821+00:00.parquet  # File name
```

#### Reading
Objects are read by creating an id with `make_object_id`:

```python
best_boy_id = Dog.make_object_id(name="Cerberus")
loaded = store.read([best_boy_id])
best_boy = loaded[best_boy_id]
```
