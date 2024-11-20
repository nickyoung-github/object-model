# object-model

# Table of Contents
1. [Overview](#Overview)
2. [Pydantic and dataclasses](#pydantic-and-dataclasses)
3. [Persistence](#Persistence)

[WORK IN PROGRESS]

## Overview
For creating a data model in python, dataclasses and pydantic model-based classes
provide a solid foundation. However, they lack important features, mostly related to persistence.
`object-model` provides these enhancements.

## Pydantic and dataclasses
Pydantic and dataclasses both provide ways to define data objects. dataclasses are
attractive in that they are part of the standard python library. Pydantic provides
useful extra functionality such as:

- converting types to JSON Schema
- serialisation to/from json
- validation of inputs when constructing objects

the above features can (mostly) also be used with dataclasses. `object-model` provides
a simple API for handling both styles. 

Input validation only truly works on pydantic objects but all else works on both.

## Serialising Subclasses
The `Base` and `BaseModel` types automatically add the object's name to the serialised output.
The name can be overridden to avoid collisions. Type names are registered in the project's
entry points under the group `object-store`. This serves as a registry of types and allows
the implementation to be moved without needing to load and re-write persisted objects.

Pydantic's json serialisation does not natively provide a mechanism to serialise a member whose
type is a subclass of the specified type. `object-model` provides a `Subclass` type, which expands
to a discriminated union, with this type field as the discriminator.

This is represented in JSON Schema as a `OneOf` and allows such subclasses to be validated
against the schema.

## Persistence
Data model objects need to be persistable. `object-model` provides:

- a bi-temporal schema with JSON/JSONB-based serialisation of the objects
- partitioning of objects by type (where the DB supports it), allowing each type to be indexed appropriately
- a simple mechanism for defining object IDs
- SQL and REST implementations of an object store

### ID
`Id` is a descriptor field applied as a `ClassVar`. At class-level it specifies the
fields (or properties) which form a unique Id (when combined with the type). When called
on an object instance, it returns a tuple of the values of those fields - the object's 
persisted Id. In the simplest case, object can just be a field called `name`. 

Hierarchies of objects share the same Id. Thus, if you have a base class, the subclasses
may not override the Id.
