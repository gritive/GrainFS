# Storage mutation results belong to the operations facade

Handlers must not perform their own pre-mutation object bookkeeping for storage
metrics, invalidation, or protocol response facts. Result-returning mutation
methods belong on `storage.Operations`, which reads previous-object facts inside
the same operation boundary before performing the mutation. A not-found previous
read becomes `Previous.Exists=false`; any other previous-read error fails before
mutation, because mutation accounting is part of the facade contract rather than
best-effort handler bookkeeping.

Object-write mutations return the newly written object plus a previous-object
summary, while delete mutations use a distinct result shape for delete-marker
state and version identity. Copy mutations report previous-object facts for the
destination object only; source object state remains part of copy-source
validation and metadata selection.
