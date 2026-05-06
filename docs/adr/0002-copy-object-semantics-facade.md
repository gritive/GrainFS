# CopyObject semantics belong to the operations facade

S3 CopyObject is a meaningful storage action, not a backend primitive. HTTP
handlers parse raw protocol fields into typed storage requests, while
`storage.Operations.CopyObject` owns source validation, copy-source
preconditions, delete-marker rejection, metadata directive behavior,
destination previous-object facts, fast-path eligibility, and mutation result
normalization.

The facade must validate the source with `HeadObject` or `HeadObjectVersion`
before opening a body stream. Missing source, explicit delete-marker source,
and failed source preconditions remain distinct typed storage errors so
protocol adapters can produce stable HTTP responses without depending on
backend-specific error text.

Optimized copy support is a private acceleration path behind the facade. A
backend adapter may move bytes or metadata after the facade has already decided
the operation is legal, but it must not own S3 CopyObject semantics. The
`storage.Backend` interface remains a primitive storage interface.

CopyObject always returns mutation result facts, including destination
previous-object facts. Tests for copy semantics primarily live at the storage
operations facade; server tests cover HTTP parsing and error mapping only.
