// Package putpipeline implements the single-node PUT actor pipeline.
//
// Pipeline shape (one long-lived goroutine per box, bounded channels between):
//
//	Handler.Put(req)
//	   |
//	   +--> IngestActor (per-request)
//	   |        | StripePlaintext (cap=N)
//	   |        v
//	   |    CPUPool (GOMAXPROCS workers)
//	   |        | EncryptedShardChunk (per-drive, cap=N)
//	   |        v
//	   |    DriveActor x N  (per data dir)
//	   |        | ShardWriteResult
//	   |        v
//	   |    CommitCoord
//	   |        |   earlyAck on K data shards -> handler returns 200
//	   |        |   finalDone on all N shards -> queue metadata
//	   |        v
//	   |    MetadataBatcher (single Badger txn per batch)
//	   v
//	return *storage.Object
//
// See docs/superpowers/specs/2026-05-23-put-actor-pipeline-design.md
// for the full design.
package putpipeline
