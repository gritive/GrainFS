package p9server

import (
	"context"
	"strings"
	"syscall"
	"time"

	"github.com/hugelgupf/p9/p9"
	"github.com/rs/zerolog/log"

	"github.com/gritive/GrainFS/internal/audit"
	"github.com/gritive/GrainFS/internal/iam/mountsastore"
	"github.com/gritive/GrainFS/internal/iam/policy"
	"github.com/gritive/GrainFS/internal/storage"
)

// p9AttachReqCtx is the immutable policy.RequestContext used for every
// grainfs:9PAttach evaluation. Resource="*" per spec D#6.
var p9AttachReqCtx = policy.RequestContext{Action: "grainfs:9PAttach", Resource: "*"}

type rootFile struct {
	noopFile
	backend      storage.Backend
	locks        *objectLocks
	mountSAStore *mountsastore.Store
	authorizer   p9Authorizer
	exportStore  exportGetter
	cfg          ConfigReader
	// T15 NFS§C: audit hook. When non-nil, called after every grainfs:9PAttach
	// allow/deny decision. nil = no audit emit (backward compat).
	auditHook func(audit.S3Event)
}

// Walk processes the first-path-component as <mount-sa>@<bucket> per D#6 of
// 2026-05-21-nfs-9p-mount-auth spec. hugelgupf/p9 lib consumes aname into
// Walk(names), so the auth gate lives here rather than Attach.
//
// Parse rules for names[0]:
//   - "<mount-sa>@<bucket>": split on FIRST '@'. Left = mount-SA ID.
//     Multiple '@' in bucket name: split-first; bucket may contain '@'.
//     In practice, S3 bucket names disallow '@', so "a@b@c" produces
//     mountSA="a", bucket="b@c" which fails HeadBucket → ENOENT.
//   - "<bucket>" (no '@'): anon path; saID = "".
//   - "@<bucket>" (empty mount-SA before '@'): rejected → ENOENT.
//   - "<mount-sa>@" (empty bucket after '@'): rejected → ENOENT.
//
// Error code policy:
//   - mount-SA pool hit + grainfs:9PAttach deny → EACCES
//   - mount-SA pool miss (with '@')             → ENOENT
//   - anon path + grainfs:9PAttach deny         → EACCES
//   - bucket not found                          → ENOENT
func (f *rootFile) Walk(names []string) ([]p9.QID, p9.File, error) {
	if len(names) == 0 {
		return nil, &rootFile{backend: f.backend, locks: f.locks, mountSAStore: f.mountSAStore, authorizer: f.authorizer, exportStore: f.exportStore, cfg: f.cfg, auditHook: f.auditHook}, nil
	}
	bucket, binding, err := f.resolveFirstComponent(names[0])
	if err != nil {
		return nil, nil, err
	}
	bqid := p9.QID{Type: p9.TypeDir, Path: qidPath(bucket)}
	bf := &bucketFile{backend: f.backend, locks: f.locks, bucket: bucket, binding: binding, exportStore: f.exportStore, cfg: f.cfg}
	if len(names) == 1 {
		return []p9.QID{bqid}, bf, nil
	}
	qids, file, err := bf.Walk(names[1:])
	if err != nil {
		return append([]p9.QID{bqid}, qids...), nil, err
	}
	return append([]p9.QID{bqid}, qids...), file, nil
}

// resolveFirstComponent parses names[0] through the IAM gate (when wired) and
// returns the resolved bucket name and binding. When the IAM gate is not wired
// (mountSAStore == nil), names[0] is treated as a raw bucket name.
func (f *rootFile) resolveFirstComponent(component string) (bucket string, binding fhBinding, err error) {
	ctx := context.Background()

	// No IAM gate: backward-compatible path — names[0] is a raw bucket name.
	if f.mountSAStore == nil {
		if err := f.backend.HeadBucket(ctx, component); err != nil {
			return "", fhBinding{}, syscall.ENOENT
		}
		return component, fhBinding{}, nil
	}

	// Parse <mount-sa>@<bucket> or <bucket>.
	atIdx := strings.IndexByte(component, '@')
	if atIdx < 0 {
		// No '@': anon path.
		return f.resolveAnon(ctx, component)
	}

	saID := component[:atIdx]
	bkt := component[atIdx+1:]

	// Reject empty mount-SA or empty bucket.
	if saID == "" || bkt == "" {
		return "", fhBinding{}, syscall.ENOENT
	}

	// Mount-SA path: pool lookup.
	if _, ok := f.mountSAStore.Get(saID); !ok {
		// Pool miss → ENOENT (not EACCES — mount-SA existence leak avoidance).
		log.Debug().Str("saID", saID).Str("bucket", bkt).Msg("p9: Walk mount-SA pool miss")
		return "", fhBinding{}, syscall.ENOENT
	}

	// Pool hit: verify bucket exists.
	if err := f.backend.HeadBucket(ctx, bkt); err != nil {
		return "", fhBinding{}, syscall.ENOENT
	}

	// Evaluate grainfs:9PAttach for this mount-SA.
	if f.authorizer != nil {
		res := f.authorizer.Authorize(ctx, saID, bkt, p9AttachReqCtx)
		if res.Decision != policy.DecisionAllow {
			log.Debug().Str("saID", saID).Str("bucket", bkt).
				Str("reason", res.Reason).Msg("p9: Walk 9PAttach denied")
			if f.auditHook != nil {
				f.auditHook(audit.S3Event{
					Ts:         time.Now().UnixMicro(),
					SAID:       saID,
					Bucket:     bkt,
					AuthStatus: "deny",
					Source:     "9p",
				})
			}
			return "", fhBinding{}, syscall.EACCES
		}
	}

	log.Debug().Str("saID", saID).Str("bucket", bkt).Msg("p9: Walk mount-SA confirmed")
	if f.auditHook != nil {
		f.auditHook(audit.S3Event{
			Ts:         time.Now().UnixMicro(),
			SAID:       saID,
			Bucket:     bkt,
			AuthStatus: "allow",
			Source:     "9p",
		})
	}
	return bkt, fhBinding{saID: saID, bucket: bkt}, nil
}

// resolveAnon handles the no-'@' path: verify bucket and run anon eval.
func (f *rootFile) resolveAnon(ctx context.Context, bucket string) (string, fhBinding, error) {
	if err := f.backend.HeadBucket(ctx, bucket); err != nil {
		return "", fhBinding{}, syscall.ENOENT
	}
	if f.authorizer != nil {
		res := f.authorizer.Authorize(ctx, "", bucket, p9AttachReqCtx)
		if res.Decision != policy.DecisionAllow {
			log.Debug().Str("bucket", bucket).
				Str("reason", res.Reason).Msg("p9: Walk anon 9PAttach denied")
			if f.auditHook != nil {
				f.auditHook(audit.S3Event{
					Ts:         time.Now().UnixMicro(),
					SAID:       "", // anon: empty string (F#39)
					Bucket:     bucket,
					AuthStatus: "deny",
					Source:     "9p",
				})
			}
			return "", fhBinding{}, syscall.EACCES
		}
	}
	log.Debug().Str("bucket", bucket).Msg("p9: Walk anon confirmed")
	if f.auditHook != nil {
		f.auditHook(audit.S3Event{
			Ts:         time.Now().UnixMicro(),
			SAID:       "", // anon: empty string (F#39)
			Bucket:     bucket,
			AuthStatus: "allow",
			Source:     "9p",
		})
	}
	return bucket, fhBinding{saID: "", bucket: bucket}, nil
}

func (f *rootFile) Open(mode p9.OpenFlags) (p9.QID, uint32, error) {
	return p9.QID{Type: p9.TypeDir, Path: 0}, 0, nil
}

func (f *rootFile) GetAttr(req p9.AttrMask) (p9.QID, p9.AttrMask, p9.Attr, error) {
	qid := p9.QID{Type: p9.TypeDir, Path: 0}
	valid := p9.AttrMask{Mode: true, NLink: true}
	attr := p9.Attr{Mode: p9.ModeDirectory | 0555, NLink: 2}
	return qid, valid, attr, nil
}

func (f *rootFile) Readdir(offset uint64, count uint32) (p9.Dirents, error) {
	buckets, err := f.backend.ListBuckets(context.Background())
	if err != nil {
		return nil, syscall.EIO
	}
	out := make(p9.Dirents, 0, direntCap(count))
	for i, name := range buckets {
		if uint64(i) < offset {
			continue
		}
		if uint32(len(out)) >= count {
			break
		}
		out = append(out, p9.Dirent{
			QID:    p9.QID{Type: p9.TypeDir, Path: qidPath(name)},
			Offset: uint64(i + 1),
			Type:   p9.TypeDir,
			Name:   name,
		})
	}
	return out, nil
}

func (f *rootFile) StatFS() (p9.FSStat, error) {
	return p9.FSStat{
		Type:       0x01021997, // v9fs magic
		BlockSize:  4096,
		NameLength: 255,
	}, nil
}

// qidPath computes a stable uint64 QID path from path components.
func qidPath(parts ...string) uint64 {
	const (
		offset64 = 14695981039346656037
		prime64  = 1099511628211
	)
	h := uint64(offset64)
	for i, p := range parts {
		if i > 0 {
			h ^= ':'
			h *= prime64
		}
		for j := 0; j < len(p); j++ {
			h ^= uint64(p[j])
			h *= prime64
		}
	}
	return h
}
