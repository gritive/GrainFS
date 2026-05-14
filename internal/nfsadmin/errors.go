package nfsadmin

import "github.com/gritive/GrainFS/internal/adminapi"

type Error = adminapi.Error

func IsBucketNotFound(err error) bool { return adminapi.IsCode(err, "bucket_not_found") }
func IsExportNotFound(err error) bool { return adminapi.IsCode(err, "export_not_found") }
