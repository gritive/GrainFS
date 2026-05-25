# First NFS Mount

This guide exports the default bucket over NFSv4 and mounts it on Linux.

## Start GrainFS

```bash
./grainfs serve --data ./tmp --port 9000
```

## Register the export

```bash
grainfs nfs export add default --endpoint ./tmp/admin.sock
```

## Mount the bucket

```bash
sudo mkdir -p /mnt/grainfs
sudo mount -t nfs4 -o nolock,nfsvers=4.0 localhost:/default /mnt/grainfs
```

## Verify

```bash
echo "hello from nfs" | sudo tee /mnt/grainfs/nfs.txt
aws --no-sign-request --endpoint-url http://localhost:9000 s3 ls s3://default/
```

See [NFSv4 compatibility](../reference/nfs-compatibility.md) before relying on POSIX behavior.
