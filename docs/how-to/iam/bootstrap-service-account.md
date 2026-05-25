# How to Bootstrap a Service Account

Use the admin socket to create the first service account. The secret is shown once. Store it immediately in your secret manager.

```bash
grainfs iam sa create admin --endpoint ./tmp/admin.sock
```

See [Identity and admin socket](../../concepts/identity-and-admin-socket.md).
