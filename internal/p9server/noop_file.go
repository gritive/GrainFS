package p9server

import (
	"syscall"

	"github.com/hugelgupf/p9/p9"
)

// noopFile implements p9.File with ENOSYS/EROFS defaults for all methods.
// Embed this into concrete file types and override only what you need.
type noopFile struct {
	p9.DefaultWalkGetAttr
}

func (noopFile) Walk(names []string) ([]p9.QID, p9.File, error) {
	return nil, nil, syscall.ENOSYS
}
func (noopFile) StatFS() (p9.FSStat, error) { return p9.FSStat{}, syscall.ENOSYS }
func (noopFile) GetAttr(req p9.AttrMask) (p9.QID, p9.AttrMask, p9.Attr, error) {
	return p9.QID{}, p9.AttrMask{}, p9.Attr{}, syscall.ENOSYS
}
func (noopFile) SetAttr(valid p9.SetAttrMask, attr p9.SetAttr) error { return syscall.EPERM }
func (noopFile) Close() error                                        { return nil }
func (noopFile) Open(mode p9.OpenFlags) (p9.QID, uint32, error) {
	return p9.QID{}, 0, syscall.ENOSYS
}
func (noopFile) ReadAt(buf []byte, offset int64) (int, error)  { return 0, syscall.ENOSYS }
func (noopFile) WriteAt(buf []byte, offset int64) (int, error) { return 0, syscall.EROFS }
func (noopFile) SetXattr(attr string, data []byte, flags p9.XattrFlags) error {
	return syscall.ENOSYS
}
func (noopFile) GetXattr(attr string) ([]byte, error) { return nil, syscall.ENOSYS }
func (noopFile) ListXattrs() ([]string, error)        { return nil, syscall.ENOSYS }
func (noopFile) RemoveXattr(attr string) error        { return syscall.ENOSYS }
func (noopFile) FSync() error                         { return nil }
func (noopFile) Lock(pid int, locktype p9.LockType, flags p9.LockFlags, start, length uint64, client string) (p9.LockStatus, error) {
	return p9.LockStatusOK, nil
}
func (noopFile) Create(name string, flags p9.OpenFlags, permissions p9.FileMode, uid p9.UID, gid p9.GID) (p9.File, p9.QID, uint32, error) {
	return nil, p9.QID{}, 0, syscall.EROFS
}
func (noopFile) Mkdir(name string, permissions p9.FileMode, uid p9.UID, gid p9.GID) (p9.QID, error) {
	return p9.QID{}, syscall.EROFS
}
func (noopFile) Symlink(oldName string, newName string, uid p9.UID, gid p9.GID) (p9.QID, error) {
	return p9.QID{}, syscall.EROFS
}
func (noopFile) Link(target p9.File, newName string) error { return syscall.EROFS }
func (noopFile) Mknod(name string, mode p9.FileMode, major uint32, minor uint32, uid p9.UID, gid p9.GID) (p9.QID, error) {
	return p9.QID{}, syscall.EROFS
}
func (noopFile) Rename(newDir p9.File, newName string) error { return syscall.EROFS }
func (noopFile) RenameAt(oldName string, newDir p9.File, newName string) error {
	return syscall.EROFS
}
func (noopFile) UnlinkAt(name string, flags uint32) error { return syscall.EROFS }
func (noopFile) Readdir(offset uint64, count uint32) (p9.Dirents, error) {
	return nil, syscall.ENOTDIR
}
func (noopFile) Readlink() (string, error)              { return "", syscall.EINVAL }
func (noopFile) Renamed(newDir p9.File, newName string) {}
