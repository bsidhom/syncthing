package files

import (
	"bytes"
	"sort"

	"github.com/calmh/syncthing/protocol"
	"github.com/calmh/syncthing/scanner"
	"github.com/syndtr/goleveldb/leveldb"
	"github.com/syndtr/goleveldb/leveldb/util"
)

const (
	keyTypeNode = iota
	keyTypeGlobal
)

type fileVersion struct {
	version uint64
	node    protocol.NodeID
}

type fileList []scanner.File

func (l fileList) Len() int {
	return len(l)
}

func (l fileList) Swap(a, b int) {
	l[a], l[b] = l[b], l[a]
}

func (l fileList) Less(a, b int) bool {
	return l[a].Name < l[b].Name
}

/*

keyTypeNode (1 byte)
    repository (64 bytes)
        node (32 bytes)
            name (variable size)
            	|
            	scanner.File

keyTypeGlobal (1 byte)
	repository (64 bytes)
		name (variable size)
			|
			[]fileVersion (sorted)

Need: iterate over keyTypeGlobal/repository in lockstep with keyTypeNode/repository/node; get file if version differs
Replace: iterate over keyTypeNode/repository/node in lockstep with new list, remove and update global as appropriate

*/

func nodeKey(repo, node, file []byte) []byte {
	k := make([]byte, 1+64+32+len(file))
	k[0] = keyTypeNode
	copy(k[1:], []byte(repo))
	copy(k[1+64:], node[:])
	copy(k[1+64+32:], []byte(file))
	return k
}

func globalKey(repo, file []byte) []byte {
	k := make([]byte, 1+64+len(file))
	k[0] = keyTypeGlobal
	copy(k[1:], []byte(repo))
	copy(k[1+64:], []byte(file))
	return k
}

func keyName(key []byte) []byte {
	return key[1+64+32:]
}

func ldbReplace(db *leveldb.DB, repo, node []byte, fs []scanner.File) {
	sort.Sort(fileList(fs)) // sort list on name, same as on disk

	start := nodeKey(repo, node, nil)                            // before all repo/node files
	limit := nodeKey(repo, node, []byte{0xff, 0xff, 0xff, 0xff}) // after all repo/node files

	dbi := db.NewIterator(&util.Range{Start: start, Limit: limit}, nil)
	more := dbi.Next()
	fsi := 0
	var newName, oldName []byte
	for {
		cmp := -1
		if more && fsi < len(fs) {
			newName = []byte(fs[fsi].Name)
			oldName = keyName(dbi.Key())
			cmp = bytes.Compare(newName, oldName)
		}
		switch {
		case cmp == -1:
			// Disk is missing this file. Insert it.
			fsi++

		case cmp == 0:
			// File exists on both sides - compare versions.
			ldbUpdateGlobal(db, repo, node, newName, fs[fsi].Version)
			// Iterate both sides.
			fsi++
			more = dbi.Next()

		case cmp == 1:
			// Disk has files that we are missing. Remove it.
			db.Delete(dbi.Key(), nil)
			ldbRemoveFromGlobal(db, repo, node, oldName)
			// Iterate db.
			more = dbi.Next()
		}
		if !more && fsi == len(fs) {
			break
		}
	}
	dbi.Release()
}

// ldbUpdateGlobal adds this node+version to the version list for the given
// file. If the node is already present in the list, the version is updated.
// If the file does not have an entry in the global list, it is created.
func ldbUpdateGlobal(db *leveldb.DB, repo, node, file []byte, version uint64) {
}

// ldbRemoveFromGlobal removes the node from the global version list for the
// given file. If the version list is empty after this, the file entry is
// removed entirely.
func ldbRemoveFromGlobal(db *leveldb.DB, repo, node, file []byte) {
}
