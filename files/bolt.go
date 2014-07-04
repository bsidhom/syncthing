package files

import (
	"bytes"
	"sort"

	"github.com/calmh/syncthing/lamport"
	"github.com/calmh/syncthing/protocol"
	"github.com/calmh/syncthing/scanner"
	"github.com/syndtr/goleveldb/leveldb"
	"github.com/syndtr/goleveldb/leveldb/iterator"
	"github.com/syndtr/goleveldb/leveldb/util"
)

const (
	keyTypeNode = iota
	keyTypeGlobal
)

type fileVersion struct {
	version uint64
	node    []byte
}

type versionList struct {
	versions []fileVersion
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

func globalKeyName(key []byte) []byte {
	return key[1+64:]
}

type deletionHandler func(db *leveldb.DB, repo, node, name []byte, dbi iterator.Iterator)

func ldbGenericReplace(db *leveldb.DB, repo, node []byte, fs []scanner.File, deleteFn deletionHandler) {
	sort.Sort(fileList(fs)) // sort list on name, same as on disk

	start := nodeKey(repo, node, nil)                            // before all repo/node files
	limit := nodeKey(repo, node, []byte{0xff, 0xff, 0xff, 0xff}) // after all repo/node files

	dbi := db.NewIterator(&util.Range{Start: start, Limit: limit}, nil)
	moreDb := dbi.Next()
	fsi := 0
	moreFs := fsi < len(fs)
	var newName, oldName []byte
	for {
		moreFs = fsi < len(fs)

		if !moreDb && !moreFs {
			break
		}

		if moreFs {
			newName = []byte(fs[fsi].Name)
		}

		if moreDb {
			oldName = keyName(dbi.Key())
		}

		cmp := bytes.Compare(newName, oldName)

		l.Debugln(fsi, moreFs, moreDb, cmp, newName, oldName)

		switch {
		case moreFs && (!moreDb || cmp == -1):
			// Disk is missing this file. Insert it.
			ldbInsert(db, repo, node, newName, fs[fsi])
			ldbUpdateGlobal(db, repo, node, newName, fs[fsi].Version)
			fsi++

		case cmp == 0:
			// File exists on both sides - compare versions.
			ldbUpdateGlobal(db, repo, node, newName, fs[fsi].Version)
			// Iterate both sides.
			fsi++
			moreDb = dbi.Next()

		case moreDb && (!moreFs || cmp == 1):
			if deleteFn != nil {
				deleteFn(db, repo, node, oldName, dbi)
			}
			moreDb = dbi.Next()
		}
	}
	dbi.Release()
}

func ldbReplace(db *leveldb.DB, repo, node []byte, fs []scanner.File) {
	ldbGenericReplace(db, repo, node, fs, func(db *leveldb.DB, repo, node, name []byte, dbi iterator.Iterator) {
		// Disk has files that we are missing. Remove it.
		db.Delete(dbi.Key(), nil)
		ldbRemoveFromGlobal(db, repo, node, name)
	})
}

func ldbReplaceWithDelete(db *leveldb.DB, repo, node []byte, fs []scanner.File) {
	ldbGenericReplace(db, repo, node, fs, func(db *leveldb.DB, repo, node, name []byte, dbi iterator.Iterator) {
		var f scanner.File
		err := f.UnmarshalXDR(dbi.Value())
		if err != nil {
			panic(err)
		}
		f.Blocks = nil
		f.Version = lamport.Default.Tick(f.Version)
		f.Flags |= protocol.FlagDeleted
		err = db.Put(dbi.Key(), f.MarshalXDR(), nil)
		if err != nil {
			panic(err)
		}
	})
}

func ldbUpdate(db *leveldb.DB, repo, node []byte, fs []scanner.File) {
	ldbGenericReplace(db, repo, node, fs, nil)
}

func ldbInsert(db *leveldb.DB, repo, node, name []byte, file scanner.File) {
	l.Debugf("ldbInsert(%q, %q, %q, ...)", repo, node, name)
	nk := nodeKey(repo, node, name)
	err := db.Put(nk, file.MarshalXDR(), nil)
	if err != nil {
		panic(err)
	}
}

// ldbUpdateGlobal adds this node+version to the version list for the given
// file. If the node is already present in the list, the version is updated.
// If the file does not have an entry in the global list, it is created.
func ldbUpdateGlobal(db *leveldb.DB, repo, node, file []byte, version uint64) {
	gk := globalKey(repo, file)
	svl, err := db.Get(gk, nil)
	if err != nil && err != leveldb.ErrNotFound {
		panic(err)
	}

	var fl versionList
	nv := fileVersion{
		node:    node,
		version: version,
	}
	if svl != nil {
		err = fl.UnmarshalXDR(svl)
		if err != nil {
			panic(err)
		}

		for i := range fl.versions {
			if bytes.Compare(fl.versions[i].node, node) == 0 {
				fl.versions = append(fl.versions[:i], fl.versions[i+1:]...)
				break
			}
		}

		for i := range fl.versions {
			if fl.versions[i].version <= version {
				t := append(fl.versions[:i], nv)
				fl.versions = append(t, fl.versions[i:]...)
				break
			}
		}
	}

	if len(fl.versions) == 0 {
		fl.versions = []fileVersion{nv}
	}

	err = db.Put(gk, fl.MarshalXDR(), nil)
	if err != nil {
		panic(err)
	}
}

// ldbRemoveFromGlobal removes the node from the global version list for the
// given file. If the version list is empty after this, the file entry is
// removed entirely.
func ldbRemoveFromGlobal(db *leveldb.DB, repo, node, file []byte) {
	gk := globalKey(repo, file)
	svl, err := db.Get(gk, nil)
	if err != nil {
		panic(err)
	}

	var fl versionList
	err = fl.UnmarshalXDR(svl)
	if err != nil {
		panic(err)
	}

	for i := range fl.versions {
		if bytes.Compare(fl.versions[i].node, node) == 0 {
			fl.versions = append(fl.versions[:i], fl.versions[i+1:]...)
			break
		}
	}

	if len(fl.versions) == 0 {
		err = db.Delete(gk, nil)
	} else {
		err = db.Put(gk, fl.MarshalXDR(), nil)
	}
	if err != nil {
		panic(err)
	}
}

func ldbHave(db *leveldb.DB, repo, node []byte) []scanner.File {
	start := nodeKey(repo, node, nil)                            // before all repo/node files
	limit := nodeKey(repo, node, []byte{0xff, 0xff, 0xff, 0xff}) // after all repo/node files
	dbi := db.NewIterator(&util.Range{Start: start, Limit: limit}, nil)
	var fs []scanner.File
	for dbi.Next() {
		var f scanner.File
		err := f.UnmarshalXDR(dbi.Value())
		if err != nil {
			panic(err)
		}
		fs = append(fs, f)
	}
	return fs
}

func ldbGlobal(db *leveldb.DB, repo []byte) []scanner.File {
	start := globalKey(repo, nil)
	limit := globalKey(repo, []byte{0xff, 0xff, 0xff, 0xff})
	dbi := db.NewIterator(&util.Range{Start: start, Limit: limit}, nil)
	var fs []scanner.File
	for dbi.Next() {
		var vl versionList
		err := vl.UnmarshalXDR(dbi.Value())
		if err != nil {
			panic(err)
		}
		if len(vl.versions) == 0 {
			l.Debugln(dbi.Key())
			panic("no versions?")
		}
		fk := nodeKey(repo, vl.versions[0].node, globalKeyName(dbi.Key()))
		bs, err := db.Get(fk, nil)
		if err != nil {
			panic(err)
		}

		var f scanner.File
		err = f.UnmarshalXDR(bs)
		if err != nil {
			panic(err)
		}

		fs = append(fs, f)
	}
	return fs
}

func ldbNeed(db *leveldb.DB, repo, node []byte) []scanner.File {
	start := globalKey(repo, nil)
	limit := globalKey(repo, []byte{0xff, 0xff, 0xff, 0xff})
	dbi := db.NewIterator(&util.Range{Start: start, Limit: limit}, nil)
	var fs []scanner.File
	for dbi.Next() {
		var vl versionList
		err := vl.UnmarshalXDR(dbi.Value())
		if err != nil {
			panic(err)
		}
		if len(vl.versions) == 0 {
			l.Debugln(dbi.Key())
			panic("no versions?")
		}

		have := false // If we have the file, any version
		need := false // If we have a lower version of the file
		for _, v := range vl.versions {
			if bytes.Compare(v.node, node) == 0 {
				// This node has a lower version of the file
				have = true
				need = v.version < vl.versions[0].version
				break
			}
		}

		if need {
			fk := nodeKey(repo, vl.versions[0].node, globalKeyName(dbi.Key()))
			bs, err := db.Get(fk, nil)
			if err != nil {
				panic(err)
			}

			var gf scanner.File
			err = gf.UnmarshalXDR(bs)
			if err != nil {
				panic(err)
			}

			if protocol.IsDeleted(gf.Flags) && !have {
				// We don't need deleted files that we don't have
				continue
			}

			fs = append(fs, gf)
		}
	}
	return fs
}
