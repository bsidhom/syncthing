// Copyright (C) 2014 Jakob Borg and other contributors. All rights reserved.
// Use of this source code is governed by an MIT-style license that can be
// found in the LICENSE file.

package files_test

import (
	"fmt"
	"os"
	"sort"
	"testing"

	"github.com/calmh/syncthing/cid"
	"github.com/calmh/syncthing/files"
	"github.com/calmh/syncthing/lamport"
	"github.com/calmh/syncthing/protocol"
	"github.com/calmh/syncthing/scanner"
	"github.com/syndtr/goleveldb/leveldb"
)

var (
	remoteNode = protocol.NodeID{1, 2, 3}
)

func genBlocks(n int) []scanner.Block {
	b := make([]scanner.Block, n)
	for i := range b {
		h := make([]byte, 32)
		for j := range h {
			h[j] = byte(i + j)
		}
		b[i].Size = uint32(i)
		b[i].Offset = int64(i)
		b[i].Hash = h
	}
	return b
}

type fileList []scanner.File

func (l fileList) Len() int {
	return len(l)
}

func (l fileList) Less(a, b int) bool {
	return l[a].Name < l[b].Name
}

func (l fileList) Swap(a, b int) {
	l[a], l[b] = l[b], l[a]
}

func TestGlobalSet(t *testing.T) {
	os.RemoveAll("testdata/index.db")
	db, err := leveldb.OpenFile("testdata/index.db", nil)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	m := files.NewSet("test", db)

	local0 := []scanner.File{
		scanner.File{Name: "a", Version: 1000, Blocks: genBlocks(1)},
		scanner.File{Name: "b", Version: 1000, Blocks: genBlocks(2)},
		scanner.File{Name: "c", Version: 1000, Blocks: genBlocks(3)},
		scanner.File{Name: "d", Version: 1000, Blocks: genBlocks(4)},
		scanner.File{Name: "z", Version: 1000, Blocks: genBlocks(8)},
	}
	local1 := []scanner.File{
		scanner.File{Name: "a", Version: 1000, Blocks: genBlocks(1)},
		scanner.File{Name: "b", Version: 1000, Blocks: genBlocks(2)},
		scanner.File{Name: "c", Version: 1000, Blocks: genBlocks(3)},
		scanner.File{Name: "d", Version: 1000, Blocks: genBlocks(4)},
	}
	localTot := []scanner.File{
		local0[0],
		local0[1],
		local0[2],
		local0[3],
		scanner.File{Name: "z", Version: 1001, Flags: protocol.FlagDeleted},
	}

	remote0 := []scanner.File{
		scanner.File{Name: "a", Version: 1000, Blocks: genBlocks(1)},
		scanner.File{Name: "c", Version: 1002, Blocks: genBlocks(5)},
	}
	remote1 := []scanner.File{
		scanner.File{Name: "b", Version: 1001, Blocks: genBlocks(6)},
		scanner.File{Name: "e", Version: 1000, Blocks: genBlocks(7)},
	}
	remoteTot := []scanner.File{
		remote0[0],
		remote1[0],
		remote0[1],
		remote1[1],
	}

	expectedGlobal := []scanner.File{
		remote0[0],
		remote1[0],
		remote0[1],
		localTot[3],
		remote1[1],
		localTot[4],
	}

	expectedLocalNeed := []scanner.File{
		remote1[0],
		remote0[1],
		remote1[1],
	}

	expectedRemoteNeed := []scanner.File{
		local0[3],
	}

	m.ReplaceWithDelete(cid.LocalNodeID, local0)
	m.ReplaceWithDelete(cid.LocalNodeID, local1)
	m.Replace(remoteNode, remote0)
	m.Update(remoteNode, remote1)

	g := m.Global()
	sort.Sort(fileList(g))

	if fmt.Sprint(g) != fmt.Sprint(expectedGlobal) {
		t.Errorf("Global incorrect;\n A: %v !=\n E: %v", g, expectedGlobal)
	}

	h := m.Have(cid.LocalNodeID)
	sort.Sort(fileList(h))

	if fmt.Sprint(h) != fmt.Sprint(localTot) {
		t.Errorf("Have incorrect;\n A: %v !=\n E: %v", h, localTot)
	}

	h = m.Have(remoteNode)
	sort.Sort(fileList(h))

	if fmt.Sprint(h) != fmt.Sprint(remoteTot) {
		t.Errorf("Have incorrect;\n A: %v !=\n E: %v", h, remoteTot)
	}

	n := m.Need(cid.LocalNodeID)
	sort.Sort(fileList(n))

	if fmt.Sprint(n) != fmt.Sprint(expectedLocalNeed) {
		t.Errorf("Need incorrect;\n A: %v !=\n E: %v", n, expectedLocalNeed)
	}

	n = m.Need(remoteNode)
	sort.Sort(fileList(n))

	if fmt.Sprint(n) != fmt.Sprint(expectedRemoteNeed) {
		t.Errorf("Need incorrect;\n A: %v !=\n E: %v", n, expectedRemoteNeed)
	}

	f := m.Get(cid.LocalNodeID, "b")
	if fmt.Sprint(f) != fmt.Sprint(localTot[1]) {
		t.Errorf("Get incorrect;\n A: %v !=\n E: %v", f, localTot[1])
	}

	f = m.Get(remoteNode, "b")
	if fmt.Sprint(f) != fmt.Sprint(remote1[0]) {
		t.Errorf("Get incorrect;\n A: %v !=\n E: %v", f, remote1[0])
	}

	f = m.GetGlobal("b")
	if fmt.Sprint(f) != fmt.Sprint(remote1[0]) {
		t.Errorf("GetGlobal incorrect;\n A: %v !=\n E: %v", f, remote1[0])
	}

	f = m.Get(cid.LocalNodeID, "zz")
	if f.Name != "" {
		t.Errorf("Get incorrect;\n A: %v !=\n E: %v", f, scanner.File{})
	}

	f = m.GetGlobal("zz")
	if f.Name != "" {
		t.Errorf("GetGlobal incorrect;\n A: %v !=\n E: %v", f, scanner.File{})
	}

	av := []protocol.NodeID{cid.LocalNodeID, remoteNode}
	a := m.Availability("a")
	if !(len(a) == 2 && (a[0] == av[0] && a[1] == av[1] || a[0] == av[1] && a[1] == av[0])) {
		t.Errorf("Availability incorrect;\n A: %v !=\n E: %v", a, av)
	}
	a = m.Availability("b")
	if len(a) != 1 || a[0] != remoteNode {
		t.Errorf("Availability incorrect;\n A: %v !=\n E: %v", a, remoteNode)
	}
	a = m.Availability("d")
	if len(a) != 1 || a[0] != cid.LocalNodeID {
		t.Errorf("Availability incorrect;\n A: %v !=\n E: %v", a, cid.LocalNodeID)
	}
}

func TestLocalDeleted(t *testing.T) {
	os.RemoveAll("testdata/index.db")
	db, err := leveldb.OpenFile("testdata/index.db", nil)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	m := files.NewSet("test", db)
	lamport.Default = lamport.Clock{}

	local1 := []scanner.File{
		scanner.File{Name: "a", Version: 1000},
		scanner.File{Name: "b", Version: 1000},
		scanner.File{Name: "c", Version: 1000},
		scanner.File{Name: "d", Version: 1000},
		scanner.File{Name: "z", Version: 1000, Flags: protocol.FlagDirectory},
	}

	m.ReplaceWithDelete(cid.LocalNodeID, local1)

	m.ReplaceWithDelete(cid.LocalNodeID, []scanner.File{
		local1[0],
		// [1] removed
		local1[2],
		local1[3],
		local1[4],
	})
	m.ReplaceWithDelete(cid.LocalNodeID, []scanner.File{
		local1[0],
		local1[2],
		// [3] removed
		local1[4],
	})
	m.ReplaceWithDelete(cid.LocalNodeID, []scanner.File{
		local1[0],
		local1[2],
		// [4] removed
	})

	expectedGlobal1 := []scanner.File{
		local1[0],
		scanner.File{Name: "b", Version: 1001, Flags: protocol.FlagDeleted},
		local1[2],
		scanner.File{Name: "d", Version: 1002, Flags: protocol.FlagDeleted},
		scanner.File{Name: "z", Version: 1003, Flags: protocol.FlagDeleted | protocol.FlagDirectory},
	}

	g := m.Global()
	sort.Sort(fileList(g))
	sort.Sort(fileList(expectedGlobal1))

	if fmt.Sprint(g) != fmt.Sprint(expectedGlobal1) {
		t.Errorf("Global incorrect;\n A: %v !=\n E: %v", g, expectedGlobal1)
	}

	m.ReplaceWithDelete(cid.LocalNodeID, []scanner.File{
		local1[0],
		// [2] removed
	})

	expectedGlobal2 := []scanner.File{
		local1[0],
		scanner.File{Name: "b", Version: 1001, Flags: protocol.FlagDeleted},
		scanner.File{Name: "c", Version: 1004, Flags: protocol.FlagDeleted},
		scanner.File{Name: "d", Version: 1002, Flags: protocol.FlagDeleted},
		scanner.File{Name: "z", Version: 1003, Flags: protocol.FlagDeleted | protocol.FlagDirectory},
	}

	g = m.Global()
	sort.Sort(fileList(g))
	sort.Sort(fileList(expectedGlobal2))

	if fmt.Sprint(g) != fmt.Sprint(expectedGlobal2) {
		t.Errorf("Global incorrect;\n A: %v !=\n E: %v", g, expectedGlobal2)
	}
}

func Benchmark10kReplace(b *testing.B) {
	os.RemoveAll("testdata/index.db")
	db, err := leveldb.OpenFile("testdata/index.db", nil)
	if err != nil {
		b.Fatal(err)
	}
	defer db.Close()

	var local []scanner.File
	for i := 0; i < 10000; i++ {
		local = append(local, scanner.File{Name: fmt.Sprintf("file%d", i), Version: 1000})
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		m := files.NewSet("test", db)
		m.ReplaceWithDelete(cid.LocalNodeID, local)
	}
}

func Benchmark10kUpdateChg(b *testing.B) {
	var remote []scanner.File
	for i := 0; i < 10000; i++ {
		remote = append(remote, scanner.File{Name: fmt.Sprintf("file%d", i), Version: 1000})
	}

	os.RemoveAll("testdata/index.db")
	db, err := leveldb.OpenFile("testdata/index.db", nil)
	if err != nil {
		b.Fatal(err)
	}
	defer db.Close()
	m := files.NewSet("test", db)
	m.Replace(remoteNode, remote)

	var local []scanner.File
	for i := 0; i < 10000; i++ {
		local = append(local, scanner.File{Name: fmt.Sprintf("file%d", i), Version: 1000})
	}

	m.ReplaceWithDelete(cid.LocalNodeID, local)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		b.StopTimer()
		for j := range local {
			local[j].Version++
		}
		b.StartTimer()
		m.Update(cid.LocalNodeID, local)
	}
}

func Benchmark10kUpdateSme(b *testing.B) {
	var remote []scanner.File
	for i := 0; i < 10000; i++ {
		remote = append(remote, scanner.File{Name: fmt.Sprintf("file%d", i), Version: 1000})
	}

	os.RemoveAll("testdata/index.db")
	db, err := leveldb.OpenFile("testdata/index.db", nil)
	if err != nil {
		b.Fatal(err)
	}
	defer db.Close()
	m := files.NewSet("test", db)
	m.Replace(remoteNode, remote)

	var local []scanner.File
	for i := 0; i < 10000; i++ {
		local = append(local, scanner.File{Name: fmt.Sprintf("file%d", i), Version: 1000})
	}

	m.ReplaceWithDelete(cid.LocalNodeID, local)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		m.Update(cid.LocalNodeID, local)
	}
}

func Benchmark10kNeed2k(b *testing.B) {
	var remote []scanner.File
	for i := 0; i < 10000; i++ {
		remote = append(remote, scanner.File{Name: fmt.Sprintf("file%d", i), Version: 1000})
	}

	os.RemoveAll("testdata/index.db")
	db, err := leveldb.OpenFile("testdata/index.db", nil)
	if err != nil {
		b.Fatal(err)
	}
	defer db.Close()

	m := files.NewSet("test", db)
	m.Replace(remoteNode, remote)

	var local []scanner.File
	for i := 0; i < 8000; i++ {
		local = append(local, scanner.File{Name: fmt.Sprintf("file%d", i), Version: 1000})
	}
	for i := 8000; i < 10000; i++ {
		local = append(local, scanner.File{Name: fmt.Sprintf("file%d", i), Version: 980})
	}

	m.ReplaceWithDelete(cid.LocalNodeID, local)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		fs := m.Need(cid.LocalNodeID)
		if l := len(fs); l != 2000 {
			b.Errorf("wrong length %d != 2k", l)
		}
	}
}

func Benchmark10kHave(b *testing.B) {
	var remote []scanner.File
	for i := 0; i < 10000; i++ {
		remote = append(remote, scanner.File{Name: fmt.Sprintf("file%d", i), Version: 1000})
	}

	os.RemoveAll("testdata/index.db")
	db, err := leveldb.OpenFile("testdata/index.db", nil)
	if err != nil {
		b.Fatal(err)
	}
	defer db.Close()

	m := files.NewSet("test", db)
	m.Replace(remoteNode, remote)

	var local []scanner.File
	for i := 0; i < 2000; i++ {
		local = append(local, scanner.File{Name: fmt.Sprintf("file%d", i), Version: 1000})
	}
	for i := 2000; i < 10000; i++ {
		local = append(local, scanner.File{Name: fmt.Sprintf("file%d", i), Version: 980})
	}

	m.ReplaceWithDelete(cid.LocalNodeID, local)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		fs := m.Have(cid.LocalNodeID)
		if l := len(fs); l != 10000 {
			b.Errorf("wrong length %d != 10k", l)
		}
	}
}

func Benchmark10kGlobal(b *testing.B) {
	var remote []scanner.File
	for i := 0; i < 10000; i++ {
		remote = append(remote, scanner.File{Name: fmt.Sprintf("file%d", i), Version: 1000})
	}

	os.RemoveAll("testdata/index.db")
	db, err := leveldb.OpenFile("testdata/index.db", nil)
	if err != nil {
		b.Fatal(err)
	}
	defer db.Close()

	m := files.NewSet("test", db)
	m.Replace(remoteNode, remote)

	var local []scanner.File
	for i := 0; i < 2000; i++ {
		local = append(local, scanner.File{Name: fmt.Sprintf("file%d", i), Version: 1000})
	}
	for i := 2000; i < 10000; i++ {
		local = append(local, scanner.File{Name: fmt.Sprintf("file%d", i), Version: 980})
	}

	m.ReplaceWithDelete(cid.LocalNodeID, local)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		fs := m.Global()
		if l := len(fs); l != 10000 {
			b.Errorf("wrong length %d != 10k", l)
		}
	}
}

func TestGlobalReset(t *testing.T) {
	os.RemoveAll("testdata/index.db")
	db, err := leveldb.OpenFile("testdata/index.db", nil)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	m := files.NewSet("test", db)

	local := []scanner.File{
		scanner.File{Name: "a", Version: 1000},
		scanner.File{Name: "b", Version: 1000},
		scanner.File{Name: "c", Version: 1000},
		scanner.File{Name: "d", Version: 1000},
	}

	remote := []scanner.File{
		scanner.File{Name: "a", Version: 1000},
		scanner.File{Name: "b", Version: 1001},
		scanner.File{Name: "c", Version: 1002},
		scanner.File{Name: "e", Version: 1000},
	}

	m.ReplaceWithDelete(cid.LocalNodeID, local)
	g := m.Global()
	sort.Sort(fileList(g))

	if fmt.Sprint(g) != fmt.Sprint(local) {
		t.Errorf("Global incorrect;\n%v !=\n%v", g, local)
	}

	m.Replace(remoteNode, remote)
	m.Replace(remoteNode, nil)

	g = m.Global()
	sort.Sort(fileList(g))

	if fmt.Sprint(g) != fmt.Sprint(local) {
		t.Errorf("Global incorrect;\n%v !=\n%v", g, local)
	}
}

func TestNeed(t *testing.T) {
	os.RemoveAll("testdata/index.db")
	db, err := leveldb.OpenFile("testdata/index.db", nil)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	m := files.NewSet("test", db)

	local := []scanner.File{
		scanner.File{Name: "a", Version: 1000},
		scanner.File{Name: "b", Version: 1000},
		scanner.File{Name: "c", Version: 1000},
		scanner.File{Name: "d", Version: 1000},
	}

	remote := []scanner.File{
		scanner.File{Name: "a", Version: 1000},
		scanner.File{Name: "b", Version: 1001},
		scanner.File{Name: "c", Version: 1002},
		scanner.File{Name: "e", Version: 1000},
	}

	shouldNeed := []scanner.File{
		scanner.File{Name: "b", Version: 1001},
		scanner.File{Name: "c", Version: 1002},
		scanner.File{Name: "e", Version: 1000},
	}

	m.ReplaceWithDelete(cid.LocalNodeID, local)
	m.Replace(remoteNode, remote)

	need := m.Need(cid.LocalNodeID)

	sort.Sort(fileList(need))
	sort.Sort(fileList(shouldNeed))

	if fmt.Sprint(need) != fmt.Sprint(shouldNeed) {
		t.Errorf("Need incorrect;\n%v !=\n%v", need, shouldNeed)
	}
}

func TestChanges(t *testing.T) {
	os.RemoveAll("testdata/index.db")
	db, err := leveldb.OpenFile("testdata/index.db", nil)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	m := files.NewSet("test", db)

	local1 := []scanner.File{
		scanner.File{Name: "a", Version: 1000},
		scanner.File{Name: "b", Version: 1000},
		scanner.File{Name: "c", Version: 1000},
		scanner.File{Name: "d", Version: 1000},
	}

	local2 := []scanner.File{
		local1[0],
		// [1] deleted
		local1[2],
		scanner.File{Name: "d", Version: 1002},
		scanner.File{Name: "e", Version: 1000},
	}

	m.ReplaceWithDelete(cid.LocalNodeID, local1)
	c0 := m.Changes(cid.LocalNodeID)

	m.ReplaceWithDelete(cid.LocalNodeID, local2)
	c1 := m.Changes(cid.LocalNodeID)
	if !(c1 > c0) {
		t.Fatal("Change number should have incremented")
	}

	m.ReplaceWithDelete(cid.LocalNodeID, local2)
	c2 := m.Changes(cid.LocalNodeID)
	if c2 != c1 {
		t.Fatal("Change number should be unchanged")
	}
}
