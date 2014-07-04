// Copyright (C) 2014 Jakob Borg and other contributors. All rights reserved.
// Use of this source code is governed by an MIT-style license that can be
// found in the LICENSE file.

// Package files provides a set type to track local/remote files with newness checks.
package files

import (
	"sync"

	"github.com/calmh/syncthing/protocol"
	"github.com/calmh/syncthing/scanner"
	"github.com/syndtr/goleveldb/leveldb"
)

type fileRecord struct {
	File   scanner.File
	Usage  int
	Global bool
}

type bitset uint64

type Set struct {
	changes [64]uint64
	cMut    sync.Mutex

	repo string
	db   *leveldb.DB
}

func NewSet(repo string, db *leveldb.DB) *Set {
	var m = Set{
		repo: repo,
		db:   db,
	}
	return &m
}

func (m *Set) Replace(node protocol.NodeID, fs []scanner.File) {
	if debug {
		l.Debugf("%s Replace(%v, [%d])", m.repo, node, len(fs))
	}
	ldbReplace(m.db, []byte(m.repo), node[:], fs)
}

func (m *Set) ReplaceWithDelete(node protocol.NodeID, fs []scanner.File) {
	if debug {
		l.Debugf("%s ReplaceWithDelete(%v, [%d])", m.repo, node, len(fs))
	}
	ldbReplaceWithDelete(m.db, []byte(m.repo), node[:], fs)
}

func (m *Set) Update(node protocol.NodeID, fs []scanner.File) {
	if debug {
		l.Debugf("%s Update(%v, [%d])", m.repo, node, len(fs))
	}
	ldbUpdate(m.db, []byte(m.repo), node[:], fs)
}

func (m *Set) Need(node protocol.NodeID) []scanner.File {
	if debug {
		l.Debugf("%s Need(%v)", m.repo, node)
	}
	return nil
}

func (m *Set) Have(node protocol.NodeID) []scanner.File {
	if debug {
		l.Debugf("%s Have(%v)", m.repo, node)
	}
	return ldbHave(m.db, []byte(m.repo), node[:])
}

func (m *Set) Global() []scanner.File {
	if debug {
		l.Debugf("%s Global()", m.repo)
	}
	return ldbGlobal(m.db, []byte(m.repo))
}

func (m *Set) Get(node protocol.NodeID, file string) scanner.File {
	var f scanner.File
	return f
}

func (m *Set) GetGlobal(file string) scanner.File {
	var f scanner.File
	return f
}

func (m *Set) Availability(name string) bitset {
	var av bitset
	return av
}

func (m *Set) Changes(node protocol.NodeID) uint64 {
	return 0
}
