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
	cMut    sync.RWMutex

	repo string
	db   *leveldb.DB
}

func NewSet(repo string, db *leveldb.DB) *Set {
	var s = Set{
		repo: repo,
		db:   db,
	}
	return &s
}

func (s *Set) Replace(node protocol.NodeID, fs []scanner.File) {
	if debug {
		l.Debugf("%s Replace(%v, [%d])", s.repo, node, len(fs))
	}
	s.cMut.Lock()
	ldbReplace(s.db, []byte(s.repo), node[:], fs)
	s.cMut.Unlock()
}

func (s *Set) ReplaceWithDelete(node protocol.NodeID, fs []scanner.File) {
	if debug {
		l.Debugf("%s ReplaceWithDelete(%v, [%d])", s.repo, node, len(fs))
	}
	s.cMut.Lock()
	ldbReplaceWithDelete(s.db, []byte(s.repo), node[:], fs)
	s.cMut.Unlock()
}

func (s *Set) Update(node protocol.NodeID, fs []scanner.File) {
	if debug {
		l.Debugf("%s Update(%v, [%d])", s.repo, node, len(fs))
	}
	s.cMut.Lock()
	ldbUpdate(s.db, []byte(s.repo), node[:], fs)
	s.cMut.Unlock()
}

func (s *Set) Need(node protocol.NodeID) []scanner.File {
	if debug {
		l.Debugf("%s Need(%v)", s.repo, node)
	}
	s.cMut.RLock()
	defer s.cMut.RUnlock()
	return ldbNeed(s.db, []byte(s.repo), node[:])
}

func (s *Set) Have(node protocol.NodeID) []scanner.File {
	if debug {
		l.Debugf("%s Have(%v)", s.repo, node)
	}
	s.cMut.RLock()
	defer s.cMut.RUnlock()
	return ldbHave(s.db, []byte(s.repo), node[:])
}

func (s *Set) Global() []scanner.File {
	if debug {
		l.Debugf("%s Global()", s.repo)
	}
	s.cMut.RLock()
	defer s.cMut.RUnlock()
	return ldbGlobal(s.db, []byte(s.repo))
}

func (s *Set) Get(node protocol.NodeID, file string) scanner.File {
	s.cMut.RLock()
	defer s.cMut.RUnlock()
	return ldbGet(s.db, []byte(s.repo), node[:], []byte(file))
}

func (s *Set) GetGlobal(file string) scanner.File {
	s.cMut.RLock()
	defer s.cMut.RUnlock()
	return ldbGetGlobal(s.db, []byte(s.repo), []byte(file))
}

func (s *Set) Availability(file string) []protocol.NodeID {
	s.cMut.RLock()
	defer s.cMut.RUnlock()
	return ldbAvailability(s.db, []byte(s.repo), []byte(file))
}

func (s *Set) Changes(node protocol.NodeID) uint64 {
	return ldbChanges(node[:])
}
