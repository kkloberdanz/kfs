/*
 *     Copyright (C) 2021 Kyle Kloberdanz
 *
 *  This program is free software: you can redistribute it and/or modify
 *  it under the terms of the GNU Affero General Public License as
 *  published by the Free Software Foundation, either version 3 of the
 *  License, or (at your option) any later version.
 *
 *  This program is distributed in the hope that it will be useful,
 *  but WITHOUT ANY WARRANTY; without even the implied warranty of
 *  MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 *  GNU Affero General Public License for more details.
 *
 *  You should have received a copy of the GNU Affero General Public License
 *  along with this program.  If not, see <https://www.gnu.org/licenses/>.
 */

package main

import (
	"database/sql"
	"fmt"
	"log"
	"math/rand"
	"sync"

	_ "github.com/mattn/go-sqlite3"
	"golang.org/x/sys/unix"
)

var (
	mutex          = &sync.Mutex{}
	db             *sql.DB
	KFS_DB_PATH    = "/home/kyle/.kfs/db/db.sqlite3"
	KFS_REDUNDANCY = 2
)

func db_reduce_space(root string, size int64) {
	stmt := `update disks set available = available - ? where root = ?`
	_, err := db.Exec(stmt, size, root)
	if err != nil {
		panic(fmt.Errorf("could not update available storage record: %v", err))
	}
}

func db_add_file_records(hash string, storage_dirs []string) {
	stmt := `
		insert into files(hash, hash_algo, storage_root)
		values(?, 'blake2b', ?)
	`
	for _, storage_dir := range storage_dirs {
		_, err := db.Exec(stmt, hash, storage_dir)
		if err != nil {
			panic(fmt.Errorf("could not add new file record: %v", err))
		}
	}
}

func db_has_hash(hash string) bool {
	var n_records int64
	query := `select count(*) from files where hash = ?`
	err := db.QueryRow(query, hash).Scan(&n_records)
	if err != nil {
		new_err := fmt.Errorf("could not select from 'files' table: %v", err)
		log.Println(new_err)
		return false
	}
	return n_records > 0
}

func AllocStorage(hash string, size int64) (bool, string, []string, error) {
	// TODO: store file metadata in table

	/*
	 * TODO: add a record to the sqlite db with the following metadata
	 * |storage root|uuid|path|filename|hash|hash algo (blake2b)|extension
	 * |file type|permissions|access time|modify time|change time|creation time
	 */
	mutex.Lock()
	defer mutex.Unlock()

	skip := false

	// if hash already exists, then don't do anything
	if db_has_hash(hash) {
		skip = true
		return skip, "", []string{""}, nil
	}

	query := `
		select root
		from disks
		where available > ?
	`
	rows, err := db.Query(query, 2*size)
	if err != nil {
		new_err := fmt.Errorf("could not query for available disk: %v", err)
		return skip, "", []string{""}, new_err
	}
	defer rows.Close()

	var disks []string

	for rows.Next() {
		var root string
		if err := rows.Scan(&root); err != nil {
			log.Fatal(err)
		}
		disks = append(disks, root)
	}
	if len(disks) < KFS_REDUNDANCY {
		new_err := fmt.Errorf(
			"not enough disks to meet redundancy requirements",
		)
		return skip, "", []string{""}, new_err
	}
	rand.Shuffle(len(disks), func(i, j int) {
		disks[i], disks[j] = disks[j], disks[i]
	})

	staging_dir := disks[0]
	var storage_dirs []string
	for i := 0; i < KFS_REDUNDANCY; i++ {
		storage_dirs = append(storage_dirs, disks[i])
	}

	// reduce disk space
	db_reduce_space(staging_dir, size)
	for _, storage := range storage_dirs {
		db_reduce_space(storage, size)
	}

	// add file to 'files' table
	db_add_file_records(hash, storage_dirs)

	staging_path := fmt.Sprintf("%s/.kfs/staging/", staging_dir)
	var storage_paths []string
	for _, dir := range storage_dirs {
		full_path := fmt.Sprintf("%s/.kfs/storage/", dir)
		storage_paths = append(storage_paths, full_path)
	}
	return skip, staging_path, storage_paths, nil
}

func db_close() {
	db.Close()
}

func db_init() {
	var err error
	db, err = sql.Open("sqlite3", KFS_DB_PATH)
	if err != nil {
		panic(fmt.Errorf("failed to open database file: %v", err))
	}
	schemas := []string{
		`
		CREATE TABLE IF NOT EXISTS files(
			hash TEXT,
			hash_algo TEXT,
			storage_root TEXT,
			path TEXT,
			filename TEXT,
			extension TEXT
		);
		`,

		`
		CREATE TABLE IF NOT EXISTS disks(
			root TEXT NOT NULL PRIMARY KEY,
			available INTEGER
		);
		`,
	}

	for _, schema := range schemas {
		_, err = db.Exec(schema)
		if err != nil {
			panic(err)
		}
	}

	// TODO: allow user to configure disk locations
	disks := []string{
		"/mnt/disk1",
		"/mnt/disk2",
		"/mnt/disk3",
		"/mnt/disk4",
	}

	disk_insert := `
		INSERT OR REPLACE INTO disks(
			root,
			available
		) values(?, ?)
	`
	for _, disk := range disks {
		space := get_disk_space(disk)
		_, err = db.Exec(disk_insert, disk, space)
		if err != nil {
			panic(err)
		}
	}
}

func get_disk_space(path string) uint64 {
	var stat unix.Statfs_t

	unix.Statfs(path, &stat)

	// Available blocks * size per block = available space in bytes
	available_space := stat.Bavail * uint64(stat.Bsize)
	return available_space
}
