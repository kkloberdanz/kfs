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
	"io"
	"log"
	"math/rand"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"sync"

	_ "github.com/mattn/go-sqlite3"
	uuid "github.com/satori/go.uuid"
	"golang.org/x/sys/unix"
)

const (
	KFS_VERSION    = "0.0.1"
	KFS_DB_PATH    = "/home/kyle/.kfs/db/db.sqlite3"
	KFS_REDUNDANCY = 2
)

var (
	mutex = &sync.Mutex{}
	db    *sql.DB
)

func index(writer http.ResponseWriter, request *http.Request) {
	fmt.Fprintf(writer, "KFS version: %s\n", KFS_VERSION)
}

func get_output_path(staging_path string, input_filename string) string {
	extension := filepath.Ext(input_filename)
	output_id := uuid.Must(uuid.NewV4(), nil)
	output_name := fmt.Sprintf("%s%s", output_id, extension)
	output_path := filepath.Join(staging_path, output_name)
	return output_path
}

func copy_file(src string, dst string) error {
	cmd := exec.Command("cp", src, dst)
	err := cmd.Run()
	if err != nil {
		return err
	}
	return nil
}

func hash_file(filename string) (string, error) {
	output, err := exec.Command("b2sum", filename).Output()
	if err != nil {
		return "", fmt.Errorf("failed to hash '%s': %s", filename, err)
	}

	output_str := string(output)
	hash := strings.Fields(output_str)[0]
	log.Printf("hash = %s\n", hash)
	return hash, nil
}

func store_file(filename string, hash string, storage_path string) {
	log.Printf("storing: %s\n", filename)
	copy_file(filename, storage_path)
	log.Printf("stored: '%s' to '%s'\n", filename, storage_path)
	// TODO: communicate errors to error queue
}

func archive_file(staging_path string, storage_paths []string, hash_filename string, hash string) {
	var wg sync.WaitGroup
	for _, storage_path := range storage_paths {
		log.Printf("path: %s\n", storage_path)
		wg.Add(1)
		go func(storage_path string, hash_filename string, hash string) {
			defer wg.Done()
			store_file(hash_filename, hash, storage_path)
		}(storage_path, hash_filename, hash)
	}

	wg.Wait()

	// TODO: check error
	os.Remove(hash_filename)
	log.Printf("removed file: %s", hash_filename)
}

/**
 * Check if the hash already exists on the server
 */
func handle_exists(writer http.ResponseWriter, request *http.Request) {
	client_hash := request.FormValue("hash")
	if db_has_hash(client_hash) {
		fmt.Fprintf(writer, "yes\n")
	} else {
		fmt.Fprintf(writer, "no\n")
	}
}

/**
 * Receive file, and write it to the staging directory.
 * When finished receiving file, run background routine to persist it to
 * durable storage and add it to the disk array.
 */
func handle_upload(writer http.ResponseWriter, request *http.Request) {
	// you can upload file with:
	// function kfs_upload()
	// {
	//     curl \
	//         -X POST \
	//         -F "file=@$1" \
	//         -F "hash=`b2sum $1 | awk '{ print $1 }'`" \
	//         -F "path=`pwd`" \
	//         localhost:8080/upload
	// }
	fmt.Println("handling upload")

	file, header, err := request.FormFile("file")
	if err != nil {
		http.Error(
			writer,
			"file upload requires key of 'file'",
			http.StatusBadRequest,
		)
		fmt.Fprintf(writer, "error\n")
		return
	}
	defer file.Close()
	client_hash := request.FormValue("hash")
	size := header.Size
	skip, staging_path, storage_paths, err := db_alloc_storage(client_hash, size)
	if err != nil {
		msg := fmt.Sprintf("could not store '%s': %v", header.Filename, err)
		log.Println(msg)
		writer.WriteHeader(http.StatusInternalServerError)
		fmt.Fprintf(writer, "%s\n", msg)
		return
	}
	if skip {
		log.Printf("skipping, already have hash: %s", client_hash)
		fmt.Fprintf(writer, "OK\n")
		return
	}
	fmt.Printf("staging: %s, storage: %s\n", staging_path, storage_paths)

	client_path := request.FormValue("path")
	fmt.Printf(
		"got file '%s/%s', size: %d, blake2b hash: %s\n",
		client_path,
		header.Filename,
		size,
		client_hash,
	)

	output_path := get_output_path(staging_path, header.Filename)
	outf, err := os.Create(output_path)
	if err != nil {
		fmt.Printf("failed to create output file: %s\n", err)
		return
	}
	defer outf.Close()
	io.Copy(outf, file)

	hash, err := hash_file(output_path)
	if err != nil {
		fmt.Printf("failed to hash file: %s\n", err)
		writer.WriteHeader(http.StatusNotAcceptable)
		return
	}
	if hash != client_hash {
		fmt.Fprintf(
			writer,
			"hashes do not match: you gave me: %s, but I calculated: %s\n",
			client_hash,
			hash,
		)
		writer.WriteHeader(http.StatusNotAcceptable)
		return
	}

	hash_filename := filepath.Join(staging_path, hash+".blake2b")
	os.Rename(output_path, hash_filename)
	go archive_file(staging_path, storage_paths, hash_filename, hash)
	fmt.Fprintf(writer, "OK\n")
}

func get_disk_space(path string) uint64 {
	var stat unix.Statfs_t

	unix.Statfs(path, &stat)

	// Available blocks * size per block = available space in bytes
	available_space := stat.Bavail * uint64(stat.Bsize)
	return available_space
}

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

func db_alloc_storage(hash string, size int64) (bool, string, []string, error) {
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

func db_init() {
	var err error
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

func main() {
	fmt.Println("KFS -- Kyle's File Storage")
	fmt.Printf("version: %s\n", KFS_VERSION)
	var err error
	db, err = sql.Open("sqlite3", KFS_DB_PATH)
	if err != nil {
		panic(fmt.Errorf("failed to open database file: %v", err))
	}
	defer db.Close()
	db_init()
	mux := http.NewServeMux()
	mux.HandleFunc("/", index)
	mux.HandleFunc("/upload", handle_upload)
	mux.HandleFunc("/exists", handle_exists)
	server := &http.Server{
		Addr:    "0.0.0.0:8080",
		Handler: mux,
	}
	log.Fatal(server.ListenAndServe())
}
