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
)

func index(writer http.ResponseWriter, request *http.Request) {
	fmt.Fprintf(writer, "KFS version: %s", KFS_VERSION)
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
	fmt.Printf("hash = %s\n", hash)
	return hash, nil
}

func store_file(filename string, hash string, storage_path string) {
	fmt.Printf("storing: %s\n", filename)
	copy_file(filename, storage_path)
	fmt.Printf("stored: '%s' to '%s'\n", filename, storage_path)

	// TODO: communicate errors to error queue
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
	// TODO: request storage locations
	staging_path, storage_paths, err := db_alloc_storage(size)
	if err != nil {
		msg := fmt.Sprintf("could not store '%s': %v", header.Filename, err)
		log.Println(msg)
		writer.WriteHeader(http.StatusInternalServerError)
		fmt.Fprintf(writer, "%s\n", msg)
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
	// TODO: lookup hash in database, if it already exists, then do nothing

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
	for _, storage_path := range storage_paths {
		fmt.Printf("path: %s\n", storage_path)
		// TODO: when all store_file goroutines have finished, remove
		//       the file from the staging directory
		go store_file(hash_filename, hash, storage_path)
	}
	fmt.Fprintf(writer, "OK\n")
}

func get_disk_space(path string) uint64 {
	var stat unix.Statfs_t

	unix.Statfs(path, &stat)

	// Available blocks * size per block = available space in bytes
	available_space := stat.Bavail * uint64(stat.Bsize)
	return available_space
}

func db_alloc_storage(size int64) (string, []string, error) {
	/*
	 * TODO: add a record to the sqlite db with the following metadata
	 * |storage root|uuid|path|filename|hash|hash algo (blake2b)|extension
	 * |file type|permissions|access time|modify time|change time|creation time
	 */
	mutex.Lock()
	defer mutex.Unlock()

	db, err := sql.Open("sqlite3", KFS_DB_PATH)
	if err != nil {
		new_err := fmt.Errorf("could not open db: %v", err)
		return "", []string{""}, new_err
	}
	query := `
		select root
		from disks
		where available > ?
	`
	fmt.Printf("size: %d\n", size)
	rows, err := db.Query(query, size)
	if err != nil {
		new_err := fmt.Errorf("could not query for available disk: %v", err)
		return "", []string{""}, new_err
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
		return "", []string{""}, new_err
	}
	rand.Shuffle(len(disks), func(i, j int) {
		disks[i], disks[j] = disks[j], disks[i]
	})

	staging_dir := fmt.Sprintf("%s/.kfs/staging/", disks[0])
	var storage_dirs []string
	for i := 0; i < KFS_REDUNDANCY; i++ {
		storage_dirs = append(
			storage_dirs,
			fmt.Sprintf("%s/.kfs/storage/", disks[i]),
		)
	}

	// TODO: reduce disk space
	// TODO: add file to 'files' table

	return staging_dir, storage_dirs, nil
}

func db_init() {
	db, err := sql.Open("sqlite3", KFS_DB_PATH)
	if err != nil {
		panic(err)
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

func main() {
	fmt.Println("KFS -- Kyle's File Storage")
	fmt.Printf("version: %s\n", KFS_VERSION)
	db_init()
	mux := http.NewServeMux()
	mux.HandleFunc("/", index)
	mux.HandleFunc("/upload", handle_upload)
	server := &http.Server{
		Addr:    "0.0.0.0:8080",
		Handler: mux,
	}
	log.Fatal(server.ListenAndServe())
}
