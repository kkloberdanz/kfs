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
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"strings"

	_ "github.com/mattn/go-sqlite3"
	uuid "github.com/satori/go.uuid"
)

const (
	KFS_VERSION      = "0.0.1"
	KFS_STORAGE_PATH = "/home/kyle/.kfs/storage"
	KFS_STAGING_PATH = "/home/kyle/.kfs/staging"
	KFS_DB_PATH      = "/home/kyle/.kfs/db/db.sqlite3"
)

func index(writer http.ResponseWriter, request *http.Request) {
	fmt.Fprintf(writer, "KFS version: %s", KFS_VERSION)
}

func get_output_path(input_filename string) string {
	extension := filepath.Ext(input_filename)
	output_id := uuid.Must(uuid.NewV4(), nil)
	output_name := fmt.Sprintf("%s%s", output_id, extension)
	output_path := filepath.Join(KFS_STAGING_PATH, output_name)
	return output_path
}

func move_file(src string, dst string) error {
	cmd := exec.Command("mv", src, dst)
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

func store_file(filename string) {
	defer os.Remove(filename)
	fmt.Printf("storing: %s\n", filename)
	hash, err := hash_file(filename)
	if err != nil {
		fmt.Printf("failed to hash file: %s\n", err)
	}
	new_path := filepath.Join(KFS_STORAGE_PATH, hash)

	// TODO: acquire file lock
	move_file(filename, new_path)
	// TODO: release file lock

	/*
	 * TODO: add a record to the sqlite db with the following metadata
	 * |storage root|uuid|path|filename|hash|hash algo (blake2)|extension
	 * |file type|permissions|access time|modify time|change time|creation time
	 */

	// TODO: compress and encrypt file

	// TODO: handle errors
}

/**
 * Receive file, and write it to the staging directory.
 * When finished receiving file, run background routine to persist it to
 * durable storage and add it to the disk array.
 */
func handle_upload(writer http.ResponseWriter, request *http.Request) {
	// you can upload file with:
	// curl -X POST -F "file=@src/main.go" localhost:8080/upload
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
	fmt.Printf("got file '%s'\n", header.Filename)
	output_path := get_output_path(header.Filename)
	outf, err := os.Create(output_path)
	if err != nil {
		fmt.Printf("failed to create output file: %s\n", err)
		return
	}
	defer outf.Close()
	io.Copy(outf, file)
	store_file(output_path)
}

func initialize_db() {
	db, err := sql.Open("sqlite3", KFS_DB_PATH)
	if err != nil {
		fmt.Printf("failed to open database: %s\n", err)
		return
	}

	// TODO: create the actual schema here
	schema := `
	CREATE TABLE IF NOT EXISTS items(
		Id TEXT NOT NULL PRIMARY KEY,
		Name TEXT,
		Phone TEXT,
		InsertedDatetime DATETIME
	);
	`
	_, err = db.Exec(schema)
	if err != nil {
		panic(err)
	}
}

func main() {
	fmt.Println("KFS -- Kyle's File Storage")
	fmt.Printf("version: %s\n", KFS_VERSION)
	initialize_db()
	mux := http.NewServeMux()
	mux.HandleFunc("/", index)
	mux.HandleFunc("/upload", handle_upload)
	server := &http.Server{
		Addr:    "0.0.0.0:8080",
		Handler: mux,
	}
	log.Fatal(server.ListenAndServe())
}
