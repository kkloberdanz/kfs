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
	"fmt"
	"io"
	"log"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"sync"

	_ "github.com/mattn/go-sqlite3"
	uuid "github.com/satori/go.uuid"
)

const (
	KFS_VERSION = "0.0.1"
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
	skip, staging_path, storage_paths, err := AllocStorage(client_hash, size)
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

func main() {
	fmt.Println("KFS -- Kyle's File Storage")
	fmt.Printf("version: %s\n", KFS_VERSION)
	db_init()
	defer db_close()
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
