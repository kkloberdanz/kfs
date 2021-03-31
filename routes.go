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
	"path/filepath"

	"github.com/julienschmidt/httprouter"
)

func index(writer http.ResponseWriter, request *http.Request, p httprouter.Params) {
	fmt.Fprintf(writer, "KFS version: %s\n", KFS_VERSION)
}

/**
 * Check if the hash already exists on the server
 */
func handle_exists(writer http.ResponseWriter, request *http.Request, p httprouter.Params) {
	hash := p.ByName("hash")
	if db_has_hash(hash) {
		log.Printf("hash: %s exists", hash)
		fmt.Fprintf(writer, "yes")
	} else {
		log.Printf("hash: %s does not exist", hash)
		fmt.Fprintf(writer, "no")
	}
}

/**
 * Receive file, and write it to the staging directory.
 * When finished receiving file, run background routine to persist it to
 * durable storage and add it to the disk array.
 */
func handle_upload(writer http.ResponseWriter, request *http.Request, p httprouter.Params) {
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
	log.Println("handling upload")

	file, header, err := request.FormFile("file")
	if err != nil {
		http.Error(
			writer,
			"file upload requires key of 'file'",
			http.StatusBadRequest,
		)
		fmt.Fprintf(writer, "error")
		return
	}
	defer file.Close()
	client_hash := request.FormValue("hash")
	client_path := request.FormValue("path")
	size := header.Size
	fmt.Printf(
		"got file '%s/%s', size: %d, blake2b hash: %s\n",
		client_path,
		header.Filename,
		size,
		client_hash,
	)

	skip, staging_path, storage_paths, err := db_alloc_storage(client_hash, size, client_path)
	if err != nil {
		msg := fmt.Sprintf("could not store '%s': %v", header.Filename, err)
		log.Println(msg)
		writer.WriteHeader(http.StatusInternalServerError)
		fmt.Fprintf(writer, "%s", msg)
		return
	}
	if skip {
		log.Printf("skipping, already have hash: %s", client_hash)
		fmt.Fprintf(writer, "ok")
		return
	}
	fmt.Printf("staging: %s, storage: %s\n", staging_path, storage_paths)

	output_path := get_output_path(staging_path, header.Filename)
	outf, err := os.Create(output_path)
	if err != nil {
		writer.WriteHeader(http.StatusInternalServerError)
		log.Printf("failed to create output file: %s\n", err)
		return
	}
	defer outf.Close()
	io.Copy(outf, file)

	hash, err := hash_file(output_path)
	if err != nil {
		log.Printf("failed to hash file: %s\n", err)
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
	outf.Close()
	go archive_file(staging_path, storage_paths, hash_filename, hash)
	fmt.Fprintf(writer, "ok")
}
