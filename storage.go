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
	"log"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"sync"

	uuid "github.com/satori/go.uuid"
)

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
