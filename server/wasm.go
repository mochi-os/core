// Comms: WASM
// Copyright Alistair Cunningham 2024

package main

import (
	"bufio"
	"encoding/json"
	"github.com/wasmerio/wasmer-go/wasmer"
	"os"
	"strconv"
	"strings"
)

func wasm_invoke(f func(...any) (any, error), out *os.File) {
	log_debug("WASM calling function")
	f()
	log_debug("WASM function finished")
	out.Write([]byte("\nfinish\n"))
}

func wasm_read(r *bufio.Reader, def string) string {
	data, err := r.ReadString('\n')
	if err != nil {
		log_info("WASM error reading from app: %s", err)
		return def
	}
	data = strings.TrimRight(data, "\n")
	log_debug("WASM received from app '%s'", data)
	return data
}

func wasm_run(u *User, app string, track string, function string, data ...any) any {
	log_debug("WASM running app '%s', track '%s', function '%s'", app, track, function)

	file := data_dir + "/apps/" + app + "/" + track + "/app.wasm"
	wasm, err := os.ReadFile(file)
	if err != nil {
		log_warn("WASM unable to read file '%s': %v", file, err)
		return nil
	}

	storage := "users/" + strconv.Itoa(u.ID) + "/apps/" + app
	file_mkdir(storage)
	if !file_exists(storage + "/.in") {
		file_mkfifo(storage + "/.in")
	}
	in, err := os.OpenFile(data_dir+"/"+storage+"/.in", os.O_RDWR, 0600)
	if err != nil {
		log_warn("WASM unable to open input pipe for writing: %s", err)
		return nil
	}
	defer in.Close()
	w := bufio.NewWriter(in)

	if !file_exists(storage + "/.out") {
		file_mkfifo(storage + "/.out")
	}
	out, err := os.OpenFile(data_dir+"/"+storage+"/.out", os.O_RDWR, 0600)
	if err != nil {
		log_warn("WASM unable to open output pipe for reading: %s", err)
		return nil
	}
	defer out.Close()
	r := bufio.NewReader(out)

	store := wasmer.NewStore(wasmer.NewEngine())
	module, _ := wasmer.NewModule(store, wasm)
	wasi, _ := wasmer.NewWasiStateBuilder("comms").MapDirectory("/", data_dir+"/"+storage).InheritStdout().InheritStderr().Finalize()
	io, err := wasi.GenerateImportObject(store, module)
	fatal(err)
	instance, err := wasmer.NewInstance(module, io)
	fatal(err)
	start, err := instance.Exports.GetWasiStartFunction()
	fatal(err)
	start()

	f, err := instance.Exports.GetFunction(function)
	fatal(err)
	go wasm_invoke(f, out)

	wasm_write(w, data...)

	for {
		action := wasm_read(r, "finish")
		switch action {
		case "finish":
			log_debug("App has finished")
			return nil
		case "service":
			log_debug("App asked for a service")
		}
	}
	return nil
}

func wasm_write(w *bufio.Writer, data ...any) {
	j, err := json.Marshal(data)
	fatal(err)
	log_debug("Writing to app '%s'", j)
	w.Write(j)
	w.WriteRune('\n')
	w.Flush()
}
