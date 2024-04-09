// Comms: WASM
// Copyright Alistair Cunningham 2024

package main

import (
	"bufio"
	"encoding/json"
	"fmt"
	"github.com/wasmerio/wasmer-go/wasmer"
	"os"
	"strconv"
	"strings"
)

var wasm_instances = map[string]*wasmer.Instance{}

func wasm_invoke(f func(...any) (any, error), out *os.File) {
	log_debug("WASM calling function")
	f()
	log_debug("WASM function finished")
	out.Write([]byte("\nfinish\n"))
}

func wasm_read(r *bufio.Reader) string {
	output, err := r.ReadString('\n')
	if err != nil {
		log_info("WASM error reading from app: %s", err)
		return ""
	}
	output = strings.TrimRight(output, "\n")
	log_debug("WASM received from app '%s'", output)
	return output
}

func wasm_run(u *User, a *App, function string, input any) (string, error) {
	log_debug("WASM running app '%s', version '%s', function '%s'", a.Name, a.Version, function)

	file := data_dir + "/apps/" + a.Name + "/" + a.Version + "/" + a.WASM.File
	wasm, err := os.ReadFile(file)
	if err != nil {
		log_warn("WASM unable to read file '%s': %v", file, err)
		return "", error_message("WASM unable to read file '%s': %v", file, err)
	}

	storage := "users/" + strconv.Itoa(u.ID) + "/apps/" + a.Name
	file_mkdir(storage)
	if !file_exists(storage + "/.in") {
		file_mkfifo(storage + "/.in")
	}
	in, err := os.OpenFile(data_dir+"/"+storage+"/.in", os.O_RDWR, 0600)
	if err != nil {
		log_warn("WASM unable to open input pipe for writing: %s", err)
		return "", error_message("WASM unable to open input pipe for writing: %s", err)
	}
	defer in.Close()
	w := bufio.NewWriter(in)

	if !file_exists(storage + "/.out") {
		file_mkfifo(storage + "/.out")
	}
	out, err := os.OpenFile(data_dir+"/"+storage+"/.out", os.O_RDWR, 0600)
	if err != nil {
		log_warn("WASM unable to open output pipe for reading: %s", err)
		return "", error_message("WASM unable to open output pipe for reading: %s", err)
	}
	defer out.Close()
	r := bufio.NewReader(out)

	key := fmt.Sprintf("%d-%s", u.ID, a.ID)
	i, found := wasm_instances[key]
	if !found {
		store := wasmer.NewStore(wasmer.NewEngine())
		module, _ := wasmer.NewModule(store, wasm)
		wasi, _ := wasmer.NewWasiStateBuilder("comms").MapDirectory("/", data_dir+"/"+storage).InheritStdout().InheritStderr().Finalize()
		io, err := wasi.GenerateImportObject(store, module)
		fatal(err)
		i, err = wasmer.NewInstance(module, io)
		fatal(err)
		start, err := i.Exports.GetWasiStartFunction()
		fatal(err)
		start()
		wasm_instances[key] = i
	}

	f, err := i.Exports.GetFunction(function)
	fatal(err)
	go wasm_invoke(f, out)

	ji, err := json.Marshal(input)
	if err != nil {
		log_warn("WASM unable to marshal JSON for app input: %s", err)
	}
	wasm_write(w, string(ji))

	var run_return string
	for {
		read := wasm_read(r)
		splits := strings.SplitN(read, " ", 2)
		if len(splits) == 0 {
			log_info("WASM app returned invalid message")
		}
		action := splits[0]
		output := ""
		if len(splits) > 1 {
			output = splits[1]
		}

		switch action {
		case "finish":
			return run_return, nil

		case "service":
			log_debug("WASM app asked for a service")
			//TODO Check for recursion
			splits := strings.SplitN(output, " ", 3)
			if len(splits) >= 2 {
				var service_return []byte
				var err error
				if len(splits) > 2 {
					service_return, err = service_json(u, splits[0], splits[1], splits[2])
				} else {
					service_return, err = service_json(u, splits[0], splits[1])
				}
				if err != nil {
					log_info("WASM call to service returned error: %s", err)
				}
				wasm_write(w, string(service_return))
			} else {
				log_info("WASM app called service without app and service; ignoring service request")
			}

		case "return":
			run_return = output
		}
	}

	return run_return, nil
}

func wasm_write(w *bufio.Writer, data string) {
	log_debug("Writing to app '%s'", data)
	w.WriteString(data)
	w.WriteRune('\n')
	w.Flush()
}
