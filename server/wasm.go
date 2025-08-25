// Mochi: WASM
// Copyright Alistair Cunningham 2024-2025

package main

/* None of this is used
import (
	"bufio"
	"fmt"
	"github.com/wasmerio/wasmer-go/wasmer"
	"os"
	"strings"
)

var (
	wasm_invoke_id int64 = 0
)

func wasm_cleanup(storage string, id int64) {
	file_delete(fmt.Sprintf("%s/.in-%d", storage, wasm_invoke_id))
	file_delete(fmt.Sprintf("%s/.out-%d", storage, wasm_invoke_id))
}

func wasm_invoke(f func(...any) (any, error), out *os.File, id int64) {
	defer wasm_finish(out, id)
	debug("WASM calling function")
	f(id)
}

func wasm_finish(out *os.File, id int64) {
	recover()
	debug("WASM function finished")
	out.Write([]byte("\nfinish\n"))
}

func wasm_read(r *bufio.Reader) string {
	output, err := r.ReadString('\n')
	if err != nil {
		info("WASM error reading from app: %s", err)
		return ""
	}
	output = strings.TrimRight(output, "\n")
	debug("WASM received from app '%s'", output)
	return output
}

func wasm_run(u *User, a *App, function string, depth int, input any) (string, error) {
	debug("WASM running app '%s', version '%s', function '%s'", a.Name, a.Version, function)

	file := data_dir + "/apps/" + a.Name + "/" + a.Version + "/" + a.WASM.File
	wasm, err := os.ReadFile(file)
	if err != nil {
		warn("WASM unable to read file '%s': %v", file, err)
		return "", error_message("WASM unable to read file '%s': %v", file, err)
	}

	storage := fmt.Sprintf("%s/users/%d/%d", data_dir, u.ID, u.Identity.ID, a.ID)
	if !file_exists(storage) {
		file_mkdir(storage)
	}
	wasm_invoke_id = wasm_invoke_id + 1
	defer wasm_cleanup(storage, wasm_invoke_id)

	in_fifo := fmt.Sprintf("%s/.in-%d", storage, wasm_invoke_id)
	if !file_exists(in_fifo) {
		file_mkfifo(in_fifo)
	}
	in, err := os.OpenFile(data_dir+"/"+in_fifo, os.O_RDWR, 0600)
	if err != nil {
		warn("WASM unable to open input pipe for writing: %s", err)
		return "", error_message("WASM unable to open input pipe for writing: %s", err)
	}
	defer in.Close()
	w := bufio.NewWriter(in)

	out_fifo := fmt.Sprintf("%s/.out-%d", storage, wasm_invoke_id)
	if !file_exists(out_fifo) {
		file_mkfifo(out_fifo)
	}
	out, err := os.OpenFile(data_dir+"/"+out_fifo, os.O_RDWR, 0600)
	if err != nil {
		warn("WASM unable to open output pipe for reading: %s", err)
		return "", error_message("WASM unable to open output pipe for reading: %s", err)
	}
	defer out.Close()
	r := bufio.NewReader(out)

	store := wasmer.NewStore(wasmer.NewEngine())
	module, _ := wasmer.NewModule(store, wasm)
	wasi, _ := wasmer.NewWasiStateBuilder("mochi").MapDirectory("/", data_dir+"/"+storage).InheritStdout().InheritStderr().Finalize()
	io := must(wasi.GenerateImportObject(store, module))
	i := must(wasmer.NewInstance(module, io))
	start := must(i.Exports.GetWasiStartFunction())
	start()

	f, err := i.Exports.GetFunction(function)
	if err != nil {
		info("WASM unable to find function '%s': %s", function, err)
		return "", error_message("WASM unable to find function '%s': %s", function, err)
	}
	go wasm_invoke(f, out, wasm_invoke_id)
	wasm_write(w, json_encode(input))

	var run_return string
	for {
		read := wasm_read(r)
		splits := strings.SplitN(read, " ", 2)
		if len(splits) == 0 {
			info("WASM app returned invalid message")
		}
		action := splits[0]
		output := ""
		if len(splits) > 1 {
			output = splits[1]
		}

		switch action {
		case "finish":
			return run_return, nil

		case "return":
			run_return = output
		}
	}

	return run_return, nil
}

func wasm_write(w *bufio.Writer, data string) {
	debug("WASM writing to app '%s'", data)
	w.WriteString(data)
	w.WriteRune('\n')
	w.Flush()
}
*/
