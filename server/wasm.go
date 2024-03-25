// Comms: WASM
// Copyright Alistair Cunningham 2024

package main

import (
	"fmt"
	"io/ioutil"

	wasmer "github.com/wasmerio/wasmer-go/wasmer"
)

func wasm_run(file string, function string, values ...any) {
	file = base_dir + file
	wasm, err := ioutil.ReadFile(file)
	if err != nil {
		log_warn("Unable to read file '%s': %v", file, err)
		return
	}

	store := wasmer.NewStore(wasmer.NewEngine())
	module, _ := wasmer.NewModule(store, wasm)
	wasi, _ := wasmer.NewWasiStateBuilder("comms").PreopenDirectory(base_dir + "data").Finalize()
	io, err := wasi.GenerateImportObject(store, module)
	fatal(err)

	instance, err := wasmer.NewInstance(module, io)
	fatal(err)
	start, err := instance.Exports.GetWasiStartFunction()
	fatal(err)
	start()
	f, err := instance.Exports.GetFunction(function)
	fatal(err)
	result, _ := f(values...)
	fmt.Printf("Result='%v'\n", result)
}
