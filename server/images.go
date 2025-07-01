// Comms: Images
// Copyright Alistair Cunningham 2025

package main

import (
	"fmt"
	"github.com/nfnt/resize"
	"image"
	"image/gif"
	"image/jpeg"
	"image/png"
	"os"
	"path/filepath"
	"strings"
)

func thumbnail_create(file string) (string, error) {
	thumb := thumbnail_name(file)
	log_debug("Getting thumbnail '%s' for '%s'", thumb, file)
	if file_exists(thumb) {
		return thumb, nil
	}
	log_debug("Thumbnail does not exist, generating...")

	f, err := os.Open(file)
	if err != nil {
		log_warn("Unable to open image file '%s' to create thumbnail: %v", file, err)
		return "", err
	}
	defer f.Close()

	i, format, err := image.Decode(f)
	if err != nil {
		log_info("Unable to decode image file '%s' to create thumbnail: %v", file, err)
		return "", err
	}

	t := resize.Thumbnail(250, 250, i, resize.Lanczos3)

	o, err := os.Create(thumb)
	check(err)
	defer o.Close()

	switch format {
	case "gif":
		err = gif.Encode(o, t, nil)
	case "jpeg":
		err = jpeg.Encode(o, t, nil)
	case "png":
		err = png.Encode(o, t)
	default:
		return "", nil
	}

	if err != nil {
		log_info("Unable to encode image file '%s' to create thumbnail: %v", file, err)
		return "", err
	}

	return thumb, nil
}

func thumbnail_name(name string) string {
	ext := filepath.Ext(name)
	strings.TrimSuffix(name, ext)
	return fmt.Sprintf("%s_thumbnail%s", strings.TrimSuffix(name, ext), ext)
}
