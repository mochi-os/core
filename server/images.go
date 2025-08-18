// Mochi: Images
// Copyright Alistair Cunningham 2025

package main

import (
	"github.com/nfnt/resize"
	"image"
	"image/gif"
	"image/jpeg"
	"image/png"
	"os"
	"path/filepath"
	"strings"
)

func is_image(file string) bool {
	ext := filepath.Ext(file)

	switch ext {
	case ".gif":
		return true
	case ".jpeg":
		return true
	case ".jpg":
		return true
	case ".png":
		return true
	case ".webp":
		return true
	}

	return false
}

func thumbnail_create(path string) (string, error) {
	dir, file := filepath.Split(path)
	thumb := dir + "thumbnail/" + thumbnail_name(file)

	if file_exists(thumb) {
		return thumb, nil
	}

	f, err := os.Open(path)
	if err != nil {
		warn("Unable to open image file '%s' to create thumbnail: %v", path, err)
		return "", err
	}
	defer f.Close()

	i, format, err := image.Decode(f)
	if err != nil {
		info("Unable to decode image file '%s' to create thumbnail: %v", path, err)
		return "", err
	}

	t := resize.Thumbnail(250, 250, i, resize.Lanczos3)

	file_mkdir(dir + "thumbnail")
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
		info("Unable to encode image file '%s' to create thumbnail: %v", path, err)
		return "", err
	}

	return thumb, nil
}

func thumbnail_name(name string) string {
	ext := filepath.Ext(name)
	return strings.TrimSuffix(name, ext) + "_thumbnail" + ext
}
