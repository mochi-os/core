// Mochi: Images
// Copyright Alistair Cunningham 2025

package main

import (
	"bytes"
	"github.com/disintegration/imaging"
	"github.com/nfnt/resize"
	"github.com/rwcarlsen/goexif/exif"
	"image"
	"image/gif"
	"image/jpeg"
	"image/png"
	"io"
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
	thumb := dir + "thumbnails/" + thumbnail_name(file)
	tmp := thumb + ".tmp"

	// Clean up any leftover temp file from a previous failed attempt
	_ = os.Remove(tmp)

	if file_exists(thumb) {
		return thumb, nil
	}

	f, err := os.Open(path)
	if err != nil {
		warn("Unable to open image file %q to create thumbnail: %v", path, err)
		return "", err
	}
	defer f.Close()

	// Read the file into memory so we can inspect EXIF and decode the image
	b, err := io.ReadAll(f)
	if err != nil {
		info("Unable to decode image file %q to create thumbnail: %v", path, err)
		return "", err
	}

	// Try to parse EXIF orientation (best-effort)
	var orientation int
	if ex, err := exif.Decode(bytes.NewReader(b)); err == nil && ex != nil {
		if tag, err := ex.Get(exif.Orientation); err == nil && tag != nil {
			if iv, err := tag.Int(0); err == nil {
				orientation = iv
			}
		}
	}

	i, format, err := image.Decode(bytes.NewReader(b))
	if err != nil {
		warn("Unable to decode image file %q to create thumbnail: %v", path, err)
		return "", err
	}

	// Fix orientation according to EXIF tag before resizing
	if orientation != 0 {
		i = fix_orientation(i, orientation)
	}

	t := resize.Thumbnail(250, 250, i, resize.Lanczos3)

	file_mkdir_for_file(thumb)

	o, err := os.Create(tmp)
	if err != nil {
		warn("Unable to create temp thumbnail file %q: %v", tmp, err)
		return "", err
	}
	// ensure tmp is closed and removed on error
	close_and_remove_tmp := func(remove bool) {
		_ = o.Close()
		if remove {
			_ = os.Remove(tmp)
		}
	}

	switch format {
	case "gif":
		err = gif.Encode(o, t, nil)
	case "jpeg":
		err = jpeg.Encode(o, t, &jpeg.Options{Quality: 80})
	case "png":
		err = png.Encode(o, t)
	case "webp":
		err = imaging.Encode(o, t, imaging.JPEG, imaging.JPEGQuality(80))
	default:
		close_and_remove_tmp(true)
		return "", nil
	}

	if err != nil {
		close_and_remove_tmp(true)
		info("Unable to encode image file %q to create thumbnail: %v", path, err)
		return "", err
	}

	if err := o.Close(); err != nil {
		_ = os.Remove(tmp)
		info("Unable to close thumbnail file %q: %v", tmp, err)
		return "", err
	}

	if err := os.Rename(tmp, thumb); err != nil {
		_ = os.Remove(tmp)
		info("Unable to move thumbnail into place %q: %v", thumb, err)
		return "", err
	}

	return thumb, nil
}

func thumbnail_name(name string) string {
	ext := filepath.Ext(name)
	return strings.TrimSuffix(name, ext) + "_thumbnail" + ext
}

// thumbnail_path returns the thumbnail path for a given original file path
func thumbnail_path(path string) string {
	dir, file := filepath.Split(path)
	return dir + "thumbnails/" + thumbnail_name(file)
}

func fix_orientation(img image.Image, orientation int) image.Image {
	switch orientation {
	case 2:
		return imaging.FlipH(img)
	case 3:
		return imaging.Rotate180(img)
	case 4:
		return imaging.FlipV(img)
	case 5:
		// transpose: rotate 270 then flip horizontal (EXIF semantics)
		return imaging.FlipH(imaging.Rotate270(img))
	case 6:
		// rotate 90 CW (EXIF semantics) -> use Rotate270 to achieve 90 CW
		return imaging.Rotate270(img)
	case 7:
		// transverse: rotate 90 then flip horizontal (EXIF semantics)
		return imaging.FlipH(imaging.Rotate90(img))
	case 8:
		// rotate 270 CW (EXIF semantics) -> use Rotate90 to achieve 270 CW
		return imaging.Rotate90(img)
	default:
		return img
	}
}
