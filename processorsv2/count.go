package processors

import (
	"bufio"
	"bytes"
	"emperror.dev/errors"
	"fmt"
	"io"
	"os"
	"path"
	"strconv"
	"strings"
)

func Count(targetDir string, outputFile string, delim byte) error {
	out, err := os.OpenFile(outputFile, os.O_CREATE|os.O_RDWR, os.ModePerm)
	if err != nil {
		return errors.Wrap(err, "os.OpenFile")
	}

	for i := 0; i < 16; i++ {
		//for j := 0; j < 16; j++ {
		//for k := 0; k < 16; k++ {
		for m := 0; m < 16; m++ {
			dirPath := path.Join(targetDir, fmt.Sprintf("%x", i), fmt.Sprintf("%x", m))
			dir, err := os.ReadDir(dirPath)
			if err != nil {
				return errors.Wrap(err, "os.ReadDir")
			}

			for _, filename := range dir {
				f, err := os.Open(path.Join(dirPath, filename.Name()))
				if err != nil {
					return errors.Wrap(err, "os.Open")
				}

				reader := bufio.NewReader(f)
				for {
					r, err := reader.ReadBytes(delim)
					if err == io.EOF {
						break
					}
					if err != nil {
						return errors.Wrap(err, "reader.ReadBytes")
					}

					split := bytes.Split(r, d)

					n, err := strconv.Atoi(strings.TrimSpace(string(split[1])))
					if err != nil {
						return errors.Wrap(err, "strconv.Atoi")
					}

					v := append(split[0], d...)
					v = append(v, []byte(strconv.Itoa(n))...)
					v = append(v, []byte("\n")...)

					if _, err := out.Write(v); err != nil {
						return errors.Wrap(err, "out.Write")
					}
				}
				f.Close()
				if err := os.Remove(f.Name()); err != nil {
					return errors.Wrap(err, "os.Remove")
				}
			}
			if err := os.Remove(path.Join(targetDir, fmt.Sprintf("%x", i), fmt.Sprintf("%x", m))); err != nil {
				return errors.Wrap(err, "os.Remove")
			}
		}
		//}
		//}
		if err := os.Remove(path.Join(targetDir, fmt.Sprintf("%x", i))); err != nil {
			return errors.Wrap(err, "os.Remove")
		}
	}

	if err := os.Remove(targetDir); err != nil {
		return errors.Wrap(err, "os.Remove")
	}

	return nil
}
