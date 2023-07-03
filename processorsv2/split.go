package processors

import (
	"bufio"
	"bytes"
	"crypto/sha256"
	"emperror.dev/errors"
	"fmt"
	"io"
	"os"
	"path"
	"strconv"
)

func SplitFile(inputFileName string, targetDir string, delimiter byte) error {
	inputFile, err := os.Open(inputFileName)
	if err != nil {
		return errors.Wrap(err, "os.Open")
	}
	defer inputFile.Close()

	inputReader := bufio.NewReader(inputFile)

	for i := 0; i < 16; i++ {
		//for j := 0; j < 16; j++ {
		if err := os.Mkdir(path.Join(targetDir, fmt.Sprintf("%x", i)), os.ModeDir); err != nil {
			return errors.Wrap(err, "os.Mkdir")
		}
		//for k := 0; k < 16; k++ {
		for m := 0; m < 16; m++ {
			if err := os.Mkdir(path.Join(targetDir, fmt.Sprintf("%x", i), fmt.Sprintf("%x", m)), os.ModeDir); err != nil {
				return errors.Wrap(err, "os.Mkdir")
			}
		}
		//}
		//}
	}

	fmt.Println("Directory created")

	for {
		row, err := inputReader.ReadBytes(delimiter)

		if err == io.EOF {
			break
		}

		h := sha256.New()
		h.Write(row)

		if err := addRow(targetDir, h.Sum(nil), bytes.TrimSpace(row), delimiter); err != nil {
			return errors.Wrap(err, "addRow")
		}
	}

	return nil
}

var d = []byte("\t")

var dLen = int64(len(d))

func addRow(targetDirPath string, hash, row []byte, delimiter byte) error {
	hashStr := fmt.Sprintf("%x", hash)
	file, err := os.OpenFile(path.Join(targetDirPath, hashStr[0:1], hashStr[1:2], hashStr[2:3]+".t"), os.O_CREATE|os.O_RDWR, os.ModePerm)
	if err != nil {
		return errors.Wrap(err, "os.OpenFile")
	}

	defer file.Close()

	reader := bufio.NewReader(file)
	total := int64(0)

	for {
		r, err := reader.ReadBytes(delimiter)
		if err == io.EOF {
			break
		}

		split := bytes.Split(r, d)
		if bytes.Compare(bytes.TrimSpace(split[0]), row) == 0 {
			rawNum := bytes.TrimSpace(split[1])
			n, err := strconv.Atoi(string(rawNum))
			if err != nil {
				return errors.Wrap(err, "strconv.Atoi")
			}
			nextNum := fmt.Sprintf("%020d", uint64(n+1))
			if _, err := file.WriteAt(append([]byte(nextNum), delimiter), total+int64(len(split[0]))+dLen); err != nil {
				return errors.Wrap(err, "file.WriteAt")
			}
			return nil
		}

		total += int64(len(r))
	}

	n := fmt.Sprintf("%020d", uint64(1))
	r := append(bytes.TrimSpace(row), d...)
	r = append(r, []byte(n)...)
	r = append(r, delimiter)
	if _, err = file.Write(r); err != nil {
		return errors.Wrap(err, "file.Write")
	}

	return nil
}
