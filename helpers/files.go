package helpers

import (
	"bufio"
	"bytes"
	"io"
	"os"
	"sort"
)

var lineDelimiter = byte('\n')

func WriteRest(from *bufio.Reader, to *bufio.Writer, limit int) error {
	counter := 0
	for {
		line, err := from.ReadBytes(lineDelimiter)
		if err == io.EOF {
			to.Flush()
			break
		}

		if err != nil {
			return err
		}

		to.Write(line)
		counter++
		if counter >= limit {
			to.Flush()
		}
	}

	return nil
}

func WriteTemp(fileName string, rows [][]byte) error {
	sort.Slice(rows, func(i, j int) bool {
		return bytes.Compare(rows[i], rows[j]) == -1
	})

	tempFile, err := os.Create(fileName)
	if err != nil {
		return err
	}

	res := bytes.Join(rows, nil)
	writer := bufio.NewWriter(tempFile)
	_, err = writer.Write(res)
	writer.Flush()
	if err != nil {
		return err
	}
	return tempFile.Close()
}
