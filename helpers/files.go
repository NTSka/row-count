package helpers

import (
	"bufio"
	"bytes"
	"io"
	"os"
	"sort"
)

var lineDelimiter = byte('\n')

// Write file from current reader position to the end to writer
func WriteRest(from *bufio.Reader, to *bufio.Writer, limit int) error {
	counter := 0
	for {
		line, err := from.ReadBytes(lineDelimiter)

		if err == io.EOF {
			if err := to.Flush(); err != nil {
				return err
			}

			break
		}

		if err != nil {
			return err
		}

		if _, err := to.Write(line); err != nil {
			return err
		}

		counter++

		// Flush if wrote rows more, than limit
		if counter >= limit-1 {
			if err := to.Flush(); err != nil {
				return err
			}
		}
	}

	return nil
}

// Create temp file and write rows
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

	if err := writer.Flush(); err != nil {
		return err
	}

	if err != nil {
		return err
	}
	return tempFile.Close()
}
