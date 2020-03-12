package processors

import (
	"bufio"
	"fmt"
	"io"
	"os"
)

func Count(outPath, lastTempFileName string, delimiter byte) error {
	outFile, err := os.Create(outPath)
	if err != nil {
		return fmt.Errorf("can't create output file: %v", err)
	}

	tempFile, err := os.OpenFile(lastTempFileName, os.O_RDONLY, 0665)
	if err != nil {
		return fmt.Errorf("can't open temp file: %v", err)
	}

	lastRow := ""
	count := int64(1)
	inputReader := bufio.NewReader(tempFile)
	outWriter := bufio.NewWriter(outFile)

	for {
		row, err := inputReader.ReadBytes(delimiter)
		if err == io.EOF {

			// For empty input file
			if len(lastRow) == 0 {
				break
			}

			if _, err := outWriter.WriteString(fmt.Sprintf("%s\t%d\n", lastRow[0:len(lastRow)-1], count)); err != nil {
				return err
			}

			if err := outWriter.Flush(); err != nil {
				return err
			}

			break
		}

		if err != nil {
			return err
		}

		strRow := string(row)

		if strRow != lastRow {
			if lastRow != "" {
				if _, err := outWriter.WriteString(fmt.Sprintf("%s\t%d\n", lastRow[0:len(lastRow)-1], count)); err != nil {
					return err
				}

				if err := outWriter.Flush(); err != nil {
					return err
				}
			}

			lastRow = strRow
			count = 1
		} else {
			count++
		}
	}

	if err := os.Remove(lastTempFileName); err != nil {
		return fmt.Errorf("error while removing temp file: %v", err)
	}

	return nil
}
