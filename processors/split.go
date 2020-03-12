package processors

import (
	"bufio"
	"fmt"
	"github.com/NTSka/row-count/helpers"
	"io"
	"os"
	"path/filepath"
)

func SplitFile(inputFile *os.File, targetDir string, delimiter byte, rowLimit int) (int64, error) {
	fileCount := int64(0)
	rows := make([][]byte, 0)
	inputReader := bufio.NewReader(inputFile)

	for {
		row, err := inputReader.ReadBytes(delimiter)

		// If EOF - finish last temp file and finish function
		if err == io.EOF {
			if err := helpers.WriteTemp(filepath.Join(targetDir, fmt.Sprintf("temp_%d", fileCount)), rows); err != nil {
				return 0, err
			}
			break
		}

		if err != nil {
			return 0, err
		}

		rows = append(rows, row)
		if len(rows) == rowLimit {
			if err := helpers.WriteTemp(filepath.Join(targetDir, fmt.Sprintf("temp_%d", fileCount)), rows); err != nil {
				return 0, err
			}
			fileCount++

			rows = make([][]byte, 0)
		}
	}

	return fileCount, nil
}
