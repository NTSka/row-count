package processors

import (
	"bufio"
	"bytes"
	"fmt"
	"github.com/NTSka/row-count/helpers"
	"io"
	"os"
	"path"
)

func Merge(fileCount int64, tempDir string, delimiter byte, limit int) (string, error) {
	var secondSavedRow []byte
	var firstReader, secondReader *bufio.Reader
	var writer *bufio.Writer
	writerCounter := 0

	for lastFileId := int64(0); lastFileId < fileCount; lastFileId += 2 {
		firstFile, err := os.Open(path.Join(tempDir, fmt.Sprintf("temp_%d", lastFileId)))
		if err != nil {
			return "", fmt.Errorf("error while open temp file: %v", err)
		}

		secondFile, err := os.Open(path.Join(tempDir, fmt.Sprintf("temp_%d", lastFileId+1)))
		if err != nil {
			return "", fmt.Errorf("error while open temp file: %v", err)
		}

		outFile, err := os.Create(path.Join(tempDir, fmt.Sprintf("temp_%d", fileCount+1)))
		if err != nil {
			return "", fmt.Errorf("error while creating temp file: %v", err)
		}

		// Create readers and writer if not exists or reset
		if firstReader == nil {
			firstReader = bufio.NewReader(firstFile)
			secondReader = bufio.NewReader(secondFile)
			writer = bufio.NewWriter(outFile)
		} else {
			firstReader.Reset(firstFile)
			secondReader.Reset(secondFile)
			writer.Reset(outFile)
		}

		var firstErr, secondErr error
		var row, secondRow []byte

		// Firstly got row from first file, then from second. Compare them:
		// If second row less or they are equal - we continue reading second file;
		// If first row less then second - we save last second row and continue reading first file;
		//
		// If saved second row already exists we not read second file and compare with it
		for {
			row, firstErr = firstReader.ReadBytes(delimiter)

			// If got EOF from first file - write saved row if exists and write rest of second file
			if firstErr == io.EOF {
				if secondSavedRow != nil {
					if _, err := writer.Write(secondSavedRow); err != nil {
						return "", err
					}
				}

				if err := writer.Flush(); err != nil {
					return "", err
				}

				err := helpers.WriteRest(secondReader, writer, limit)
				if err != nil {
					return "", err
				}
				break
			}

			// If got not EOF error
			if firstErr != nil {
				return "", err
			}

			// For empty row
			if len(row) == 1 {
				continue
			}

			for {

				// Check saved row existence; If exist - use it for compare, otherwise - read new
				if secondSavedRow == nil {
					secondRow, secondErr = secondReader.ReadBytes(delimiter)

					// If got EOF from first file - write rest of second file
					if secondErr == io.EOF {
						if _, err := writer.Write(row); err != nil {
							return "", err
						}

						if err := writer.Flush(); err != nil {
							return "", err
						}

						if err = helpers.WriteRest(firstReader, writer, limit); err != nil {
							return "", err
						}

						break
					}
				} else {
					secondRow = secondSavedRow
					secondSavedRow = nil
				}

				compareRes := bytes.Compare(secondRow, row)
				if len(secondRow) == 1 {
					continue
				} else if compareRes == -1 || compareRes == 0 {
					if _, err := writer.Write(secondRow); err != nil {
						return "", err
					}

					// Flush when we are close to limit
					if writerCounter >= limit-2 {
						if err := writer.Flush(); err != nil {
							return "", err
						}
					}
				} else {
					// Set saved row if second row grater the first
					secondSavedRow = secondRow
					if _, err := writer.Write(row); err != nil {
						return "", err
					}
					if writerCounter >= limit-2 {
						if err := writer.Flush(); err != nil {
							return "", err
						}
					}
					break
				}
			}
		}

		secondSavedRow = nil

		if err := firstFile.Close(); err != nil {
			return "", fmt.Errorf("closing file failed: %v", err)
		}

		if err := secondFile.Close(); err != nil {
			return "", fmt.Errorf("closing file failed: %v", err)
		}

		if err := outFile.Close(); err != nil {
			return "", fmt.Errorf("closing file failed: %v", err)
		}

		if err := os.Remove(path.Join(tempDir, fmt.Sprintf("temp_%d", lastFileId))); err != nil {
			return "", fmt.Errorf("removing file failed: %v", err)
		}

		if err := os.Remove(path.Join(tempDir, fmt.Sprintf("temp_%d", lastFileId+1))); err != nil {
			return "", fmt.Errorf("removing file failed: %v", err)
		}

		fileCount++
	}

	return path.Join(tempDir, fmt.Sprintf("temp_%d", fileCount)), nil
}
