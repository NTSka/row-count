package main

import (
	"bufio"
	"bytes"
	"flag"
	"fmt"
	"github.com/NTSka/row-count/helpers"
	"io"
	"os"
	"path"
	"path/filepath"
)

var (
	inputFilePath  = flag.String("input", "", "Input file path")
	outPutFilePath = flag.String("output", "", "Output file path")
	tempPath       = flag.String("temp", "", "Temp dir path")
	limit          = flag.Int("limit", 150, "Rows in memory")
)

var lineDelimiter = byte('\n')

func main() {
	flag.Parse()

	dir, _ := os.Getwd()

	if inputFilePath == nil || *inputFilePath == "" {
		panic("Alarm, input file path not provided")
	}

	if outPutFilePath == nil {
		panic("Alarm, output file path not provided")
	}

	if *limit > 950 {
		panic("Alarm, limit too large")
	}

	if *limit < 150 {
		panic("Alarm, limit too small")
	}

	if *limit == 0 {
		*limit = 150
	}

	inputPath := filepath.Join(dir, *inputFilePath)
	outputPath := filepath.Join(dir, *outPutFilePath)
	tempDir := filepath.Join(dir, *tempPath)

	inputFile, err := os.Open(inputPath)
	if err != nil {
		fmt.Println(err)
		return
	}

	defer inputFile.Close()

	fileCount := 0
	rows := make([][]byte, 0)

	inputReader := bufio.NewReader(inputFile)

	for {
		row, err := inputReader.ReadBytes(lineDelimiter)

		if err == io.EOF {
			if err := helpers.WriteTemp(filepath.Join(tempDir, fmt.Sprintf("temp_%d", fileCount)), rows); err != nil {
				fmt.Println(err)
				return
			}
			break
		}
		if err != nil {
			fmt.Print(err)
			return
		}

		rows = append(rows, row)
		if len(rows) == *limit {
			if err := helpers.WriteTemp(filepath.Join(tempDir, fmt.Sprintf("temp_%d", fileCount)), rows); err != nil {
				fmt.Println(err)
				return
			}
			fileCount++

			rows = make([][]byte, 0)

		}
	}
	fmt.Println("SUCCSES")

	var secondSavedRow []byte
	var firstReader, secondReader *bufio.Reader
	var writer *bufio.Writer
	writerCounter := 0

	for lastFileId := 0; lastFileId < fileCount; lastFileId += 2 {
		firstFile, err := os.Open(path.Join(tempDir, fmt.Sprintf("temp_%d", lastFileId)))
		if err != nil {
			fmt.Println(err)
			return
		}

		secondFile, err := os.Open(path.Join(tempDir, fmt.Sprintf("temp_%d", lastFileId+1)))
		if err != nil {
			fmt.Println(err)
		}

		outFile, err := os.Create(path.Join(tempDir, fmt.Sprintf("temp_%d", fileCount+1)))
		if err != nil {
			fmt.Println(err)
			return
		}

		if firstReader == nil {
			firstReader = bufio.NewReader(firstFile)
			secondReader = bufio.NewReader(secondFile)
		} else {
			firstReader.Reset(firstFile)
			secondReader.Reset(secondFile)
		}

		if writer == nil {
			writer = bufio.NewWriter(outFile)
		} else {
			writer.Reset(outFile)
		}

		var firstErr, secondErr error
		var row, secondRow []byte
		for firstErr != io.EOF && secondErr != io.EOF {
			row, firstErr = firstReader.ReadBytes(lineDelimiter)
			if firstErr == io.EOF {
				if secondSavedRow != nil {
					writer.Write(secondSavedRow)
				}
				writer.Flush()
				err := helpers.WriteRest(secondReader, writer, *limit)
				if err != nil {
					fmt.Println(err)
					return
				}
				break
			}

			if firstErr != nil {
				fmt.Println(firstErr)
				return
			}
			if len(row) == 0 {
				continue
			}

			for {
				if secondSavedRow == nil {
					secondRow, secondErr = secondReader.ReadBytes(lineDelimiter)
					if secondErr == io.EOF {
						writer.Write(row)
						writer.Flush()
						if err = helpers.WriteRest(firstReader, writer, *limit); err != nil {
							fmt.Println(err)
							return
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
					writer.Write(secondRow)
					if writerCounter >= *limit-2 {
						writer.Flush()
					}
				} else {
					secondSavedRow = secondRow
					writer.Write(row)
					if writerCounter >= *limit-2 {
						writer.Flush()
					}
					break
				}
			}
		}

		secondSavedRow = nil

		firstFile.Close()
		secondFile.Close()
		outFile.Close()

		if err := os.Remove(path.Join(tempDir, fmt.Sprintf("temp_%d", lastFileId))); err != nil {
			fmt.Println(err)
			return
		}

		if err := os.Remove(path.Join(tempDir, fmt.Sprintf("temp_%d", lastFileId+1))); err != nil {
			fmt.Println(err)
			return
		}

		fileCount++
	}

	outFile, err := os.Create(outputPath)
	if err != nil {
		fmt.Println(err)
		return
	}

	tempFile, err := os.OpenFile(path.Join(tempDir, fmt.Sprintf("temp_%d", fileCount)), os.O_RDONLY, 0665)
	if err != nil {
		fmt.Println(err)
	}

	lastRow := ""
	count := int64(1)
	inputReader = bufio.NewReader(tempFile)
	outWriter := bufio.NewWriter(outFile)
	for {
		row, err := inputReader.ReadBytes(lineDelimiter)
		if err == io.EOF {
			_, err := outWriter.WriteString(fmt.Sprintf("%s \t %d \n", lastRow[0:len(lastRow)-1], count))
			outWriter.Flush()
			if err != nil {
				fmt.Println(err)
				return
			}
			break
		}

		if err != nil {
			fmt.Println(err)
			return
		}

		strRow := string(row)

		if strRow != lastRow {
			if lastRow != "" {
				_, err := outWriter.WriteString(fmt.Sprintf("%s \t %d \n", lastRow[0:len(lastRow)-1], count))
				outWriter.Flush()
				if err != nil {
					fmt.Println(err)
					return
				}
			}

			lastRow = strRow
			count = 1
		} else {
			count++
		}
	}
}
