package main

import (
	"flag"
	"fmt"
	"github.com/NTSka/row-count/processors"
	"os"
	"path/filepath"
	"time"
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

	if *limit == 0 {
		*limit = 150
	}

	if *limit > 950 {
		panic("Alarm, limit too large")
	}

	if *limit < 150 {
		panic("Alarm, limit too small")
	}

	inputPath := filepath.Join(dir, *inputFilePath)
	outputPath := filepath.Join(dir, *outPutFilePath)

	// Create temp dir if not exists
	tempDir := filepath.Join(dir, *tempPath)
	if _, err := os.Stat(tempDir); os.IsNotExist(err) {
		if err := os.MkdirAll(tempDir, 0766); err != nil {
			fmt.Println(err)
			return
		}
	}

	inputFile, err := os.Open(inputPath)
	if err != nil {
		fmt.Println("Can't open input file")
		fmt.Println(err)
		return
	}

	defer inputFile.Close()

	t := time.Now()

	// Split input file to the sorted temp files; Return count of temp files
	fileCount, err := processors.SplitFile(inputFile, tempDir, lineDelimiter, *limit)
	if err != nil {
		fmt.Println("Error while splitting")
		fmt.Println(err)
		return
	}
	fmt.Println("Files split successful")

	// Merge sorted files to one sorted file; Return path of the last temp file
	lastTempFileName, err := processors.Merge(fileCount, tempDir, lineDelimiter, *limit)
	if err != nil {
		fmt.Println("Error while merging")
		fmt.Println(err)
		return
	}
	fmt.Println("Files merge successful")

	// Count repeated rows int last temp file
	if err := processors.Count(outputPath, lastTempFileName, lineDelimiter); err != nil {
		fmt.Println("Error while counting")
		fmt.Println(err)
		return
	}

	fmt.Printf("Success, you can see result here: %s \n", outputPath)
	fmt.Println("Time: ", time.Now().Unix()-t.Unix())
}
