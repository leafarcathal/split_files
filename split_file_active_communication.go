package main

import (
	"bufio"
	"flag"
	"io"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"time"
)

var (
	filePathPointer  *string
	shareSizePointer *int
	file             string
	shareSize        int
)

func init() {
	filePathPointer = flag.String("filePath", "", "Full path of the file that will be read")
	shareSizePointer = flag.Int("size", 2, "Number of files that will be created")
	flag.Parse()

	file = *filePathPointer
	shareSize = *shareSizePointer
}

func main() {

	println(shareSize)

	if file == "" {
		println("FilePath is required")
		os.Exit(-1)
	}

	fileInfo := readFile(file)
	dir, _ := filepath.Split(file)
	ext := filepath.Ext(file)

	makeFile(fileInfo, shareSize, dir, ext)
}

func readFile(filePath string) []string {
	fileInfo := []string{}
	file, err := os.Open(filePath)

	if err != nil {
		println("There was an error opening file: ", err.Error())
	}

	println("Starting to read file:", file.Name())

	reader := bufio.NewReader(file)

	for {
		line, err := reader.ReadString('\n')
		line = strings.TrimSpace(line)

		if line == "consumer_id" {
			continue
		}

		if line == "" {
			break
		}

		fileInfo = append(fileInfo, line)

		if err == io.EOF {
			break
		}
	}

	file.Close()

	println("Found: ", len(fileInfo), " valid consumers to import, excluing \"consumer_id\" line and any empty lines", "\n")

	return fileInfo
}

func makeFile(fileInfo []string, numberOfSplits int, dir string, ext string) {
	totalLines := len(fileInfo)
	registerPerFile := (totalLines / numberOfSplits)
	start := 0
	startInfo := 0
	ending := registerPerFile

	for i := 1; i <= numberOfSplits; i++ {

		file, _ := os.OpenFile(
			getFileName(
				dir,
				strconv.FormatInt(int64(i), 10),
				ext,
			),
			os.O_RDWR|os.O_CREATE|os.O_APPEND,
			0666,
		)

		println("File created: ", file.Name())

		file.WriteString("consumer_id\n")

		startInfo = start

		for start <= ending {
			file.WriteString(fileInfo[start])

			if start != ending {
				file.WriteString("\n")
			}

			start++
		}

		file.Close()

		if i == numberOfSplits {
			ending = totalLines
		}

		println("Script wrote: ", registerPerFile, " lines on the file, from: ", startInfo, " to: ", ending, "\n")

		ending = start + registerPerFile

		if i == (numberOfSplits - 1) {
			ending = totalLines - 1
			registerPerFile = totalLines - start
		}
	}
}

func getFileName(dir string, suffix string, ext string) string {
	fullpath := dir + time.Now().Format("20060102") + "_" + suffix + ext
	os.Remove(fullpath)

	return fullpath
}
