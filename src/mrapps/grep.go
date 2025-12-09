package main

import (
	"6.5840/mr"
	"bufio"
	"fmt"
	"log"
	"os"
	"sort"
	"strings"
)

var pattern string

func init() {
	pattern = os.Getenv("MR_GREP_PATTERN")
	if pattern == "" {
		log.Fatal("set MR_GREP_PATTERN to the substring you want to search for")
	}
}

// Map scans the file line by line and emits a record for every matching line.
// Value format: "<filename>:<line number>:<line contents>".
func Map(filename string, contents string) []mr.KeyValue {
	scanner := bufio.NewScanner(strings.NewReader(contents))
	// Allow reasonably long log lines before falling back to the scanner's hard cap.
	scanner.Buffer(make([]byte, 1024), 1024*1024)

	kva := make([]mr.KeyValue, 0)
	lineNum := 0
	for scanner.Scan() {
		lineNum++
		line := scanner.Text()
		if strings.Contains(line, pattern) {
			kva = append(kva, mr.KeyValue{
				Key:   pattern,
				Value: fmt.Sprintf("%s:%d:%s", filename, lineNum, line),
			})
		}
	}

	if err := scanner.Err(); err != nil {
		log.Fatalf("scan error for %s: %v", filename, err)
	}

	return kva
}

// Reduce collates all matches for the pattern and orders them for stable output.
func Reduce(_ string, values []string) string {
	sort.Strings(values)
	return strings.Join(values, "\n")
}
