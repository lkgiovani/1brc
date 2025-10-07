package main

import (
	"bytes"
	"fmt"
	"os"
	"runtime"
	"sort"
	"sync"
	"time"
)

type cityStats struct {
	min   int
	max   int
	total int
	count int
}

func cleanCityName(b []byte) []byte {
	// Remove BOM UTF-8 (EF BB BF)
	if len(b) >= 3 && b[0] == 0xEF && b[1] == 0xBB && b[2] == 0xBF {
		b = b[3:]
	}
	// Remove outros caracteres invisíveis no início
	for len(b) > 0 && (b[0] < 32 || b[0] == 0xEF || b[0] == 0xBB || b[0] == 0xBF) {
		b = b[1:]
	}
	return bytes.TrimSpace(b)
}

func bytesToNumber(b []byte) (int, bool) {
	if len(b) < 3 {
		return 0, false
	}

	var neg bool
	var idx int

	if b[0] == '-' {
		neg = true
		idx = 1
	}

	if idx+2 >= len(b) {
		return 0, false
	}

	var result int

	if b[idx+1] == '.' {
		result = int(b[idx]-'0')*10 + int(b[idx+2]-'0')
	} else {
		if idx+3 >= len(b) {
			return 0, false
		}
		result = int(b[idx]-'0')*100 + int(b[idx+1]-'0')*10 + int(b[idx+3]-'0')
	}

	if neg {
		return -result, true
	}
	return result, true
}

func main() {
	start := time.Now()

	file, err := os.Open("measurements.txt")
	if err != nil {
		fmt.Println("Error opening file:", err)
		return
	}

	ch := make(chan []byte)
	results := make(chan map[string]cityStats)

	defer file.Close()
	// count := 0

	go func() {
		buf := make([]byte, 1024*1024*50)
		for {

			scanner, err := file.Read(buf)

			if err != nil {
				close(ch)
				return
			}

			newlineIndex := bytes.LastIndexAny(buf[:scanner], "\n")
			validBytes := newlineIndex + 1

			chunk := make([]byte, validBytes)
			copy(chunk, buf[:validBytes])
			ch <- chunk

			if validBytes < scanner {
				offset := int64(validBytes - scanner)
				file.Seek(offset, 1)
			}

			// count++

			// if count > 20 {
			// 	close(ch)
			// 	break
			// }
		}

	}()

	var wg sync.WaitGroup
	numWorkers := runtime.NumCPU() - 1

	for range numWorkers {
		wg.Go(func() {
			for buf := range ch {

				result := processesBuffer(buf)

				results <- result
			}
		})
	}

	go func() {
		wg.Wait()
		close(results)
	}()

	cities := make(map[string]cityStats)

	for res := range results {
		for city, stats := range res {
			if existing, ok := cities[city]; ok {

				if stats.min < existing.min {
					existing.min = stats.min
				}
				if stats.max > existing.max {
					existing.max = stats.max
				}
				existing.total += stats.total
				existing.count += stats.count
				cities[city] = existing
			} else {
				cities[city] = stats
			}
		}
	}

	cityNames := make([]string, 0, len(cities))
	for city := range cities {
		cityNames = append(cityNames, city)
	}
	sort.Strings(cityNames)

	// Print results in the required format
	fmt.Print("{")
	for i, city := range cityNames {
		stats := cities[city]
		min := float64(stats.min) / 10.0
		mean := float64(stats.total) / float64(stats.count) / 10.0
		max := float64(stats.max) / 10.0

		if i > 0 {
			fmt.Print(", ")
		}
		fmt.Printf("%s=%.1f/%.1f/%.1f", city, min, mean, max)
	}
	fmt.Println("}")

	fmt.Printf("\nTotal de cidades processadas: %d\n", len(cities))

	elapsed := time.Since(start)
	fmt.Printf("\nTempo TOTAL de execução: %.2f segundos (%.2f ms)\n", elapsed.Seconds(), float64(elapsed.Milliseconds()))
}

func processesBuffer(buf []byte) map[string]cityStats {
	lines := bytes.Split(buf, []byte("\n"))
	cities := make(map[string]cityStats)

	for _, line := range lines {
		if len(line) == 0 {
			continue
		}
		parts := bytes.Split(line, []byte(";"))
		if len(parts) != 2 {
			continue
		}

		city := string(cleanCityName(parts[0]))
		num, ok := bytesToNumber(parts[1])
		if !ok {
			continue
		}

		if stats, exists := cities[city]; exists {
			if num < int(stats.min) {
				stats.min = num
			}
			if num > int(stats.max) {
				stats.max = num
			}
			stats.total += int(num)
			stats.count++
			cities[city] = stats
		} else {
			cities[city] = cityStats{min: num, max: num, total: num, count: 1}
		}
	}
	return cities
}
