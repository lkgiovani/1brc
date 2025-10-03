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
	// totalReadTime := time.Duration(0)

	go func() {
		buf := make([]byte, 1024*1024*50)
		for {
			// readStart := time.Now()
			scanner, err := file.Read(buf)
			// readTime := time.Since(readStart)
			// totalReadTime += readTime

			if err != nil {
				// fmt.Printf("\nFinalizou leitura após %d chunks\n", count)
				// fmt.Printf("Tempo total de leitura: %v\n", totalReadTime)
				// fmt.Printf("Tempo médio por chunk: %v\n", totalReadTime/time.Duration(count))
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
			// fmt.Printf("Chunk %3d: leu %6d bytes em %8v\n", count, scanner, readTime)

			// if count > 100 {
			// 	fmt.Printf("\nFinalizou leitura após %d chunks\n", count)
			// 	fmt.Printf("Tempo total de leitura: %v\n", totalReadTime)
			// 	fmt.Printf("Tempo médio por chunk: %v\n", totalReadTime/time.Duration(count))
			// 	close(ch)
			// 	break
			// }
		}

	}()

	var wg sync.WaitGroup
	numWorkers := runtime.NumCPU() - 1
	processCount := 0
	totalProcessTime := time.Duration(0)
	var mu sync.Mutex

	for range numWorkers {
		wg.Go(func() {
			for buf := range ch {
				processStart := time.Now()
				result := processesBuffer(buf)
				processTime := time.Since(processStart)

				mu.Lock()
				processCount++
				totalProcessTime += processTime
				fmt.Printf("Process %3d: processou %6d bytes em %8v\n", processCount, len(buf), processTime)
				mu.Unlock()

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

	fmt.Printf("\n=== RESUMO ===\n")
	fmt.Printf("Total de cidades processadas: %d\n", len(cities))
	fmt.Printf("Total de chunks processados: %d\n", processCount)
	fmt.Printf("Tempo total de processamento: %v\n", totalProcessTime)
	if processCount > 0 {
		fmt.Printf("Tempo médio por chunk: %v\n", totalProcessTime/time.Duration(processCount))
	}

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

		city := string(parts[0])
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
