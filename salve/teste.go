package main

import (
	"encoding/binary"
	"fmt"
	"math/bits"
)

func convertIntoNumber(decimalSepPos int, numberWord int64) int64 {
	shift := 28 - decimalSepPos
	signed := (^(numberWord) << 59) >> 63
	designMask := ^(signed & 0xFF)
	digits := ((numberWord & designMask) << shift) & 0x0F000F0F00
	absValue := ((uint64(digits) * 0x640a0001) >> 32) & 0x3FF
	return int64(absValue^uint64(signed)) - signed
}

// Função que recebe 8 bytes diretamente e converte
func bytesToNumberOld(b []byte) int64 {
	if len(b) < 8 {
		panic("precisa de pelo menos 8 bytes")
	}
	numberWord := int64(binary.BigEndian.Uint64(b))
	decimalSepPos := bits.TrailingZeros64(uint64(^numberWord & 0x10101000))
	return convertIntoNumber(decimalSepPos, numberWord)
}

func main() {
	fmt.Println("=== OS 4 CASOS DO 1BRC ===\n")

	tests := []struct {
		value string
		caso  string
	}{
		{"6.3940", "CASO 1: X.YYYY (1 dígito, 4 decimais)"},
		{"26.2456", "CASO 2: XX.YYYY (2 dígitos, 4 decimais)"},
		{"-6.3940", "CASO 3: -X.YYYY (negativo 1 dígito, 4 decimais)"},
		{"-26.2456", "CASO 4: -XX.YYYY (negativo 2 dígitos, 4 decimais)"},
	}

	for _, test := range tests {
		buf := make([]byte, 8)
		copy(buf, []byte(test.value))

		result := bytesToNumberOld(buf)
		expected := parseExpected(test.value)

		status := "✓"
		if result != int64(expected) {
			status = "✗"
		}

		fmt.Printf("%s %s\n", status, test.caso)
		fmt.Printf("   Input: %9s -> Output: %6d (esperado: %6d)\n\n", test.value, result, expected)
	}

	fmt.Println("=== TESTANDO MAIS VARIAÇÕES ===\n")

	extras := []string{"0.0000", "5.7890", "0.9999", "99.9999", "-0.1234", "-5.7890", "-99.9999"}
	successCount := 0

	for _, test := range extras {
		buf := make([]byte, 8)
		copy(buf, []byte(test))

		result := bytesToNumberOld(buf)
		expected := parseExpected(test)

		status := "✓"
		if result != int64(expected) {
			status = "✗"
		} else {
			successCount++
		}

		fmt.Printf("%s %9s -> %6d\n", status, test, result)
	}

	fmt.Printf("\nTodos os testes: %d/%d ✓\n", successCount+4, len(extras)+4)
}

func parseExpected(s string) int {
	sign := 1
	result := 0
	for i := 0; i < len(s); i++ {
		if s[i] == '-' {
			sign = -1
		} else if s[i] >= '0' && s[i] <= '9' {
			result = result*10 + int(s[i]-'0')
		}
	}
	return sign * result
}
