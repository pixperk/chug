package ui

import (
	"fmt"
	"os"
	"strings"

	"github.com/charmbracelet/lipgloss"
)

// PrintLogo prints the Chug logo to the terminal
func PrintLogo() {
	fmt.Println(LogoStyle.Render(Logo))
}

// PrintTitle prints a styled title
func PrintTitle(title string) {
	fmt.Println(TitleStyle.Render(title))
}

// PrintSubtitle prints a styled subtitle
func PrintSubtitle(subtitle string) {
	fmt.Println(SubtitleStyle.Render(subtitle))
}

// PrintSuccess prints a success message
func PrintSuccess(message string) {
	fmt.Println(SuccessStyle.Render("✓ " + message))
}

// PrintError prints an error message
func PrintError(message string) {
	fmt.Println(ErrorStyle.Render("✗ " + message))
}

// PrintWarning prints a warning message
func PrintWarning(message string) {
	fmt.Println(WarningStyle.Render("! " + message))
}

// PrintInfo prints an info message
func PrintInfo(message string) {
	fmt.Println(InfoStyle.Render(message))
}

// PrintHighlight prints a highlighted message
func PrintHighlight(message string) {
	fmt.Println(HighlightStyle.Render(message))
}

// PrintBox prints content in a styled box
func PrintBox(title string, content string) {
	titleText := HighlightStyle.Render(title)
	contentText := InfoStyle.Render(content)
	boxContent := lipgloss.JoinVertical(lipgloss.Left, titleText, contentText)
	fmt.Println(BoxStyle.Render(boxContent))
}

// ExitWithError prints an error message and exits with code 1
func ExitWithError(message string) {
	PrintError(message)
	os.Exit(1)
}

// DisplayTable prints a styled table with headers and rows
func DisplayTable(headers []string, rows [][]string) {
	// Calculate column widths
	colWidths := make([]int, len(headers))
	for i, header := range headers {
		colWidths[i] = len(header)
	}

	for _, row := range rows {
		for i, cell := range row {
			if i < len(colWidths) && len(cell) > colWidths[i] {
				colWidths[i] = len(cell)
			}
		}
	}

	// Print headers
	headerCells := make([]string, len(headers))
	for i, header := range headers {
		headerCells[i] = TableHeaderStyle.Render(
			lipgloss.PlaceHorizontal(
				colWidths[i]+2, // Add padding
				lipgloss.Left,
				header,
			),
		)
	}

	headerRow := lipgloss.JoinHorizontal(lipgloss.Top, headerCells...)
	fmt.Println(headerRow)

	// Print separator
	separator := make([]string, len(headers))
	for i, width := range colWidths {
		separator[i] = strings.Repeat("─", width+2) // Add padding
	}
	separatorRow := lipgloss.JoinHorizontal(lipgloss.Top, separator...)
	fmt.Println(HighlightStyle.Render(separatorRow))

	// Print rows
	for _, row := range rows {
		rowCells := make([]string, len(row))
		for i, cell := range row {
			if i < len(colWidths) {
				rowCells[i] = TableCellStyle.Render(
					lipgloss.PlaceHorizontal(
						colWidths[i]+2, // Add padding
						lipgloss.Left,
						cell,
					),
				)
			}
		}
		fmt.Println(lipgloss.JoinHorizontal(lipgloss.Top, rowCells...))
	}
}
