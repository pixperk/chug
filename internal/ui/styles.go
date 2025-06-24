package ui

import (
	"github.com/charmbracelet/lipgloss"
)

// Theme represents the color scheme for the application
var (
	// Colors
	PrimaryColor   = lipgloss.Color("#4B6BFD")
	SecondaryColor = lipgloss.Color("#FF4D6D")
	AccentColor    = lipgloss.Color("#38FFB6")
	SuccessColor   = lipgloss.Color("#75F591")
	WarningColor   = lipgloss.Color("#FFB238")
	ErrorColor     = lipgloss.Color("#FF4D4D")
	TextColor      = lipgloss.Color("#FFFFFF")
	DimTextColor   = lipgloss.Color("#AAAAAA")
	BgColor        = lipgloss.Color("#222222")

	// Styles
	AppStyle = lipgloss.NewStyle().
			Padding(1, 2).
			Background(BgColor)

	TitleStyle = lipgloss.NewStyle().
			Foreground(PrimaryColor).
			Bold(true).
			Padding(0, 1).
			MarginBottom(1)

	SubtitleStyle = lipgloss.NewStyle().
			Foreground(DimTextColor).
			MarginBottom(1)

	InfoStyle = lipgloss.NewStyle().
			Foreground(TextColor)

	HighlightStyle = lipgloss.NewStyle().
			Foreground(AccentColor).
			Bold(true)

	SuccessStyle = lipgloss.NewStyle().
			Foreground(SuccessColor).
			Bold(true)

	ErrorStyle = lipgloss.NewStyle().
			Foreground(ErrorColor).
			Bold(true)

	WarningStyle = lipgloss.NewStyle().
			Foreground(WarningColor).
			Bold(true)

	ButtonStyle = lipgloss.NewStyle().
			Foreground(TextColor).
			Background(PrimaryColor).
			Padding(0, 3).
			Margin(1, 1).
			Bold(true)

	ActiveButtonStyle = lipgloss.NewStyle().
				Foreground(TextColor).
				Background(SecondaryColor).
				Padding(0, 3).
				Margin(1, 1).
				Bold(true)

	BoxStyle = lipgloss.NewStyle().
			Border(lipgloss.RoundedBorder()).
			BorderForeground(PrimaryColor).
			Padding(1, 2)

	ProgressBarEmpty = "━"
	ProgressBarFull  = "━"

	TableHeaderStyle = lipgloss.NewStyle().
				Foreground(AccentColor).
				Bold(true).
				Padding(0, 1)

	TableCellStyle = lipgloss.NewStyle().
			Foreground(TextColor).
			Padding(0, 1)

	LogoStyle = lipgloss.NewStyle().
			Foreground(PrimaryColor).
			Bold(true).
			Padding(1, 0)

	FooterStyle = lipgloss.NewStyle().
			Foreground(DimTextColor).
			Align(lipgloss.Center).
			Padding(1, 0)
)

// GetProgressBar returns a styled progress bar based on percentage
func GetProgressBar(percent float64, width int) string {
	filled := int(float64(width) * percent)
	empty := width - filled

	filledBar := SuccessStyle.Render(ProgressBarFull)
	emptyBar := ErrorStyle.Render(ProgressBarEmpty)

	bar := ""
	for i := 0; i < filled; i++ {
		bar += filledBar
	}
	for i := 0; i < empty; i++ {
		bar += emptyBar
	}

	return bar
}

// Spinner characters for loading indicators
var Spinner = []string{"⠋", "⠙", "⠹", "⠸", "⠼", "⠴", "⠦", "⠧", "⠇", "⠏"}
