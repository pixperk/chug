package ui

import (
	"fmt"
	"time"

	"github.com/charmbracelet/bubbles/spinner"
	tea "github.com/charmbracelet/bubbletea"
	"github.com/charmbracelet/lipgloss"
)

// Logo represents the ASCII art logo for Chug
const Logo = `
   _____ _    _ _    _  _____
  / ____| |  | | |  | |/ ____|
 | |    | |__| | |  | | |  __
 | |    |  __  | |  | | | |_ |
 | |____| |  | | |__| | |__| |
  \_____|_|  |_|\____/ \_____|
`

// AppModel is the main model for the TUI application
type AppModel struct {
	Width       int
	Height      int
	Spinner     spinner.Model
	StatusMsg   string
	IsLoading   bool
	CurrentView string
	Error       error
}

// NewAppModel creates a new AppModel with default values
func NewAppModel() AppModel {
	s := spinner.New()
	s.Spinner = spinner.Dot
	s.Style = lipgloss.NewStyle().Foreground(PrimaryColor)

	return AppModel{
		Spinner:     s,
		StatusMsg:   "Ready",
		IsLoading:   false,
		CurrentView: "main",
	}
}

// Init initializes the model
func (m AppModel) Init() tea.Cmd {
	return m.Spinner.Tick
}

// Update handles user input and updates the model
func (m AppModel) Update(msg tea.Msg) (tea.Model, tea.Cmd) {
	switch msg := msg.(type) {
	case tea.KeyMsg:
		switch msg.String() {
		case "ctrl+c", "q":
			return m, tea.Quit
		}
	case tea.WindowSizeMsg:
		m.Width = msg.Width
		m.Height = msg.Height
	}

	var cmd tea.Cmd
	m.Spinner, cmd = m.Spinner.Update(msg)
	return m, cmd
}

// View renders the current state of the model
func (m AppModel) View() string {
	if m.Width == 0 {
		return "Loading..."
	}

	var view string
	switch m.CurrentView {
	case "main":
		view = m.renderMainView()
	default:
		view = m.renderMainView()
	}

	// Footer
	footer := FooterStyle.Render("Press q to quit")

	// Combine all elements
	return lipgloss.JoinVertical(lipgloss.Center, view, footer)
}

// renderMainView renders the main screen
func (m AppModel) renderMainView() string {
	// Logo
	logo := LogoStyle.Render(Logo)

	// Title
	title := TitleStyle.Render("Chug: Blazing-Fast ETL Pipeline")
	subtitle := SubtitleStyle.Render("PostgreSQL to ClickHouse data transfer")

	// Status bar
	var status string
	if m.IsLoading {
		status = fmt.Sprintf("%s %s", m.Spinner.View(), m.StatusMsg)
	} else if m.Error != nil {
		status = ErrorStyle.Render(fmt.Sprintf("Error: %s", m.Error.Error()))
	} else {
		status = InfoStyle.Render(m.StatusMsg)
	}

	// Help text
	helpText := BoxStyle.Render(lipgloss.JoinVertical(
		lipgloss.Left,
		HighlightStyle.Render("Available Commands:"),
		InfoStyle.Render("connect   - Test database connections"),
		InfoStyle.Render("ingest    - Transfer data from PostgreSQL to ClickHouse"),
		InfoStyle.Render("sample-config - Generate a sample configuration file"),
	))

	// Stats box (placeholder)
	currentTime := time.Now().Format("15:04:05")
	stats := BoxStyle.Render(lipgloss.JoinVertical(
		lipgloss.Left,
		HighlightStyle.Render("Stats:"),
		InfoStyle.Render(fmt.Sprintf("Current time: %s", currentTime)),
		InfoStyle.Render("Terminal size: "+fmt.Sprintf("%d×%d", m.Width, m.Height)),
	))

	// Combine boxes in a horizontal layout
	boxes := lipgloss.JoinHorizontal(lipgloss.Top, helpText, stats)

	// Join everything vertically
	return lipgloss.JoinVertical(
		lipgloss.Center,
		logo,
		title,
		subtitle,
		"",
		boxes,
		"",
		status,
	)
}
