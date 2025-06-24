package ui

import (
	"fmt"
	"strings"

	"github.com/charmbracelet/bubbles/progress"
	tea "github.com/charmbracelet/bubbletea"
	"github.com/charmbracelet/lipgloss"
)

// ProgressModel represents a model for displaying a progress bar with details
type ProgressModel struct {
	progress      progress.Model
	total         int
	current       int
	operationName string
	status        string
	width         int
}

// NewProgressModel creates a new progress bar model
func NewProgressModel(operationName string, total int) ProgressModel {
	p := progress.New(
		progress.WithDefaultGradient(),
		progress.WithWidth(40),
		progress.WithoutPercentage(),
	)

	return ProgressModel{
		progress:      p,
		total:         total,
		current:       0,
		operationName: operationName,
		status:        "Starting...",
		width:         80,
	}
}

// IncrementProgress advances the progress counter
func (m *ProgressModel) IncrementProgress(amount int, status string) tea.Cmd {
	m.current += amount
	if m.current > m.total {
		m.current = m.total
	}

	m.status = status
	return nil
}

// SetProgress sets the progress to a specific value
func (m *ProgressModel) SetProgress(value int, status string) tea.Cmd {
	m.current = value
	if m.current > m.total {
		m.current = m.total
	}

	m.status = status
	return nil
}

// Init initializes the model
func (m ProgressModel) Init() tea.Cmd {
	return nil
}

// Update handles messages and updates the model
func (m ProgressModel) Update(msg tea.Msg) (tea.Model, tea.Cmd) {
	switch msg := msg.(type) {
	case tea.WindowSizeMsg:
		m.width = msg.Width
		m.progress.Width = msg.Width - 20
	}

	progressModel, cmd := m.progress.Update(msg)
	m.progress = progressModel.(progress.Model)

	return m, cmd
}

// View renders the model
func (m ProgressModel) View() string {
	// Calculate percentage
	percent := float64(m.current) / float64(m.total)
	if m.total == 0 {
		percent = 0
	}

	// Progress bar
	pad := strings.Repeat(" ", 2)
	progressBar := m.progress.ViewAs(percent)

	// Operation title
	title := HighlightStyle.Render(m.operationName)

	// Stats
	stats := InfoStyle.Render(fmt.Sprintf("%d/%d", m.current, m.total))

	// Status
	status := InfoStyle.Render(m.status)

	// Combine all elements
	return lipgloss.JoinVertical(
		lipgloss.Left,
		title,
		progressBar,
		pad+stats,
		pad+status,
	)
}
