package cmd

import (
	"fmt"
	"os"

	tea "github.com/charmbracelet/bubbletea"
	"github.com/pixperk/chug/internal/logx"
	"github.com/pixperk/chug/internal/ui"
	"github.com/spf13/cobra"
	"go.uber.org/zap"
)

var (
	useInteractive bool
	verboseLogging bool
)

var rootCmd = &cobra.Command{
	Use:   "chug",
	Short: "Chug is a blazing-fast ETL pipeline from Postgres to ClickHouse",
	Long: `Chug streams data from your Postgres tables into ClickHouse
for analytics at ludicrous speed.
Just point, chug, and ask.`,
	PersistentPreRun: func(cmd *cobra.Command, args []string) {
		// Initialize logger with verbosity level
		logx.InitLoggerWithLevel(verboseLogging)

		// If interactive mode is enabled and no subcommand, show interactive UI
		if useInteractive && cmd.Name() == "chug" {
			showInteractiveUI()
		}
	},
	// Show the logo only when 'chug' with no args is run or help is requested
	Run: func(cmd *cobra.Command, args []string) {
		showLogo()
	},
}

// showLogo displays the application logo and header
func showLogo() {
	ui.PrintLogo()
	ui.PrintTitle("Chug: Blazing-Fast ETL Pipeline")
	ui.PrintSubtitle("PostgreSQL to ClickHouse data transfer")
	fmt.Println()
}

func Execute() {
	// Only show logo for help commands
	if len(os.Args) > 1 && (os.Args[1] == "--help" || os.Args[1] == "-h" || os.Args[1] == "help") {
		showLogo()
	}

	if err := rootCmd.Execute(); err != nil {
		logx.StyledLog.Error("Command execution failed", zap.Error(err))
		os.Exit(1)
	}
}

func init() {
	rootCmd.PersistentFlags().BoolVarP(&useInteractive, "interactive", "i", false, "Use interactive TUI mode")
	rootCmd.PersistentFlags().BoolVarP(&verboseLogging, "verbose", "v", false, "Enable verbose logging (shows all operations)")
}

// showInteractiveUI launches the interactive TUI
func showInteractiveUI() {
	p := tea.NewProgram(ui.NewAppModel(), tea.WithAltScreen())
	if _, err := p.Run(); err != nil {
		logx.StyledLog.Error("Error running interactive UI", zap.Error(err))
		os.Exit(1)
	}
	os.Exit(0) // Exit after interactive UI is closed
}
