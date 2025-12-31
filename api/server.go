package api

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"os"
	"sync"
	"time"

	"github.com/gorilla/websocket"
	"github.com/pixperk/chug/internal/config"
	"github.com/pixperk/chug/internal/db"
	"github.com/pixperk/chug/internal/etl"
	"go.uber.org/zap"
)

type Server struct {
	config    *config.Config
	logger    *zap.Logger
	upgrader  websocket.Upgrader
	jobs      sync.Map // jobID -> *IngestionJob
	broadcast chan ProgressUpdate
	clients   sync.Map // clientID -> *websocket.Conn
}

type IngestionJob struct {
	ID           string              `json:"id"`
	Status       string              `json:"status"` // pending, running, completed, failed
	Tables       []string            `json:"tables"`
	TableConfigs []TableConfigRequest `json:"table_configs,omitempty"` // Store table configurations for UI display
	Results      []etl.TableResult   `json:"results"`
	Progress     []ProgressUpdate    `json:"progress"`
	StartTime    time.Time           `json:"start_time"`
	EndTime      *time.Time          `json:"end_time,omitempty"`
	Error        string              `json:"error,omitempty"`
	mu           sync.RWMutex
}

type ProgressUpdate struct {
	JobID        string    `json:"job_id"`
	Table        string    `json:"table"`
	Event        string    `json:"event"` // started, extracting, inserting, completed, error
	Message      string    `json:"message"`
	RowCount     int64     `json:"row_count,omitempty"`      // Total rows processed for this table
	CurrentRows  int64     `json:"current_rows,omitempty"`   // Current batch/progress
	TotalRows    int64     `json:"total_rows,omitempty"`     // Expected total (from limit)
	Percentage   float64   `json:"percentage,omitempty"`     // Completion percentage
	Phase        string    `json:"phase,omitempty"`          // extracting, inserting, completed
	Duration     string    `json:"duration,omitempty"`
	Timestamp    time.Time `json:"timestamp"`
}

type TableConfigRequest struct {
	Name      string                `json:"name"`
	Limit     *int                  `json:"limit,omitempty"`
	BatchSize *int                  `json:"batch_size,omitempty"`
	Polling   *config.PollingConfig `json:"polling,omitempty"`
}

type IngestRequest struct {
	Tables    []TableConfigRequest `json:"tables"`
	PgURL     string               `json:"pg_url,omitempty"`
	ChURL     string               `json:"ch_url,omitempty"`
	Limit     *int                 `json:"limit,omitempty"`     // Default limit for tables without specific config
	BatchSize *int                 `json:"batch_size,omitempty"` // Default batch size
	Polling   *config.PollingConfig `json:"polling,omitempty"`   // Default polling config
}

type HealthResponse struct {
	Status    string    `json:"status"`
	Timestamp time.Time `json:"timestamp"`
	Version   string    `json:"version"`
}

type JobStatusResponse struct {
	Job *IngestionJob `json:"job"`
}

func NewServer(cfg *config.Config, logger *zap.Logger) *Server {
	return &Server{
		config:    cfg,
		logger:    logger,
		broadcast: make(chan ProgressUpdate, 100),
		upgrader: websocket.Upgrader{
			CheckOrigin: func(r *http.Request) bool {
				return true // Allow all origins (configure properly in production)
			},
		},
	}
}

func (s *Server) Start(addr string) error {
	// Start WebSocket broadcaster
	go s.broadcastUpdates()

	// Setup routes
	http.HandleFunc("/", s.handleWebUI)
	http.HandleFunc("/health", s.handleHealth)
	http.HandleFunc("/api/v1/tables", s.handleListTables)
	http.HandleFunc("/api/v1/tables/columns", s.handleTableColumns)
	http.HandleFunc("/api/v1/ingest", s.handleIngest)
	http.HandleFunc("/api/v1/jobs", s.handleListJobs)
	http.HandleFunc("/api/v1/jobs/", s.handleJobStatus)
	http.HandleFunc("/ws", s.handleWebSocket)

	s.logger.Info("Starting API server", zap.String("addr", addr))
	return http.ListenAndServe(addr, nil)
}

func (s *Server) handleWebUI(w http.ResponseWriter, r *http.Request) {
	distDir := "web/dist"

	// For SPA routing - serve index.html for non-existent files
	if r.URL.Path != "/" {
		filePath := distDir + r.URL.Path
		if _, err := os.Stat(filePath); os.IsNotExist(err) {
			http.ServeFile(w, r, distDir+"/index.html")
			return
		}
	}

	fs := http.FileServer(http.Dir(distDir))
	fs.ServeHTTP(w, r)
}

func (s *Server) handleHealth(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
		return
	}

	response := HealthResponse{
		Status:    "healthy",
		Timestamp: time.Now(),
		Version:   "1.0.0",
	}

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(response)
}

func (s *Server) handleListTables(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
		return
	}

	// Get pg_url from query params or use server default
	pgURL := r.URL.Query().Get("pg_url")
	if pgURL == "" {
		pgURL = s.config.PostgresURL
	}

	if pgURL == "" {
		http.Error(w, "PostgreSQL URL not configured", http.StatusBadRequest)
		return
	}

	// Connect to PostgreSQL
	pgConn, err := db.GetPostgresPool(pgURL)
	if err != nil {
		http.Error(w, fmt.Sprintf("Failed to connect to PostgreSQL: %v", err), http.StatusInternalServerError)
		return
	}

	// Query for all tables in public schema
	ctx := context.Background()
	query := `
		SELECT table_name
		FROM information_schema.tables
		WHERE table_schema = 'public'
		AND table_type = 'BASE TABLE'
		ORDER BY table_name
	`

	rows, err := pgConn.Query(ctx, query)
	if err != nil {
		http.Error(w, fmt.Sprintf("Failed to query tables: %v", err), http.StatusInternalServerError)
		return
	}
	defer rows.Close()

	var tables []string
	for rows.Next() {
		var tableName string
		if err := rows.Scan(&tableName); err != nil {
			continue
		}
		tables = append(tables, tableName)
	}

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(map[string]interface{}{
		"tables": tables,
	})
}

func (s *Server) handleTableColumns(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
		return
	}

	// Get parameters from query
	tableName := r.URL.Query().Get("table")
	if tableName == "" {
		http.Error(w, "table parameter is required", http.StatusBadRequest)
		return
	}

	pgURL := r.URL.Query().Get("pg_url")
	if pgURL == "" {
		pgURL = s.config.PostgresURL
	}

	if pgURL == "" {
		http.Error(w, "PostgreSQL URL not configured", http.StatusBadRequest)
		return
	}

	// Connect to PostgreSQL
	pgConn, err := db.GetPostgresPool(pgURL)
	if err != nil {
		http.Error(w, fmt.Sprintf("Failed to connect to PostgreSQL: %v", err), http.StatusInternalServerError)
		return
	}

	// Query for columns of the specified table
	ctx := context.Background()
	query := `
		SELECT column_name, data_type
		FROM information_schema.columns
		WHERE table_schema = 'public'
		AND table_name = $1
		ORDER BY ordinal_position
	`

	rows, err := pgConn.Query(ctx, query, tableName)
	if err != nil {
		http.Error(w, fmt.Sprintf("Failed to query columns: %v", err), http.StatusInternalServerError)
		return
	}
	defer rows.Close()

	type Column struct {
		Name     string `json:"name"`
		DataType string `json:"data_type"`
	}

	var columns []Column
	for rows.Next() {
		var col Column
		if err := rows.Scan(&col.Name, &col.DataType); err != nil {
			http.Error(w, fmt.Sprintf("Failed to scan column: %v", err), http.StatusInternalServerError)
			return
		}
		columns = append(columns, col)
	}

	if err := rows.Err(); err != nil {
		http.Error(w, fmt.Sprintf("Error iterating columns: %v", err), http.StatusInternalServerError)
		return
	}

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(map[string]interface{}{
		"columns": columns,
	})
}

func (s *Server) handleIngest(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
		return
	}

	var req IngestRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, fmt.Sprintf("Invalid request: %v", err), http.StatusBadRequest)
		return
	}

	// Validate request
	if len(req.Tables) == 0 {
		http.Error(w, "No tables specified", http.StatusBadRequest)
		return
	}

	// Create job
	jobID := fmt.Sprintf("job_%d", time.Now().UnixNano())

	// Extract table names for job tracking
	tableNames := make([]string, len(req.Tables))
	for i, t := range req.Tables {
		tableNames[i] = t.Name
	}

	job := &IngestionJob{
		ID:           jobID,
		Status:       "pending",
		Tables:       tableNames,
		TableConfigs: req.Tables, // Store table configurations for UI display
		Progress:     make([]ProgressUpdate, 0),
		StartTime:    time.Now(),
	}
	s.jobs.Store(jobID, job)

	// Start ingestion in background
	go s.runIngestion(jobID, req)

	// Return job ID immediately
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(map[string]string{
		"job_id": jobID,
		"status": "accepted",
	})
}

func (s *Server) handleListJobs(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
		return
	}

	jobs := make([]*IngestionJob, 0)
	s.jobs.Range(func(key, value interface{}) bool {
		if job, ok := value.(*IngestionJob); ok {
			jobs = append(jobs, job)
		}
		return true
	})

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(map[string]interface{}{
		"jobs": jobs,
	})
}

func (s *Server) handleJobStatus(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
		return
	}

	// Extract job ID from path
	jobID := r.URL.Path[len("/api/v1/jobs/"):]
	if jobID == "" {
		http.Error(w, "Job ID required", http.StatusBadRequest)
		return
	}

	jobValue, ok := s.jobs.Load(jobID)
	if !ok {
		http.Error(w, "Job not found", http.StatusNotFound)
		return
	}

	job := jobValue.(*IngestionJob)
	job.mu.RLock()
	defer job.mu.RUnlock()

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(JobStatusResponse{Job: job})
}

func (s *Server) handleWebSocket(w http.ResponseWriter, r *http.Request) {
	conn, err := s.upgrader.Upgrade(w, r, nil)
	if err != nil {
		s.logger.Error("WebSocket upgrade failed", zap.Error(err))
		return
	}

	clientID := fmt.Sprintf("client_%d", time.Now().UnixNano())
	s.clients.Store(clientID, conn)

	s.logger.Info("WebSocket client connected", zap.String("client_id", clientID))

	// Keep connection alive and handle client disconnect
	go func() {
		defer func() {
			s.clients.Delete(clientID)
			conn.Close()
			s.logger.Info("WebSocket client disconnected", zap.String("client_id", clientID))
		}()

		for {
			if _, _, err := conn.ReadMessage(); err != nil {
				break
			}
		}
	}()
}

func (s *Server) broadcastUpdates() {
	for update := range s.broadcast {
		// Send to all connected WebSocket clients
		s.clients.Range(func(key, value interface{}) bool {
			conn := value.(*websocket.Conn)
			if err := conn.WriteJSON(update); err != nil {
				s.logger.Warn("Failed to send update to client",
					zap.String("client_id", key.(string)),
					zap.Error(err))
			}
			return true
		})
	}
}

func (s *Server) sendUpdate(update ProgressUpdate) {
	// Add to job progress
	if jobValue, ok := s.jobs.Load(update.JobID); ok {
		job := jobValue.(*IngestionJob)
		job.mu.Lock()
		job.Progress = append(job.Progress, update)
		job.mu.Unlock()
	}

	// Broadcast to WebSocket clients
	select {
	case s.broadcast <- update:
	default:
		s.logger.Warn("Broadcast channel full, dropping update")
	}
}

func (s *Server) runIngestion(jobID string, req IngestRequest) {
	jobValue, _ := s.jobs.Load(jobID)
	job := jobValue.(*IngestionJob)

	job.mu.Lock()
	job.Status = "running"
	job.mu.Unlock()

	s.sendUpdate(ProgressUpdate{
		JobID:     jobID,
		Event:     "started",
		Message:   fmt.Sprintf("Starting ingestion for %d tables", len(req.Tables)),
		Timestamp: time.Now(),
	})

	// Build config
	cfg := &config.Config{
		PostgresURL:   s.getOrDefault(req.PgURL, s.config.PostgresURL),
		ClickHouseURL: s.getOrDefault(req.ChURL, s.config.ClickHouseURL),
		Limit:         req.Limit,
		BatchSize:     req.BatchSize,
	}

	if req.Polling != nil {
		cfg.Polling = *req.Polling
	}

	// Create table configs using per-table settings
	for _, tableConfig := range req.Tables {
		// Use table-specific config if provided, otherwise fall back to defaults
		limit := tableConfig.Limit
		if limit == nil {
			limit = req.Limit
		}
		batchSize := tableConfig.BatchSize
		if batchSize == nil {
			batchSize = req.BatchSize
		}
		polling := tableConfig.Polling
		if polling == nil {
			polling = req.Polling
		}

		cfg.Tables = append(cfg.Tables, config.TableConfig{
			Name:      tableConfig.Name,
			Limit:     limit,
			BatchSize: batchSize,
			Polling:   polling,
		})
	}

	// Get PostgreSQL connection
	ctx := context.Background()
	pgConn, err := db.GetPostgresPool(cfg.PostgresURL)
	if err != nil {
		s.handleJobError(job, fmt.Sprintf("Failed to connect to PostgreSQL: %v", err))
		return
	}

	// Create ingestion options with progress callbacks
	opts := &etl.IngestOptions{
		OnTableStart: func(tableName string) {
			s.sendUpdate(ProgressUpdate{
				JobID:     jobID,
				Table:     tableName,
				Event:     "started",
				Message:   "Starting extraction",
				Timestamp: time.Now(),
			})
		},
		OnExtractStart: func(tableName string, columnCount int) {
			s.sendUpdate(ProgressUpdate{
				JobID:     jobID,
				Table:     tableName,
				Event:     "extracting",
				Message:   fmt.Sprintf("Extracting data (%d columns)", columnCount),
				Timestamp: time.Now(),
			})
		},
		OnInsertStart: func(tableName string) {
			s.sendUpdate(ProgressUpdate{
				JobID:     jobID,
				Table:     tableName,
				Event:     "inserting",
				Message:   "Inserting data into ClickHouse",
				Timestamp: time.Now(),
			})
		},
		OnProgress: func(tableName string, currentRows int64, totalRows int64, percentage float64, phase string) {
			s.sendUpdate(ProgressUpdate{
				JobID:       jobID,
				Table:       tableName,
				Event:       phase,
				Message:     fmt.Sprintf("Processing: %d rows", currentRows),
				CurrentRows: currentRows,
				TotalRows:   totalRows,
				Percentage:  percentage,
				Phase:       phase,
				Timestamp:   time.Now(),
			})
		},
		OnTableComplete: func(tableName string, rowCount int64, duration time.Duration) {
			s.sendUpdate(ProgressUpdate{
				JobID:       jobID,
				Table:       tableName,
				Event:       "completed",
				Message:     "Ingestion completed",
				RowCount:    rowCount,
				CurrentRows: rowCount,
				Percentage:  100,
				Phase:       "completed",
				Duration:    duration.String(),
				Timestamp:   time.Now(),
			})
		},
		OnTableError: func(tableName string, err error) {
			s.sendUpdate(ProgressUpdate{
				JobID:     jobID,
				Table:     tableName,
				Event:     "error",
				Message:   err.Error(),
				Timestamp: time.Now(),
			})
		},
	}

	// Run ingestion
	results := etl.IngestMultipleTables(ctx, cfg, pgConn, opts)

	// Update job with results
	job.mu.Lock()
	job.Results = results
	endTime := time.Now()
	job.EndTime = &endTime

	// Check if any table failed
	allSuccess := true
	for _, result := range results {
		if !result.Success {
			allSuccess = false
			break
		}
	}

	if allSuccess {
		job.Status = "completed"
	} else {
		job.Status = "failed"
	}
	job.mu.Unlock()

	s.sendUpdate(ProgressUpdate{
		JobID:     jobID,
		Event:     "job_completed",
		Message:   fmt.Sprintf("Job completed with status: %s", job.Status),
		Timestamp: time.Now(),
	})
}

func (s *Server) handleJobError(job *IngestionJob, errMsg string) {
	job.mu.Lock()
	job.Status = "failed"
	job.Error = errMsg
	endTime := time.Now()
	job.EndTime = &endTime
	job.mu.Unlock()

	s.sendUpdate(ProgressUpdate{
		JobID:     job.ID,
		Event:     "error",
		Message:   errMsg,
		Timestamp: time.Now(),
	})
}

func (s *Server) getOrDefault(value, defaultValue string) string {
	if value != "" {
		return value
	}
	return defaultValue
}
