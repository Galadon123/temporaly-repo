# Multi-Framework Support for TensorCode
## Complete Architecture: Queue-Based Execution with NumPy and PyTorch

**Inspired by**: deep-ml.com framework selection feature
**Status**: Production-Ready Architecture Design
**Frameworks**: NumPy (educational) + PyTorch (production)
**Architecture**: Asynchronous queue-based execution with Cloudflare Workers
**Version**: 3.0

---

## Table of Contents

1. [System Overview](#1-system-overview)
2. [Architecture Components](#2-architecture-components)
3. [User Flow](#3-user-flow)
4. [Frontend Implementation](#4-frontend-implementation)
5. [Problem Submission Service](#5-problem-submission-service)
6. [Queue System](#6-queue-system)
7. [Job Consuming Workers](#7-job-consuming-workers)
8. [Backend Executors](#8-backend-executors)
9. [Database & Storage](#9-database--storage)
10. [Real-time Updates](#10-real-time-updates)
11. [Implementation Guide](#11-implementation-guide)

---

## 1. System Overview

### Complete Architecture Diagram

```mermaid
graph TB
    subgraph "User Layer"
        U[User]
    end

    subgraph "Frontend - Next.js"
        FW[Framework Selector<br/>NumPy | PyTorch]
        CE[Code Editor<br/>Monaco]
        WS[WebSocket Client<br/>Real-time Updates]
    end

    subgraph "Submission Flow"
        PSS[Problem Submission Service<br/>Node.js/Express]
        SDB[(Submission DB<br/>PostgreSQL)]
    end

    subgraph "Queue System"
        PEQ[Problem Execution Queue<br/>Bull/Redis]
        Q1[NumPy Jobs]
        Q2[PyTorch Jobs]
    end

    subgraph "Cloudflare Workers"
        JCW[Job Consuming Worker<br/>Cloudflare Worker]
    end

    subgraph "Backend Executors"
        NPY[NumPy Backend<br/>Python FastAPI<br/>CPU Only]
        PYT[PyTorch Backend<br/>Python FastAPI<br/>GPU Optional]
        TEST[Test Runner<br/>Execute & Compare]
    end

    subgraph "Results Storage"
        RDB[(Results DB<br/>Verdict: ACCEPTED,<br/>WRONG_ANSWER, TLE, etc.)]
    end

    %% User Flow
    U -->|1. Select Framework| FW
    FW -->|2. Load starter_code| CE
    CE -->|3. Submit Code| PSS

    %% Submission Processing
    PSS -->|4. Create Submission| SDB
    SDB -->|5. Return submission_id| PSS
    PSS -->|6. Enqueue Job| PEQ
    PSS -->|7. Return submission_id| CE

    %% Queue Distribution
    PEQ -->|Framework: numpy| Q1
    PEQ -->|Framework: pytorch| Q2

    %% Worker Processing
    Q1 -->|8. Pull Job| JCW
    Q2 -->|8. Pull Job| JCW

    %% Backend Execution
    JCW -->|9. Forward Request<br/>numpy| NPY
    JCW -->|9. Forward Request<br/>pytorch| PYT

    %% Test Execution
    NPY -->|10. Pull test_cases.json| TEST
    PYT -->|10. Pull test_cases.json| TEST
    TEST -->|11. Execute & Compare| TEST

    %% Results Storage
    TEST -->|12. Store Results<br/>ACCEPTED/WRONG_ANSWER/TLE| RDB
    RDB -->|13. Update Status| SDB

    %% Real-time Updates
    CE -->|14. WebSocket Subscribe| WS
    SDB -.->|15. Notify| WS
    WS -.->|16. Push Update| CE
    CE -->|17. Display Results| U

    %% Styling
    style U fill:#FFE082
    style FW fill:#2196F3,color:#fff
    style PSS fill:#FFC107
    style PEQ fill:#E91E63,color:#fff
    style JCW fill:#00BCD4,color:#fff
    style NPY fill:#4CAF50,color:#fff
    style PYT fill:#FF9800,color:#fff
    style TEST fill:#9C27B0,color:#fff
    style SDB fill:#F44336,color:#fff
    style RDB fill:#F44336,color:#fff
```

### Key Features

1. ✅ **Asynchronous Execution** - No blocking operations
2. ✅ **Queue-Based** - Scalable job processing
3. ✅ **Real-time Updates** - WebSocket for live status
4. ✅ **Framework Selection** - NumPy or PyTorch
5. ✅ **Cloudflare Workers** - Serverless job consumers
6. ✅ **Verdict System** - ACCEPTED, WRONG_ANSWER, TLE, etc.

---

## 2. Architecture Components

### 2.1 Component Overview

| Component | Technology | Purpose | Scalability |
|-----------|-----------|---------|-------------|
| **Frontend** | Next.js + Monaco | UI, Framework Selection | CDN + Edge |
| **Submission Service** | Node.js + Express | API, Job Creation | Horizontal |
| **Queue** | Bull + Redis | Job Distribution | Vertical |
| **Workers** | Cloudflare Workers | Job Processing | Serverless |
| **NumPy Backend** | Python + FastAPI | CPU Execution | Horizontal |
| **PyTorch Backend** | Python + FastAPI | GPU Execution | Limited GPU |
| **Database** | PostgreSQL | Submissions & Results | Replication |
| **WebSocket** | Socket.io | Real-time Updates | Horizontal |

### 2.2 Data Flow

```
User Submits Code
    ↓
Immediate Response (submission_id)
    ↓
Job Queued (async)
    ↓
Worker Picks Job
    ↓
Backend Executes Tests
    ↓
Results Stored
    ↓
WebSocket Notifies User
    ↓
UI Updates with Results
```

---

## 3. User Flow

### 3.1 Complete User Journey

```mermaid
sequenceDiagram
    actor User
    participant FE as Frontend
    participant PSS as Submission Service
    participant DB as Submission DB
    participant Q as Redis Queue
    participant W as Cloudflare Worker
    participant BE as Backend Executor
    participant RDB as Results DB
    participant WS as WebSocket

    User->>FE: 1. Select Framework (NumPy/PyTorch)
    FE->>FE: 2. Load framework starter_code

    User->>FE: 3. Write solution code
    User->>FE: 4. Click Submit

    FE->>PSS: 5. POST /api/submissions<br/>{code, framework, problemName}
    PSS->>DB: 6. INSERT submission<br/>{status: 'QUEUED'}
    DB-->>PSS: 7. Return submission_id

    PSS->>Q: 8. Enqueue job<br/>{submission_id, framework}
    Q-->>PSS: 9. Job queued

    PSS-->>FE: 10. {submission_id, status: 'QUEUED'}
    FE->>WS: 11. WebSocket subscribe<br/>submission_id

    FE-->>User: 12. Show "Executing..."

    W->>Q: 13. Pull job from queue
    Q-->>W: 14. Job details

    W->>DB: 15. UPDATE status='RUNNING'
    DB-.->WS: 16. Notify status change
    WS-.->FE: 17. Push "RUNNING"
    FE-->>User: 18. Update status UI

    W->>BE: 19. POST /execute<br/>{code, tests, framework}
    BE->>BE: 20. Execute tests<br/>Compare outputs
    BE-->>W: 21. {verdict, results}

    W->>RDB: 22. Store results<br/>{verdict, tests_passed, details}
    W->>DB: 23. UPDATE status='COMPLETED'

    RDB-.->WS: 24. Notify completion
    WS-.->FE: 25. Push results
    FE-->>User: 26. Display verdict & details
```

### 3.2 Status States

```
QUEUED → RUNNING → COMPLETED
   ↓        ↓          ↓
  [1]      [2]    [3-Success]
                  [3-Failed]
                  [3-TLE]
                  [3-Error]
```

| Status | Description | Duration |
|--------|-------------|----------|
| **QUEUED** | Job waiting in queue | 1-10s |
| **RUNNING** | Worker processing job | 5-30s |
| **COMPLETED** | Execution finished | Final |

### 3.3 Verdict Types

| Verdict | Code | Description |
|---------|------|-------------|
| **ACCEPTED** | AC | All tests passed ✅ |
| **WRONG_ANSWER** | WA | Output doesn't match ❌ |
| **TIME_LIMIT_EXCEEDED** | TLE | Execution timeout ⏱️ |
| **RUNTIME_ERROR** | RE | Code crashed 💥 |
| **COMPILATION_ERROR** | CE | Syntax error 🔧 |

---

## 4. Frontend Implementation

### 4.1 Framework Selector Component

```tsx
// components/FrameworkSelector.tsx
import { useState } from 'react';
import { Button } from '@/components/ui/button';
import { Badge } from '@/components/ui/badge';

interface Framework {
  name: 'numpy' | 'pytorch';
  difficulty: string;
  description: string;
  default?: boolean;
}

export function FrameworkSelector({
  frameworks,
  onSelect
}: {
  frameworks: Framework[];
  onSelect: (name: string) => void;
}) {
  const [selected, setSelected] = useState<string>(
    frameworks.find(f => f.default)?.name || 'numpy'
  );

  const handleSelect = (name: string) => {
    setSelected(name);
    onSelect(name);
  };

  return (
    <div className="framework-selector">
      <label className="text-sm font-medium">Choose Framework:</label>

      <div className="flex gap-3 mt-2">
        {frameworks.map(fw => (
          <Button
            key={fw.name}
            variant={selected === fw.name ? 'default' : 'outline'}
            onClick={() => handleSelect(fw.name)}
            className="flex-1"
          >
            <span className="capitalize">{fw.name}</span>
            <Badge variant="outline" className="ml-2">
              {fw.difficulty}
            </Badge>
          </Button>
        ))}
      </div>

      <p className="text-sm text-muted-foreground mt-2">
        {frameworks.find(f => f.name === selected)?.description}
      </p>
    </div>
  );
}
```

### 4.2 Code Submission with WebSocket

```tsx
// hooks/useSubmission.ts
import { useState, useEffect } from 'react';
import io from 'socket.io-client';

interface SubmissionStatus {
  status: 'QUEUED' | 'RUNNING' | 'COMPLETED';
  verdict?: 'ACCEPTED' | 'WRONG_ANSWER' | 'TLE' | 'RE';
  tests_passed?: number;
  tests_failed?: number;
  results?: TestResult[];
}

export function useSubmission() {
  const [status, setStatus] = useState<SubmissionStatus | null>(null);
  const [socket, setSocket] = useState<any>(null);

  useEffect(() => {
    // Initialize WebSocket connection
    const ws = io(process.env.NEXT_PUBLIC_WS_URL);
    setSocket(ws);

    return () => {
      ws.disconnect();
    };
  }, []);

  const submit = async (code: string, framework: string, problemName: string) => {
    // Submit code
    const response = await fetch('/api/submissions', {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ code, framework, problemName })
    });

    const { submission_id } = await response.json();

    // Subscribe to updates
    socket?.emit('subscribe', submission_id);

    // Listen for status updates
    socket?.on(`submission:${submission_id}`, (data: SubmissionStatus) => {
      setStatus(data);
    });

    return submission_id;
  };

  return { submit, status };
}
```

### 4.3 Results Display

```tsx
// components/ResultsPanel.tsx
export function ResultsPanel({ status }: { status: SubmissionStatus }) {
  if (!status) return null;

  return (
    <div className="results-panel">
      {/* Status Badge */}
      <StatusBadge status={status.status} />

      {/* Verdict */}
      {status.status === 'COMPLETED' && (
        <VerdictDisplay
          verdict={status.verdict}
          passed={status.tests_passed}
          failed={status.tests_failed}
        />
      )}

      {/* Test Results */}
      {status.results && (
        <div className="test-results">
          {status.results.map((result, idx) => (
            <TestCaseResult key={idx} result={result} />
          ))}
        </div>
      )}
    </div>
  );
}

function VerdictDisplay({ verdict, passed, failed }) {
  const verdictConfig = {
    'ACCEPTED': { color: 'green', icon: '✅', text: 'Accepted' },
    'WRONG_ANSWER': { color: 'red', icon: '❌', text: 'Wrong Answer' },
    'TLE': { color: 'orange', icon: '⏱️', text: 'Time Limit Exceeded' },
    'RE': { color: 'red', icon: '💥', text: 'Runtime Error' }
  };

  const config = verdictConfig[verdict];

  return (
    <div className={`verdict verdict-${config.color}`}>
      <span className="text-2xl">{config.icon}</span>
      <h3>{config.text}</h3>
      <p>{passed}/{passed + failed} tests passed</p>
    </div>
  );
}
```

---

## 5. Problem Submission Service

### 5.1 Service Architecture

```
Problem Submission Service (Node.js + Express)
├── Controllers
│   └── submissionController.js
├── Services
│   ├── problemService.js (GitHub integration)
│   ├── queueService.js (Redis queue)
│   └── validationService.js
├── Models
│   └── Submission.js (PostgreSQL)
└── Routes
    └── submissions.js
```

### 5.2 Submission Controller

```javascript
// src/controllers/submissionController.js

const { v4: uuidv4 } = require('uuid');
const Submission = require('../models/Submission');
const queueService = require('../services/queueService');
const problemService = require('../services/problemService');
const { serverLogger } = require('../utils/logger');

class SubmissionController {
  /**
   * Submit code for execution
   * POST /api/submissions
   */
  async submit(req, res) {
    try {
      const { code, framework, problemName, userId } = req.body;

      // Validate input
      if (!code || !framework || !problemName) {
        return res.status(400).json({
          error: 'Missing required fields: code, framework, problemName'
        });
      }

      // Validate framework
      if (!['numpy', 'pytorch'].includes(framework)) {
        return res.status(400).json({
          error: 'Invalid framework. Must be numpy or pytorch'
        });
      }

      // Load problem metadata
      const problem = await problemService.getProblem(problemName);
      if (!problem) {
        return res.status(404).json({ error: 'Problem not found' });
      }

      // Check if framework is enabled for this problem
      if (!problem.frameworks?.[framework]?.enabled) {
        return res.status(400).json({
          error: `Framework ${framework} not enabled for this problem`
        });
      }

      // Create submission in database
      const submission_id = uuidv4();
      const submission = await Submission.create({
        id: submission_id,
        user_id: userId,
        problem_name: problemName,
        code: code,
        framework: framework,
        status: 'QUEUED',
        created_at: new Date()
      });

      serverLogger.info('Submission created', {
        submission_id,
        problemName,
        framework
      });

      // Enqueue job for processing
      await queueService.enqueueJob({
        submission_id,
        problem_name: problemName,
        code,
        framework,
        user_id: userId
      });

      serverLogger.info('Job enqueued', { submission_id });

      // Return immediate response
      return res.status(202).json({
        submission_id,
        status: 'QUEUED',
        message: 'Submission queued for execution'
      });

    } catch (error) {
      serverLogger.error('Submission error', { error: error.message });
      return res.status(500).json({
        error: 'Failed to submit code',
        details: error.message
      });
    }
  }

  /**
   * Get submission status
   * GET /api/submissions/:id
   */
  async getStatus(req, res) {
    try {
      const { id } = req.params;

      const submission = await Submission.findById(id);
      if (!submission) {
        return res.status(404).json({ error: 'Submission not found' });
      }

      return res.json({
        submission_id: submission.id,
        status: submission.status,
        verdict: submission.verdict,
        tests_passed: submission.tests_passed,
        tests_failed: submission.tests_failed,
        execution_time_ms: submission.execution_time_ms,
        created_at: submission.created_at,
        completed_at: submission.completed_at
      });

    } catch (error) {
      serverLogger.error('Get status error', { error: error.message });
      return res.status(500).json({ error: 'Failed to get status' });
    }
  }

  /**
   * Get submission results
   * GET /api/submissions/:id/results
   */
  async getResults(req, res) {
    try {
      const { id } = req.params;

      const submission = await Submission.findByIdWithResults(id);
      if (!submission) {
        return res.status(404).json({ error: 'Submission not found' });
      }

      if (submission.status !== 'COMPLETED') {
        return res.status(202).json({
          message: 'Submission still processing',
          status: submission.status
        });
      }

      return res.json({
        submission_id: submission.id,
        verdict: submission.verdict,
        tests_passed: submission.tests_passed,
        tests_failed: submission.tests_failed,
        execution_time_ms: submission.execution_time_ms,
        results: submission.results
      });

    } catch (error) {
      serverLogger.error('Get results error', { error: error.message });
      return res.status(500).json({ error: 'Failed to get results' });
    }
  }
}

module.exports = new SubmissionController();
```

### 5.3 Submission Model (PostgreSQL)

```javascript
// src/models/Submission.js

const { Pool } = require('pg');
const pool = new Pool({
  connectionString: process.env.DATABASE_URL
});

class Submission {
  /**
   * Create new submission
   */
  static async create(data) {
    const query = `
      INSERT INTO submissions (
        id, user_id, problem_name, code, framework,
        status, created_at
      ) VALUES ($1, $2, $3, $4, $5, $6, $7)
      RETURNING *
    `;

    const values = [
      data.id,
      data.user_id,
      data.problem_name,
      data.code,
      data.framework,
      data.status,
      data.created_at
    ];

    const result = await pool.query(query, values);
    return result.rows[0];
  }

  /**
   * Find submission by ID
   */
  static async findById(id) {
    const query = 'SELECT * FROM submissions WHERE id = $1';
    const result = await pool.query(query, [id]);
    return result.rows[0];
  }

  /**
   * Update submission status
   */
  static async updateStatus(id, status, additionalData = {}) {
    const fields = ['status = $2', 'updated_at = NOW()'];
    const values = [id, status];
    let paramIndex = 3;

    // Add optional fields
    if (additionalData.verdict) {
      fields.push(`verdict = $${paramIndex}`);
      values.push(additionalData.verdict);
      paramIndex++;
    }

    if (additionalData.tests_passed !== undefined) {
      fields.push(`tests_passed = $${paramIndex}`);
      values.push(additionalData.tests_passed);
      paramIndex++;
    }

    if (additionalData.tests_failed !== undefined) {
      fields.push(`tests_failed = $${paramIndex}`);
      values.push(additionalData.tests_failed);
      paramIndex++;
    }

    if (additionalData.execution_time_ms !== undefined) {
      fields.push(`execution_time_ms = $${paramIndex}`);
      values.push(additionalData.execution_time_ms);
      paramIndex++;
    }

    if (additionalData.results) {
      fields.push(`results = $${paramIndex}`);
      values.push(JSON.stringify(additionalData.results));
      paramIndex++;
    }

    if (status === 'COMPLETED') {
      fields.push('completed_at = NOW()');
    }

    const query = `
      UPDATE submissions
      SET ${fields.join(', ')}
      WHERE id = $1
      RETURNING *
    `;

    const result = await pool.query(query, values);
    return result.rows[0];
  }

  /**
   * Find submission with results
   */
  static async findByIdWithResults(id) {
    const query = `
      SELECT
        s.*,
        r.results
      FROM submissions s
      LEFT JOIN results r ON s.id = r.submission_id
      WHERE s.id = $1
    `;

    const result = await pool.query(query, [id]);
    return result.rows[0];
  }
}

module.exports = Submission;
```

### 5.4 Database Schema

```sql
-- Submissions table
CREATE TABLE submissions (
    id UUID PRIMARY KEY,
    user_id UUID REFERENCES users(id),
    problem_name VARCHAR(255) NOT NULL,
    code TEXT NOT NULL,
    framework VARCHAR(20) NOT NULL CHECK (framework IN ('numpy', 'pytorch')),
    status VARCHAR(20) NOT NULL DEFAULT 'QUEUED'
        CHECK (status IN ('QUEUED', 'RUNNING', 'COMPLETED', 'FAILED')),
    verdict VARCHAR(20) CHECK (verdict IN ('ACCEPTED', 'WRONG_ANSWER', 'TLE', 'RE', 'CE')),
    tests_passed INTEGER,
    tests_failed INTEGER,
    execution_time_ms INTEGER,
    created_at TIMESTAMP DEFAULT NOW(),
    updated_at TIMESTAMP DEFAULT NOW(),
    completed_at TIMESTAMP
);

-- Results table (detailed test results)
CREATE TABLE results (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    submission_id UUID REFERENCES submissions(id) ON DELETE CASCADE,
    results JSONB NOT NULL,
    created_at TIMESTAMP DEFAULT NOW()
);

-- Indexes
CREATE INDEX idx_submissions_user_id ON submissions(user_id);
CREATE INDEX idx_submissions_status ON submissions(status);
CREATE INDEX idx_submissions_created_at ON submissions(created_at DESC);
CREATE INDEX idx_results_submission_id ON results(submission_id);
```

---

## 6. Queue System

### 6.1 Queue Architecture

```
Redis Queue (Bull)
├── problem-exec-queue
│   ├── numpy-jobs (priority: 1)
│   └── pytorch-jobs (priority: 2)
├── Configuration
│   ├── Max concurrent: 10
│   ├── Timeout: 60s
│   └── Retry: 3 attempts
└── Dead Letter Queue (DLQ)
```

### 6.2 Queue Service Implementation

```javascript
// src/services/queueService.js

const Bull = require('bull');
const { serverLogger } = require('../utils/logger');

class QueueService {
  constructor() {
    // Initialize Redis queue
    this.queue = new Bull('problem-execution-queue', {
      redis: {
        host: process.env.REDIS_HOST || 'localhost',
        port: process.env.REDIS_PORT || 6379,
        password: process.env.REDIS_PASSWORD
      },
      defaultJobOptions: {
        attempts: 3,
        backoff: {
          type: 'exponential',
          delay: 2000
        },
        removeOnComplete: 100, // Keep last 100 completed jobs
        removeOnFail: 500       // Keep last 500 failed jobs
      }
    });

    // Configure queue limits
    this.queue.setMaxListeners(100);

    serverLogger.info('Queue service initialized', {
      redis: `${process.env.REDIS_HOST}:${process.env.REDIS_PORT}`
    });

    // Setup event listeners
    this.setupEventListeners();
  }

  /**
   * Enqueue a job for execution
   */
  async enqueueJob(jobData) {
    const { submission_id, framework, problem_name, code, user_id } = jobData;

    // Set priority based on framework
    // PyTorch jobs get lower priority (higher value) due to GPU constraints
    const priority = framework === 'pytorch' ? 2 : 1;

    const job = await this.queue.add(
      {
        submission_id,
        framework,
        problem_name,
        code,
        user_id,
        enqueued_at: new Date().toISOString()
      },
      {
        priority,
        jobId: submission_id, // Use submission_id as job ID
        timeout: 60000 // 60 seconds
      }
    );

    serverLogger.info('Job enqueued', {
      job_id: job.id,
      framework,
      priority
    });

    return job;
  }

  /**
   * Get job status
   */
  async getJobStatus(submission_id) {
    const job = await this.queue.getJob(submission_id);

    if (!job) {
      return null;
    }

    const state = await job.getState();
    const progress = job.progress();

    return {
      id: job.id,
      state,
      progress,
      attempts: job.attemptsMade,
      data: job.data
    };
  }

  /**
   * Setup event listeners for monitoring
   */
  setupEventListeners() {
    // Job completed
    this.queue.on('completed', (job, result) => {
      serverLogger.info('Job completed', {
        job_id: job.id,
        framework: job.data.framework,
        duration: Date.now() - new Date(job.data.enqueued_at).getTime()
      });
    });

    // Job failed
    this.queue.on('failed', (job, err) => {
      serverLogger.error('Job failed', {
        job_id: job.id,
        framework: job.data.framework,
        error: err.message,
        attempts: job.attemptsMade
      });
    });

    // Job stalled
    this.queue.on('stalled', (job) => {
      serverLogger.warn('Job stalled', {
        job_id: job.id,
        framework: job.data.framework
      });
    });

    // Job active
    this.queue.on('active', (job) => {
      serverLogger.info('Job started', {
        job_id: job.id,
        framework: job.data.framework
      });
    });
  }

  /**
   * Get queue statistics
   */
  async getStats() {
    const counts = await this.queue.getJobCounts();
    return {
      waiting: counts.waiting,
      active: counts.active,
      completed: counts.completed,
      failed: counts.failed,
      delayed: counts.delayed
    };
  }

  /**
   * Clear completed jobs
   */
  async clearCompleted() {
    await this.queue.clean(24 * 60 * 60 * 1000); // Clear jobs older than 24 hours
    serverLogger.info('Cleared completed jobs');
  }
}

module.exports = new QueueService();
```

### 6.3 Queue Monitoring Dashboard

```javascript
// Optional: Queue monitoring endpoint
// GET /api/queue/stats

app.get('/api/queue/stats', async (req, res) => {
  const stats = await queueService.getStats();
  const jobs = {
    waiting: await queue.getWaiting(),
    active: await queue.getActive(),
    failed: await queue.getFailed()
  };

  res.json({
    stats,
    jobs: {
      waiting: jobs.waiting.map(j => ({
        id: j.id,
        framework: j.data.framework,
        enqueued_at: j.data.enqueued_at
      })),
      active: jobs.active.map(j => ({
        id: j.id,
        framework: j.data.framework,
        started_at: j.processedOn
      })),
      failed: jobs.failed.slice(0, 10).map(j => ({
        id: j.id,
        framework: j.data.framework,
        error: j.failedReason
      }))
    }
  });
});
```

---

## 7. Job Consuming Workers

### 7.1 Cloudflare Worker Architecture

```
Cloudflare Workers (Serverless)
├── Worker Function: jobConsumer
├── Triggers: Cron (every 10s) + Queue listener
├── Tasks:
│   ├── Pull job from Redis queue
│   ├── Forward request to backend
│   ├── Handle response
│   └── Update database
└── Error Handling:
    ├── Retry logic (3 attempts)
    ├── Dead letter queue
    └── Alert on failure
```

### 7.2 Cloudflare Worker Implementation

```javascript
// cloudflare-workers/job-consumer.js

/**
 * Cloudflare Worker - Job Consumer
 * Pulls jobs from Redis queue and forwards to Python backends
 */

addEventListener('fetch', event => {
  event.respondWith(handleRequest(event.request));
});

// Scheduled execution (every 10 seconds)
addEventListener('scheduled', event => {
  event.waitUntil(processQueue());
});

/**
 * Main queue processing function
 */
async function processQueue() {
  try {
    // Pull job from Redis queue
    const job = await pullJobFromQueue();

    if (!job) {
      console.log('No jobs in queue');
      return;
    }

    console.log('Processing job:', job.id);

    // Update status to RUNNING
    await updateSubmissionStatus(job.data.submission_id, 'RUNNING');

    // Forward to appropriate backend
    const result = await forwardToBackend(job);

    // Store results
    await storeResults(job.data.submission_id, result);

    // Update status to COMPLETED
    await updateSubmissionStatus(job.data.submission_id, 'COMPLETED', {
      verdict: result.verdict,
      tests_passed: result.tests_passed,
      tests_failed: result.tests_failed,
      execution_time_ms: result.execution_time_ms
    });

    // Mark job as complete in queue
    await completeJob(job.id);

    console.log('Job completed:', job.id);

  } catch (error) {
    console.error('Queue processing error:', error);
    await handleJobFailure(job, error);
  }
}

/**
 * Pull job from Redis queue
 */
async function pullJobFromQueue() {
  const response = await fetch(`${REDIS_API_URL}/queue/pull`, {
    method: 'POST',
    headers: {
      'Authorization': `Bearer ${REDIS_API_TOKEN}`,
      'Content-Type': 'application/json'
    }
  });

  if (response.status === 204) {
    return null; // No jobs available
  }

  const job = await response.json();
  return job;
}

/**
 * Forward job to appropriate backend
 */
async function forwardToBackend(job) {
  const { framework, code, problem_name, submission_id } = job.data;

  // Determine backend URL
  const backendUrl = framework === 'numpy'
    ? NUMPY_BACKEND_URL
    : PYTORCH_BACKEND_URL;

  console.log(`Forwarding to ${framework} backend:`, backendUrl);

  // Load test cases from GitHub/S3
  const tests = await loadTestCases(problem_name, framework);

  // Execute code
  const response = await fetch(`${backendUrl}/execute`, {
    method: 'POST',
    headers: {
      'Content-Type': 'application/json',
      'Authorization': `Bearer ${BACKEND_API_TOKEN}`
    },
    body: JSON.stringify({
      submission_id,
      source_code: code,
      tests,
      framework,
      timeout_ms: 30000
    }),
    signal: AbortSignal.timeout(60000) // 60s timeout
  });

  if (!response.ok) {
    throw new Error(`Backend error: ${response.status} ${response.statusText}`);
  }

  const result = await response.json();
  return result;
}

/**
 * Load test cases for problem
 */
async function loadTestCases(problemName, framework) {
  // Try framework-specific tests first
  let testsUrl = `${GITHUB_RAW_URL}/${problemName}/variants/${framework}/tests.json`;

  let response = await fetch(testsUrl);

  // Fallback to default tests for numpy
  if (!response.ok && framework === 'numpy') {
    testsUrl = `${GITHUB_RAW_URL}/${problemName}/tests.json`;
    response = await fetch(testsUrl);
  }

  if (!response.ok) {
    throw new Error(`Failed to load test cases: ${response.status}`);
  }

  const tests = await response.json();
  return tests;
}

/**
 * Update submission status in database
 */
async function updateSubmissionStatus(submissionId, status, additionalData = {}) {
  await fetch(`${API_URL}/api/submissions/${submissionId}/status`, {
    method: 'PATCH',
    headers: {
      'Content-Type': 'application/json',
      'Authorization': `Bearer ${API_TOKEN}`
    },
    body: JSON.stringify({
      status,
      ...additionalData,
      updated_at: new Date().toISOString()
    })
  });
}

/**
 * Store execution results
 */
async function storeResults(submissionId, result) {
  await fetch(`${API_URL}/api/submissions/${submissionId}/results`, {
    method: 'POST',
    headers: {
      'Content-Type': 'application/json',
      'Authorization': `Bearer ${API_TOKEN}`
    },
    body: JSON.stringify({
      verdict: result.verdict,
      tests_passed: result.tests_passed,
      tests_failed: result.tests_failed,
      execution_time_ms: result.execution_time_ms,
      results: result.results
    })
  });
}

/**
 * Mark job as complete in queue
 */
async function completeJob(jobId) {
  await fetch(`${REDIS_API_URL}/queue/complete`, {
    method: 'POST',
    headers: {
      'Authorization': `Bearer ${REDIS_API_TOKEN}`,
      'Content-Type': 'application/json'
    },
    body: JSON.stringify({ job_id: jobId })
  });
}

/**
 * Handle job failure
 */
async function handleJobFailure(job, error) {
  console.error('Job failed:', job.id, error.message);

  // Update submission status
  await updateSubmissionStatus(job.data.submission_id, 'FAILED', {
    error: error.message
  });

  // Mark job as failed
  await fetch(`${REDIS_API_URL}/queue/fail`, {
    method: 'POST',
    headers: {
      'Authorization': `Bearer ${REDIS_API_TOKEN}`,
      'Content-Type': 'application/json'
    },
    body: JSON.stringify({
      job_id: job.id,
      error: error.message
    })
  });
}

/**
 * Handle HTTP requests (for manual triggers)
 */
async function handleRequest(request) {
  if (request.method === 'POST' && new URL(request.url).pathname === '/process') {
    await processQueue();
    return new Response('Queue processed', { status: 200 });
  }

  return new Response('Cloudflare Worker - Job Consumer', { status: 200 });
}

// Environment variables (set in Cloudflare dashboard)
const REDIS_API_URL = REDIS_API_URL_ENV;
const REDIS_API_TOKEN = REDIS_API_TOKEN_ENV;
const NUMPY_BACKEND_URL = NUMPY_BACKEND_URL_ENV;
const PYTORCH_BACKEND_URL = PYTORCH_BACKEND_URL_ENV;
const BACKEND_API_TOKEN = BACKEND_API_TOKEN_ENV;
const API_URL = API_URL_ENV;
const API_TOKEN = API_TOKEN_ENV;
const GITHUB_RAW_URL = GITHUB_RAW_URL_ENV;
```

### 7.3 Cloudflare Worker Configuration

```toml
# wrangler.toml
name = "tensorcode-job-consumer"
type = "javascript"
account_id = "your-account-id"
workers_dev = true

[env.production]
name = "tensorcode-job-consumer-prod"
workers_dev = false

# Cron trigger - run every 10 seconds
[triggers]
crons = ["*/10 * * * *"]

# Environment variables
[vars]
REDIS_API_URL = "https://your-redis-api.com"
NUMPY_BACKEND_URL = "https://numpy.poridhi.io"
PYTORCH_BACKEND_URL = "https://pytorch.poridhi.io"
API_URL = "https://api.tensorcode.com"
GITHUB_RAW_URL = "https://raw.githubusercontent.com/your-repo/ai-problems/main/questions"

# Secrets (set via wrangler secret put)
# wrangler secret put REDIS_API_TOKEN
# wrangler secret put BACKEND_API_TOKEN
# wrangler secret put API_TOKEN
```

### 7.4 Deploy Cloudflare Worker

```bash
# Install Wrangler CLI
npm install -g wrangler

# Login to Cloudflare
wrangler login

# Create worker
wrangler init tensorcode-job-consumer

# Set secrets
wrangler secret put REDIS_API_TOKEN
wrangler secret put BACKEND_API_TOKEN
wrangler secret put API_TOKEN

# Deploy
wrangler publish
```

---

## 8. Backend Executors

### 8.1 Backend Overview

```
Backend Executors
├── NumPy Backend (Python + FastAPI)
│   ├── URL: https://numpy.poridhi.io
│   ├── Environment: CPU-only
│   ├── Packages: numpy, scipy, sklearn, pandas
│   └── Concurrency: 10-20 workers
│
└── PyTorch Backend (Python + FastAPI)
    ├── URL: https://pytorch.poridhi.io
    ├── Environment: GPU optional
    ├── Packages: torch, torchvision, numpy
    └── Concurrency: 5 GPU workers
```

### 8.2 Unified Backend Implementation

```python
# backend/main.py
from fastapi import FastAPI, HTTPException
from pydantic import BaseModel
from typing import List, Dict, Any
import asyncio
import subprocess
import tempfile
import os
from datetime import datetime

app = FastAPI(title="TensorCode Backend Executor")

# Framework configurations
FRAMEWORKS = {
    'numpy': {
        'imports_allowed': ['numpy', 'scipy', 'sklearn', 'pandas', 'math', 'typing', 'collections'],
        'gpu_support': False
    },
    'pytorch': {
        'imports_allowed': ['torch', 'torchvision', 'torch.nn', 'torch.optim', 'numpy', 'math', 'typing'],
        'gpu_support': True
    }
}

class TestCase(BaseModel):
    test: str
    expected_output: str

class ExecutionRequest(BaseModel):
    submission_id: str
    source_code: str
    tests: List[TestCase]
    framework: str
    timeout_ms: int = 30000

class ExecutionResponse(BaseModel):
    submission_id: str
    verdict: str  # ACCEPTED, WRONG_ANSWER, TLE, RE, CE
    tests_passed: int
    tests_failed: int
    execution_time_ms: int
    results: List[Dict[str, Any]]

@app.post("/execute", response_model=ExecutionResponse)
async def execute_code(request: ExecutionRequest):
    """
    Execute user code with test cases
    """
    submission_id = request.submission_id
    framework = request.framework

    # Validate framework
    if framework not in FRAMEWORKS:
        raise HTTPException(400, f"Unsupported framework: {framework}")

    config = FRAMEWORKS[framework]

    # Validate imports
    if not validate_imports(request.source_code, config['imports_allowed']):
        return ExecutionResponse(
            submission_id=submission_id,
            verdict='CE',
            tests_passed=0,
            tests_failed=len(request.tests),
            execution_time_ms=0,
            results=[{
                'test_number': 0,
                'passed': False,
                'error': 'Invalid imports detected'
            }]
        )

    # Execute tests
    results = []
    total_time = 0
    tests_passed = 0
    tests_failed = 0

    for idx, test in enumerate(request.tests):
        result = await execute_single_test(
            user_code=request.source_code,
            test=test,
            timeout_ms=request.timeout_ms,
            framework=framework
        )

        results.append({
            'test_number': idx + 1,
            'passed': result['passed'],
            'expected_output': test.expected_output,
            'actual_output': result['output'],
            'execution_time_ms': result['execution_time_ms'],
            'error': result.get('error')
        })

        total_time += result['execution_time_ms']

        if result['passed']:
            tests_passed += 1
        else:
            tests_failed += 1

        # Stop on first failure for efficiency (optional)
        # if not result['passed']:
        #     break

    # Determine verdict
    if tests_passed == len(request.tests):
        verdict = 'ACCEPTED'
    elif any(r.get('error') == 'Time limit exceeded' for r in results):
        verdict = 'TLE'
    elif any(r.get('error') and 'Error' in r.get('error', '') for r in results):
        verdict = 'RE'
    else:
        verdict = 'WRONG_ANSWER'

    return ExecutionResponse(
        submission_id=submission_id,
        verdict=verdict,
        tests_passed=tests_passed,
        tests_failed=tests_failed,
        execution_time_ms=total_time,
        results=results
    )

async def execute_single_test(
    user_code: str,
    test: TestCase,
    timeout_ms: int,
    framework: str
) -> Dict[str, Any]:
    """Execute a single test case"""

    # Combine user code with test
    combined_code = f"{user_code}\n\n{test.test}"

    # Create temporary file
    with tempfile.NamedTemporaryFile(mode='w', suffix='.py', delete=False) as f:
        f.write(combined_code)
        temp_file = f.name

    try:
        start_time = datetime.now()

        # Execute with subprocess
        process = await asyncio.create_subprocess_exec(
            'python3', temp_file,
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.PIPE,
            env=get_environment(framework)
        )

        try:
            stdout, stderr = await asyncio.wait_for(
                process.communicate(),
                timeout=timeout_ms / 1000.0
            )

            end_time = datetime.now()
            execution_time = int((end_time - start_time).total_seconds() * 1000)

            actual_output = stdout.decode('utf-8').strip()
            expected_output = test.expected_output.strip()

            # Compare outputs
            passed = compare_outputs(actual_output, expected_output)

            return {
                'passed': passed,
                'output': actual_output,
                'execution_time_ms': execution_time,
                'error': stderr.decode('utf-8') if stderr else None
            }

        except asyncio.TimeoutError:
            process.kill()
            return {
                'passed': False,
                'output': '',
                'execution_time_ms': timeout_ms,
                'error': 'Time limit exceeded'
            }

    finally:
        # Clean up temp file
        os.unlink(temp_file)

def validate_imports(code: str, allowed_imports: List[str]) -> bool:
    """Validate that code only uses allowed imports"""
    import ast

    try:
        tree = ast.parse(code)
        for node in ast.walk(tree):
            if isinstance(node, ast.Import):
                for alias in node.names:
                    if alias.name.split('.')[0] not in allowed_imports:
                        return False
            elif isinstance(node, ast.ImportFrom):
                if node.module and node.module.split('.')[0] not in allowed_imports:
                    return False
        return True
    except:
        return False

def compare_outputs(actual: str, expected: str) -> bool:
    """Compare outputs with numerical tolerance for ML problems"""
    # Exact match
    if actual == expected:
        return True

    # Try numerical comparison
    try:
        import numpy as np
        actual_val = eval(actual)
        expected_val = eval(expected)

        if isinstance(actual_val, np.ndarray):
            return np.allclose(actual_val, expected_val, rtol=1e-5, atol=1e-8)
        elif isinstance(actual_val, (float, int)):
            return abs(actual_val - expected_val) < 1e-6
    except:
        pass

    # Fuzzy match (ignore whitespace)
    return ''.join(actual.split()) == ''.join(expected.split())

def get_environment(framework: str) -> Dict[str, str]:
    """Get environment variables for execution"""
    env = os.environ.copy()

    # Disable CUDA for NumPy
    if framework == 'numpy':
        env['CUDA_VISIBLE_DEVICES'] = '-1'

    return env

@app.get("/health")
async def health():
    """Health check endpoint"""
    return {
        "status": "healthy",
        "service": "tensorcode-backend-executor",
        "frameworks": list(FRAMEWORKS.keys())
    }
```

### 8.3 Docker Configuration

**NumPy Backend Dockerfile:**
```dockerfile
FROM python:3.11-slim

WORKDIR /app

# Install dependencies
COPY requirements-numpy.txt .
RUN pip install --no-cache-dir -r requirements-numpy.txt

# Copy application
COPY . .

# Expose port
EXPOSE 8000

# Run server
CMD ["uvicorn", "main:app", "--host", "0.0.0.0", "--port", "8000", "--workers", "4"]
```

**requirements-numpy.txt:**
```
fastapi>=0.104.0
uvicorn[standard]>=0.24.0
pydantic>=2.0.0
numpy>=1.24.0
scipy>=1.11.0
scikit-learn>=1.3.0
pandas>=2.0.0
```

**PyTorch Backend Dockerfile:**
```dockerfile
FROM pytorch/pytorch:2.0.1-cuda11.7-cudnn8-runtime

WORKDIR /app

# Install dependencies
COPY requirements-pytorch.txt .
RUN pip install --no-cache-dir -r requirements-pytorch.txt

# Copy application
COPY . .

# Expose port
EXPOSE 8000

# Run server
CMD ["uvicorn", "main:app", "--host", "0.0.0.0", "--port", "8000", "--workers", "2"]
```

**requirements-pytorch.txt:**
```
fastapi>=0.104.0
uvicorn[standard]>=0.24.0
pydantic>=2.0.0
torch>=2.0.0
torchvision>=0.15.0
numpy>=1.24.0
```

---

## 9. Database & Storage

### 9.1 Complete Database Schema

```sql
-- Users table
CREATE TABLE users (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    email VARCHAR(255) UNIQUE NOT NULL,
    username VARCHAR(100) UNIQUE NOT NULL,
    created_at TIMESTAMP DEFAULT NOW()
);

-- Submissions table
CREATE TABLE submissions (
    id UUID PRIMARY KEY,
    user_id UUID REFERENCES users(id),
    problem_name VARCHAR(255) NOT NULL,
    code TEXT NOT NULL,
    framework VARCHAR(20) NOT NULL CHECK (framework IN ('numpy', 'pytorch')),
    status VARCHAR(20) NOT NULL DEFAULT 'QUEUED'
        CHECK (status IN ('QUEUED', 'RUNNING', 'COMPLETED', 'FAILED')),
    verdict VARCHAR(20) CHECK (verdict IN ('ACCEPTED', 'WRONG_ANSWER', 'TLE', 'RE', 'CE')),
    tests_passed INTEGER,
    tests_failed INTEGER,
    execution_time_ms INTEGER,
    created_at TIMESTAMP DEFAULT NOW(),
    updated_at TIMESTAMP DEFAULT NOW(),
    completed_at TIMESTAMP
);

-- Results table (detailed test case results)
CREATE TABLE results (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    submission_id UUID REFERENCES submissions(id) ON DELETE CASCADE,
    test_number INTEGER NOT NULL,
    passed BOOLEAN NOT NULL,
    expected_output TEXT,
    actual_output TEXT,
    execution_time_ms INTEGER,
    error TEXT,
    created_at TIMESTAMP DEFAULT NOW()
);

-- User progress tracking
CREATE TABLE user_progress (
    user_id UUID REFERENCES users(id),
    problem_name VARCHAR(255) NOT NULL,
    status VARCHAR(20) CHECK (status IN ('attempted', 'solved')),
    attempts INTEGER DEFAULT 0,
    first_solved_at TIMESTAMP,
    best_framework VARCHAR(20),
    best_time_ms INTEGER,
    last_attempt_at TIMESTAMP DEFAULT NOW(),
    PRIMARY KEY (user_id, problem_name)
);

-- Indexes for performance
CREATE INDEX idx_submissions_user_id ON submissions(user_id);
CREATE INDEX idx_submissions_status ON submissions(status);
CREATE INDEX idx_submissions_created_at ON submissions(created_at DESC);
CREATE INDEX idx_submissions_problem ON submissions(problem_name);
CREATE INDEX idx_results_submission_id ON results(submission_id);
CREATE INDEX idx_user_progress_user_id ON user_progress(user_id);
```

### 9.2 Problem Repository Structure (GitHub)

```
ai-problems/
└── questions/
    ├── _problems_index.json
    │
    └── [problem-name]/
        ├── meta.json              # Enhanced with frameworks
        ├── description.md
        ├── learn.md
        ├── example.json
        │
        ├── variants/
        │   ├── numpy/
        │   │   ├── starter_code.py
        │   │   ├── solution.py
        │   │   └── tests.json
        │   │
        │   └── pytorch/
        │       ├── starter_code.py
        │       ├── solution.py
        │       └── tests.json
        │
        └── shared/
            └── test_data.json
```

**Enhanced meta.json:**
```json
{
  "id": "87",
  "title": "Adam Optimizer",
  "difficulty": "hard",
  "category": "Deep Learning",

  "frameworks": {
    "numpy": {
      "enabled": true,
      "difficulty": "hard",
      "default": true,
      "estimated_time_minutes": 45,
      "description": "Implement from scratch - learn the mathematics"
    },
    "pytorch": {
      "enabled": true,
      "difficulty": "medium",
      "gpu_optional": true,
      "estimated_time_minutes": 30,
      "description": "Use PyTorch operations - production approach"
    }
  },

  "problem_type": "python",
  "tags": ["optimization", "deep-learning"],
  "contributor": [...]
}
```

---

## 10. Real-time Updates

### 10.1 WebSocket Server (Socket.io)

```javascript
// src/websocket/server.js

const { Server } = require('socket.io');
const { serverLogger } = require('../utils/logger');

class WebSocketServer {
  constructor(httpServer) {
    this.io = new Server(httpServer, {
      cors: {
        origin: process.env.FRONTEND_URL,
        methods: ['GET', 'POST']
      }
    });

    this.setupEventHandlers();
    serverLogger.info('WebSocket server initialized');
  }

  setupEventHandlers() {
    this.io.on('connection', (socket) => {
      serverLogger.info('Client connected', { socket_id: socket.id });

      // Subscribe to submission updates
      socket.on('subscribe', (submission_id) => {
        socket.join(`submission:${submission_id}`);
        serverLogger.info('Client subscribed', { submission_id });
      });

      // Unsubscribe
      socket.on('unsubscribe', (submission_id) => {
        socket.leave(`submission:${submission_id}`);
        serverLogger.info('Client unsubscribed', { submission_id });
      });

      // Disconnect
      socket.on('disconnect', () => {
        serverLogger.info('Client disconnected', { socket_id: socket.id });
      });
    });
  }

  /**
   * Notify clients about submission status update
   */
  notifyStatusUpdate(submission_id, data) {
    this.io.to(`submission:${submission_id}`).emit('status_update', {
      submission_id,
      status: data.status,
      timestamp: new Date().toISOString()
    });
  }

  /**
   * Notify clients about submission completion
   */
  notifyCompletion(submission_id, data) {
    this.io.to(`submission:${submission_id}`).emit('submission_complete', {
      submission_id,
      verdict: data.verdict,
      tests_passed: data.tests_passed,
      tests_failed: data.tests_failed,
      execution_time_ms: data.execution_time_ms,
      results: data.results,
      timestamp: new Date().toISOString()
    });
  }
}

module.exports = WebSocketServer;
```

### 10.2 Database Triggers for Real-time Notifications

```sql
-- PostgreSQL function to notify on submission updates
CREATE OR REPLACE FUNCTION notify_submission_update()
RETURNS TRIGGER AS $$
BEGIN
    PERFORM pg_notify(
        'submission_update',
        json_build_object(
            'submission_id', NEW.id,
            'status', NEW.status,
            'verdict', NEW.verdict
        )::text
    );
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

-- Trigger on submissions table
CREATE TRIGGER submission_update_trigger
AFTER UPDATE ON submissions
FOR EACH ROW
WHEN (OLD.status IS DISTINCT FROM NEW.status OR OLD.verdict IS DISTINCT FROM NEW.verdict)
EXECUTE FUNCTION notify_submission_update();
```

### 10.3 Node.js PostgreSQL Listener

```javascript
// src/database/listener.js

const { Client } = require('pg');
const { serverLogger } = require('../utils/logger');

class DatabaseListener {
  constructor(webSocketServer) {
    this.ws = webSocketServer;
    this.client = new Client({
      connectionString: process.env.DATABASE_URL
    });
  }

  async start() {
    await this.client.connect();

    // Listen to submission_update channel
    await this.client.query('LISTEN submission_update');

    this.client.on('notification', (msg) => {
      if (msg.channel === 'submission_update') {
        const data = JSON.parse(msg.payload);

        serverLogger.info('Database notification', data);

        // Notify WebSocket clients
        if (data.status === 'COMPLETED') {
          this.ws.notifyCompletion(data.submission_id, data);
        } else {
          this.ws.notifyStatusUpdate(data.submission_id, data);
        }
      }
    });

    serverLogger.info('Database listener started');
  }
}

module.exports = DatabaseListener;
```

---

## 11. Implementation Guide

### 11.1 Phase 1: Core Infrastructure (Week 1-2)

**Tasks:**
```yaml
Database:
  - Create PostgreSQL database
  - Run schema migrations
  - Setup indexes

Queue System:
  - Install Redis
  - Configure Bull queue
  - Test job enqueue/dequeue

API Development:
  - Implement submission controller
  - Create queue service
  - Add WebSocket server
```

**Verification:**
```bash
# Test database connection
psql $DATABASE_URL -c "SELECT 1"

# Test Redis connection
redis-cli ping

# Test API
curl -X POST http://localhost:3000/api/submissions \
  -H "Content-Type: application/json" \
  -d '{"code":"print(1+1)","framework":"numpy","problemName":"test"}'
```

### 11.2 Phase 2: Cloudflare Worker (Week 3)

**Tasks:**
```yaml
Cloudflare Setup:
  - Create Cloudflare account
  - Install Wrangler CLI
  - Configure worker

Development:
  - Implement job consumer logic
  - Add error handling
  - Setup cron triggers

Testing:
  - Test job pulling
  - Test backend forwarding
  - Test result storage
```

**Deploy:**
```bash
cd cloudflare-workers
wrangler login
wrangler secret put REDIS_API_TOKEN
wrangler secret put BACKEND_API_TOKEN
wrangler publish
```

### 11.3 Phase 3: Backend Executors (Week 4-5)

**Tasks:**
```yaml
NumPy Backend:
  - Create FastAPI application
  - Implement test runner
  - Add validation logic
  - Docker containerization
  - Deploy to numpy.poridhi.io

PyTorch Backend:
  - Create FastAPI application
  - Implement test runner
  - Add GPU support (optional)
  - Docker containerization
  - Deploy to pytorch.poridhi.io
```

**Deploy:**
```bash
# Build NumPy backend
cd backend
docker build -t tensorcode-numpy-backend -f Dockerfile.numpy .
docker run -p 8000:8000 tensorcode-numpy-backend

# Build PyTorch backend
docker build -t tensorcode-pytorch-backend -f Dockerfile.pytorch .
docker run -p 8001:8000 --gpus all tensorcode-pytorch-backend
```

### 11.4 Phase 4: Frontend Integration (Week 6)

**Tasks:**
```yaml
Components:
  - Framework selector
  - Code editor integration
  - Real-time status updates
  - Results display

API Integration:
  - Submission API calls
  - WebSocket connection
  - Status polling fallback

Testing:
  - End-to-end flow
  - Error handling
  - UI/UX refinement
```

### 11.5 Phase 5: Testing & Launch (Week 7-8)

**Tasks:**
```yaml
Testing:
  - Load testing (1000+ submissions)
  - Queue performance testing
  - Backend concurrency testing
  - WebSocket stress testing

Monitoring:
  - Setup logging (Winston)
  - Add metrics (Prometheus)
  - Create dashboards (Grafana)
  - Alert configuration

Documentation:
  - API documentation
  - Deployment guide
  - User guide
```

### 11.6 Environment Variables

```bash
# Node Backend (.env)
DATABASE_URL=postgresql://user:pass@localhost:5432/tensorcode
REDIS_HOST=localhost
REDIS_PORT=6379
REDIS_PASSWORD=
FRONTEND_URL=http://localhost:3000
GITHUB_TOKEN=ghp_xxx
GITHUB_REPO=poridhiAILab/ai-problems

# Cloudflare Worker (wrangler.toml + secrets)
REDIS_API_URL=https://your-redis-api.com
REDIS_API_TOKEN=secret
NUMPY_BACKEND_URL=https://numpy.poridhi.io
PYTORCH_BACKEND_URL=https://pytorch.poridhi.io
BACKEND_API_TOKEN=secret
API_URL=https://api.tensorcode.com
API_TOKEN=secret
GITHUB_RAW_URL=https://raw.githubusercontent.com/poridhiAILab/ai-problems/main/questions

# Python Backends
PORT=8000
LOG_LEVEL=info
ALLOWED_ORIGINS=https://tensorcode.com
```

---

## Summary

This architecture provides:

✅ **Asynchronous Execution** - No blocking, immediate responses
✅ **Queue-Based Processing** - Scalable job distribution
✅ **Real-time Updates** - WebSocket for live status
✅ **Multi-Framework** - NumPy (CPU) and PyTorch (GPU)
✅ **Cloudflare Workers** - Serverless job consumers
✅ **Verdict System** - ACCEPTED, WRONG_ANSWER, TLE, RE, CE
✅ **Production-Ready** - Complete error handling and monitoring

**Next Steps:**
1. ✅ Review architecture
2. ⏭️ Setup infrastructure (PostgreSQL, Redis)
3. ⏭️ Implement submission service
4. ⏭️ Deploy Cloudflare worker
5. ⏭️ Build backend executors
6. ⏭️ Integrate frontend
7. ⏭️ Test end-to-end
8. ⏭️ Launch! 🚀
