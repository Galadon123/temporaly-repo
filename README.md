# TensorCode Problem Solving System Architecture

## Overview

The TensorCode problem-solving system is a queue-based, asynchronous execution platform that enables users to solve AI/ML problems using either NumPy or PyTorch frameworks. The system leverages Cloudflare Workers for serverless job processing, Redis-backed queues for task distribution, and real-time WebSocket updates for immediate user feedback.

## Architecture Diagram

```mermaid
graph TB
    User[User Interface]
    FS[Framework Selector]
    PSS[Problem Submission Service]
    DB[(Submission Database)]
    Queue[Problem Execution Queue<br/>Redis + Bull]
    CFW[Cloudflare Worker<br/>Job Consumer]
    NPBackend[NumPy Backend<br/>FastAPI]
    PTBackend[PyTorch Backend<br/>FastAPI]
    TestRunner[Test Runner]
    Results[(Results Storage)]
    WS[WebSocket Server<br/>Socket.io]

    User -->|Select Framework| FS
    FS -->|Load starter_code| User
    User -->|Submit Code| PSS
    PSS -->|Create Submission| DB
    PSS -->|Enqueue Job| Queue
    PSS -->|Return submission_id| User

    CFW -->|Poll Every 10s| Queue
    Queue -->|Dequeue Job| CFW

    CFW -->|Framework: numpy| NPBackend
    CFW -->|Framework: pytorch| PTBackend

    NPBackend -->|Execute Tests| TestRunner
    PTBackend -->|Execute Tests| TestRunner

    TestRunner -->|Load test_cases.json| TestRunner
    TestRunner -->|Compare Outputs| TestRunner
    TestRunner -->|Return Verdict| NPBackend
    TestRunner -->|Return Verdict| PTBackend

    NPBackend -->|Store Results| Results
    PTBackend -->|Store Results| Results

    Results -->|Trigger Notification| WS
    WS -->|Real-time Update| User

    DB -.->|Status: QUEUED→RUNNING→COMPLETED| WS
```

## System Components

### 1. User Interface & Framework Selector

**Purpose**: Allows users to choose between NumPy and PyTorch frameworks before solving a problem.

**Key Features**:
- Framework selection dropdown (NumPy/PyTorch)
- Loads framework-specific starter code
- Code editor with syntax highlighting
- Real-time submission status updates via WebSocket
- Results panel showing test outcomes and verdicts

**Technology Stack**:
- React for UI components
- Monaco Editor for code editing
- Socket.io-client for WebSocket connections
- Axios for HTTP requests

**Data Flow**:
1. User selects framework (numpy/pytorch)
2. Frontend fetches starter code: `GET /api/problems/:problemName/starter-code?framework=numpy`
3. User writes solution in editor
4. User clicks "Submit"
5. Frontend sends: `POST /api/submissions` with code, framework, problemName, userId

---

### 2. Problem Submission Service

**Purpose**: Accepts code submissions, creates database records, and enqueues jobs for asynchronous processing.

**Responsibilities**:
- Validate submission data (code, framework, problemName)
- Create submission record in PostgreSQL
- Generate unique submission_id (UUID)
- Enqueue job to Redis queue
- Return immediate response to user (HTTP 202 Accepted)
- Update submission status via database triggers

**API Endpoint**:
```
POST /api/submissions
Content-Type: application/json

{
  "code": "import numpy as np\ndef solve(A):\n    return np.linalg.svd(A)",
  "framework": "numpy",
  "problemName": "12_singular-value-decomposition-svd",
  "userId": "user-uuid-here"
}

Response (202 Accepted):
{
  "submission_id": "550e8400-e29b-41d4-a716-446655440000",
  "status": "QUEUED",
  "message": "Submission queued for execution"
}
```

**Implementation** (Node.js/Express):
```javascript
// submissionController.js
const { v4: uuidv4 } = require('uuid');
const Submission = require('../models/Submission');
const queueService = require('../services/queueService');

async function submitCode(req, res) {
  const { code, framework, problemName, userId } = req.body;

  // Validation
  if (!code || !framework || !problemName || !userId) {
    return res.status(400).json({ error: 'Missing required fields' });
  }

  if (!['numpy', 'pytorch'].includes(framework)) {
    return res.status(400).json({ error: 'Invalid framework' });
  }

  // Create submission record
  const submission_id = uuidv4();
  await Submission.create({
    id: submission_id,
    user_id: userId,
    problem_name: problemName,
    code: code,
    framework: framework,
    status: 'QUEUED',
    created_at: new Date()
  });

  // Enqueue job
  await queueService.enqueueJob({
    submission_id,
    problem_name: problemName,
    code,
    framework,
    user_id: userId
  });

  // Return immediate response
  return res.status(202).json({
    submission_id,
    status: 'QUEUED',
    message: 'Submission queued for execution'
  });
}

module.exports = { submitCode };
```

---

### 3. Submission Database (PostgreSQL)

**Purpose**: Persistent storage for submissions, execution results, and user progress tracking.

**Schema**:
```sql
CREATE TABLE submissions (
    id UUID PRIMARY KEY,
    user_id UUID REFERENCES users(id) NOT NULL,
    problem_name VARCHAR(255) NOT NULL,
    code TEXT NOT NULL,
    framework VARCHAR(20) NOT NULL CHECK (framework IN ('numpy', 'pytorch')),
    status VARCHAR(20) NOT NULL DEFAULT 'QUEUED'
        CHECK (status IN ('QUEUED', 'RUNNING', 'COMPLETED', 'FAILED')),
    verdict VARCHAR(20) CHECK (verdict IN ('ACCEPTED', 'WRONG_ANSWER', 'TLE', 'RE', 'CE')),
    tests_passed INTEGER,
    tests_failed INTEGER,
    execution_time_ms INTEGER,
    error_message TEXT,
    created_at TIMESTAMP DEFAULT NOW(),
    updated_at TIMESTAMP DEFAULT NOW(),
    completed_at TIMESTAMP,
    INDEX idx_user_submissions (user_id, created_at DESC),
    INDEX idx_status (status)
);

CREATE TABLE test_results (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    submission_id UUID REFERENCES submissions(id) ON DELETE CASCADE,
    test_number INTEGER NOT NULL,
    passed BOOLEAN NOT NULL,
    expected_output TEXT,
    actual_output TEXT,
    execution_time_ms INTEGER,
    error_message TEXT,
    created_at TIMESTAMP DEFAULT NOW()
);
```

**Database Triggers** (for real-time WebSocket notifications):
```sql
CREATE OR REPLACE FUNCTION notify_submission_update()
RETURNS TRIGGER AS $$
BEGIN
    PERFORM pg_notify(
        'submission_updates',
        json_build_object(
            'submission_id', NEW.id,
            'status', NEW.status,
            'verdict', NEW.verdict,
            'tests_passed', NEW.tests_passed,
            'tests_failed', NEW.tests_failed
        )::text
    );
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

CREATE TRIGGER submission_status_trigger
AFTER UPDATE ON submissions
FOR EACH ROW
WHEN (OLD.status IS DISTINCT FROM NEW.status OR OLD.verdict IS DISTINCT FROM NEW.verdict)
EXECUTE FUNCTION notify_submission_update();
```

**Status Transitions**:
- `QUEUED`: Initial state when submission is created
- `RUNNING`: Cloudflare Worker picked up the job and forwarded to backend
- `COMPLETED`: Execution finished successfully with verdict
- `FAILED`: System error during execution

**Verdict Types**:
- `ACCEPTED`: All test cases passed
- `WRONG_ANSWER`: One or more test cases failed (incorrect output)
- `TLE`: Time Limit Exceeded (execution timeout)
- `RE`: Runtime Error (exception during execution)
- `CE`: Compilation Error (syntax errors, import failures)

---

### 4. Problem Execution Queue (Redis + Bull)

**Purpose**: Asynchronous job queue for distributing execution tasks to Cloudflare Workers.

**Technology**: Bull (Node.js queue library) backed by Redis

**Configuration**:
```javascript
// queueService.js
const Queue = require('bull');
const redis = require('redis');

const problemQueue = new Queue('problem-execution', {
  redis: {
    host: process.env.REDIS_HOST || 'localhost',
    port: process.env.REDIS_PORT || 6379,
    password: process.env.REDIS_PASSWORD
  },
  defaultJobOptions: {
    attempts: 3,
    backoff: {
      type: 'exponential',
      delay: 5000
    },
    removeOnComplete: true,
    removeOnFail: false,
    timeout: 300000 // 5 minutes max per job
  }
});

async function enqueueJob(jobData) {
  const job = await problemQueue.add('execute-submission', jobData, {
    priority: jobData.framework === 'pytorch' ? 5 : 10, // PyTorch gets higher priority
    jobId: jobData.submission_id
  });

  console.log(`Job ${job.id} enqueued for submission ${jobData.submission_id}`);
  return job;
}

async function getJobStatus(jobId) {
  const job = await problemQueue.getJob(jobId);
  if (!job) return null;

  const state = await job.getState();
  return {
    id: job.id,
    state,
    progress: job.progress(),
    failedReason: job.failedReason
  };
}

module.exports = { enqueueJob, getJobStatus, problemQueue };
```

**Job Data Structure**:
```json
{
  "submission_id": "550e8400-e29b-41d4-a716-446655440000",
  "problem_name": "12_singular-value-decomposition-svd",
  "code": "import numpy as np\ndef svd_2x2_singular_values(A):\n    return np.linalg.svd(A)",
  "framework": "numpy",
  "user_id": "user-uuid",
  "timestamp": "2025-10-20T10:30:00Z"
}
```

**Queue Features**:
- Job prioritization (PyTorch jobs get higher priority)
- Automatic retry with exponential backoff
- Job timeout handling (5 minutes max)
- Dead letter queue for failed jobs
- Job progress tracking

---

### 5. Cloudflare Worker (Job Consumer)

**Purpose**: Serverless worker that polls the queue, picks up jobs, and forwards them to appropriate backend executors.

**Execution Model**:
- Cron trigger: Runs every 10 seconds
- Pulls jobs from Redis queue
- Routes jobs to NumPy or PyTorch backend based on framework
- Updates submission status in database
- Handles errors and retries

**Implementation**:
```javascript
// cloudflare-worker/job-consumer.js

// Scheduled event (runs every 10 seconds)
addEventListener('scheduled', event => {
  event.waitUntil(processQueue());
});

async function processQueue() {
  const job = await pullJobFromQueue();

  if (!job) {
    console.log('No jobs in queue');
    return;
  }

  console.log(`Processing job ${job.id} for submission ${job.data.submission_id}`);

  try {
    // Update status to RUNNING
    await updateSubmissionStatus(job.data.submission_id, 'RUNNING');

    // Route to appropriate backend
    const backendUrl = getBackendUrl(job.data.framework);
    const result = await forwardToBackend(backendUrl, job.data);

    // Store results
    await storeResults(job.data.submission_id, result);

    // Update status to COMPLETED
    await updateSubmissionStatus(job.data.submission_id, 'COMPLETED', {
      verdict: result.verdict,
      tests_passed: result.tests_passed,
      tests_failed: result.tests_failed,
      execution_time_ms: result.total_execution_time_ms
    });

    // Mark job as complete in queue
    await completeJob(job.id);

  } catch (error) {
    console.error(`Job ${job.id} failed:`, error);

    await updateSubmissionStatus(job.data.submission_id, 'FAILED', {
      error_message: error.message
    });

    await failJob(job.id, error.message);
  }
}

async function pullJobFromQueue() {
  const response = await fetch(`${REDIS_API_URL}/queue/pop`, {
    method: 'POST',
    headers: {
      'Authorization': `Bearer ${REDIS_API_TOKEN}`,
      'Content-Type': 'application/json'
    }
  });

  if (response.status === 204) return null; // Queue empty

  return await response.json();
}

function getBackendUrl(framework) {
  return framework === 'numpy'
    ? NUMPY_BACKEND_URL  // e.g., https://numpy-backend.example.com
    : PYTORCH_BACKEND_URL; // e.g., https://pytorch-backend.example.com
}

async function forwardToBackend(backendUrl, jobData) {
  const response = await fetch(`${backendUrl}/execute`, {
    method: 'POST',
    headers: {
      'Content-Type': 'application/json',
      'X-API-Key': BACKEND_API_KEY
    },
    body: JSON.stringify({
      submission_id: jobData.submission_id,
      problem_name: jobData.problem_name,
      source_code: jobData.code,
      framework: jobData.framework,
      timeout_ms: 30000 // 30 second timeout per test
    })
  });

  if (!response.ok) {
    throw new Error(`Backend returned ${response.status}: ${await response.text()}`);
  }

  return await response.json();
}

async function updateSubmissionStatus(submissionId, status, additionalData = {}) {
  await fetch(`${DATABASE_API_URL}/submissions/${submissionId}`, {
    method: 'PATCH',
    headers: {
      'Content-Type': 'application/json',
      'Authorization': `Bearer ${DATABASE_API_TOKEN}`
    },
    body: JSON.stringify({
      status,
      updated_at: new Date().toISOString(),
      ...(status === 'COMPLETED' && { completed_at: new Date().toISOString() }),
      ...additionalData
    })
  });
}

async function storeResults(submissionId, result) {
  // Store individual test results
  for (const testResult of result.test_results) {
    await fetch(`${DATABASE_API_URL}/test-results`, {
      method: 'POST',
      headers: {
        'Content-Type': 'application/json',
        'Authorization': `Bearer ${DATABASE_API_TOKEN}`
      },
      body: JSON.stringify({
        submission_id: submissionId,
        test_number: testResult.test_number,
        passed: testResult.passed,
        expected_output: JSON.stringify(testResult.expected_output),
        actual_output: JSON.stringify(testResult.actual_output),
        execution_time_ms: testResult.execution_time_ms,
        error_message: testResult.error
      })
    });
  }
}

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

async function failJob(jobId, reason) {
  await fetch(`${REDIS_API_URL}/queue/fail`, {
    method: 'POST',
    headers: {
      'Authorization': `Bearer ${REDIS_API_TOKEN}`,
      'Content-Type': 'application/json'
    },
    body: JSON.stringify({ job_id: jobId, reason })
  });
}
```

**Cloudflare Worker Configuration** (`wrangler.toml`):
```toml
name = "tensorcode-job-consumer"
main = "src/job-consumer.js"
compatibility_date = "2025-10-20"

[triggers]
crons = ["*/10 * * * *"] # Every 10 seconds

[vars]
NUMPY_BACKEND_URL = "https://numpy-backend.example.com"
PYTORCH_BACKEND_URL = "https://pytorch-backend.example.com"
REDIS_API_URL = "https://redis-api.example.com"
DATABASE_API_URL = "https://database-api.example.com"

[[kv_namespaces]]
binding = "JOB_CACHE"
id = "your-kv-namespace-id"

[secrets]
REDIS_API_TOKEN
BACKEND_API_KEY
DATABASE_API_TOKEN
```

**Error Handling**:
- Network failures: Retry with exponential backoff
- Backend timeout: Mark as TLE, store partial results
- Database failures: Log error, retry job
- Invalid job data: Mark as FAILED, notify monitoring

---

### 6. Backend Executors (NumPy & PyTorch)

**Purpose**: Execute user code against test cases in isolated environments and return verdicts.

**Architecture**:
- Separate backends for NumPy and PyTorch
- FastAPI for HTTP API
- Docker containers for isolation
- Shared test runner logic

**Unified Backend Implementation** (Python/FastAPI):
```python
# backend/main.py
from fastapi import FastAPI, HTTPException
from pydantic import BaseModel
from typing import List, Dict, Any, Optional
import numpy as np
import json
import subprocess
import tempfile
import os
import signal
import time

app = FastAPI()

class TestCase(BaseModel):
    input: Dict[str, Any]
    expected_output: Any
    timeout_ms: Optional[int] = 30000

class ExecutionRequest(BaseModel):
    submission_id: str
    problem_name: str
    source_code: str
    framework: str  # 'numpy' or 'pytorch'
    timeout_ms: int = 30000

class TestResult(BaseModel):
    test_number: int
    passed: bool
    expected_output: Any
    actual_output: Any
    execution_time_ms: int
    error: Optional[str] = None

class ExecutionResponse(BaseModel):
    submission_id: str
    verdict: str  # ACCEPTED, WRONG_ANSWER, TLE, RE, CE
    tests_passed: int
    tests_failed: int
    total_execution_time_ms: int
    test_results: List[TestResult]

@app.post("/execute", response_model=ExecutionResponse)
async def execute_code(request: ExecutionRequest):
    """
    Execute user code against test cases and return verdict.
    """
    submission_id = request.submission_id
    problem_name = request.problem_name
    source_code = request.source_code
    framework = request.framework

    # Load test cases
    test_cases = load_test_cases(problem_name)

    if not test_cases:
        raise HTTPException(status_code=404, detail=f"Test cases not found for {problem_name}")

    # Execute tests
    results = []
    total_time = 0

    for idx, test in enumerate(test_cases):
        result = await execute_single_test(
            user_code=source_code,
            test=test,
            timeout_ms=request.timeout_ms,
            framework=framework
        )

        results.append(TestResult(
            test_number=idx + 1,
            passed=result['passed'],
            expected_output=test['expected_output'],
            actual_output=result['output'],
            execution_time_ms=result['execution_time_ms'],
            error=result.get('error')
        ))

        total_time += result['execution_time_ms']

    # Determine verdict
    tests_passed = sum(1 for r in results if r.passed)
    tests_failed = len(results) - tests_passed

    if tests_passed == len(test_cases):
        verdict = 'ACCEPTED'
    elif any(r.error and 'Time limit exceeded' in r.error for r in results):
        verdict = 'TLE'
    elif any(r.error and 'Error' in r.error for r in results):
        verdict = 'RE'
    else:
        verdict = 'WRONG_ANSWER'

    return ExecutionResponse(
        submission_id=submission_id,
        verdict=verdict,
        tests_passed=tests_passed,
        tests_failed=tests_failed,
        total_execution_time_ms=total_time,
        test_results=results
    )

def load_test_cases(problem_name: str) -> List[Dict]:
    """Load test cases from JSON file."""
    test_file = f"/app/problems/{problem_name}/test_cases.json"

    if not os.path.exists(test_file):
        return []

    with open(test_file, 'r') as f:
        return json.load(f)

async def execute_single_test(
    user_code: str,
    test: Dict,
    timeout_ms: int,
    framework: str
) -> Dict:
    """
    Execute user code for a single test case.
    Returns: { 'passed': bool, 'output': Any, 'execution_time_ms': int, 'error': str }
    """
    start_time = time.time()

    # Create temporary file with user code + test invocation
    test_code = f"""
{user_code}

# Test invocation
import json
import sys

input_data = {json.dumps(test['input'])}
expected_output = {json.dumps(test['expected_output'])}

try:
    # Call user function (assuming function name matches problem)
    # For SVD problem: svd_2x2_singular_values(A)
    result = svd_2x2_singular_values(input_data['A'])

    # Convert result to JSON-serializable format
    if isinstance(result, tuple):
        result = [r.tolist() if hasattr(r, 'tolist') else r for r in result]
    elif hasattr(result, 'tolist'):
        result = result.tolist()

    print(json.dumps({{'output': result, 'error': None}}))
except Exception as e:
    print(json.dumps({{'output': None, 'error': str(e)}}))
    sys.exit(1)
"""

    with tempfile.NamedTemporaryFile(mode='w', suffix='.py', delete=False) as f:
        f.write(test_code)
        temp_file = f.name

    try:
        # Execute with timeout
        process = subprocess.Popen(
            ['python3', temp_file],
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            preexec_fn=os.setsid
        )

        try:
            stdout, stderr = process.communicate(timeout=timeout_ms / 1000.0)
            execution_time = int((time.time() - start_time) * 1000)

            if process.returncode != 0:
                return {
                    'passed': False,
                    'output': None,
                    'execution_time_ms': execution_time,
                    'error': stderr.decode('utf-8')
                }

            # Parse output
            result_data = json.loads(stdout.decode('utf-8'))

            if result_data['error']:
                return {
                    'passed': False,
                    'output': None,
                    'execution_time_ms': execution_time,
                    'error': result_data['error']
                }

            # Compare outputs
            passed = compare_outputs(result_data['output'], test['expected_output'])

            return {
                'passed': passed,
                'output': result_data['output'],
                'execution_time_ms': execution_time,
                'error': None
            }

        except subprocess.TimeoutExpired:
            os.killpg(os.getpgid(process.pid), signal.SIGTERM)
            return {
                'passed': False,
                'output': None,
                'execution_time_ms': timeout_ms,
                'error': 'Time limit exceeded'
            }

    finally:
        os.unlink(temp_file)

def compare_outputs(actual, expected) -> bool:
    """
    Compare actual and expected outputs with numerical tolerance.
    Handles NumPy arrays, lists, tuples, and scalars.
    """
    try:
        if isinstance(expected, (list, tuple)) and isinstance(actual, (list, tuple)):
            if len(expected) != len(actual):
                return False

            for exp, act in zip(expected, actual):
                if not compare_outputs(act, exp):
                    return False
            return True

        elif isinstance(expected, (int, float)) and isinstance(actual, (int, float)):
            return np.allclose(actual, expected, rtol=1e-5, atol=1e-8)

        else:
            # Convert to numpy arrays for comparison
            exp_array = np.array(expected)
            act_array = np.array(actual)
            return np.allclose(act_array, exp_array, rtol=1e-5, atol=1e-8)

    except Exception:
        return False

@app.get("/health")
async def health_check():
    return {"status": "healthy", "framework": os.getenv("FRAMEWORK", "unknown")}
```

**Dockerfile** (NumPy Backend):
```dockerfile
FROM python:3.11-slim

WORKDIR /app

# Install NumPy and dependencies
RUN pip install --no-cache-dir \
    fastapi==0.104.1 \
    uvicorn==0.24.0 \
    numpy==1.26.0 \
    pydantic==2.5.0

# Copy backend code
COPY main.py /app/
COPY problems/ /app/problems/

ENV FRAMEWORK=numpy

EXPOSE 8000

CMD ["uvicorn", "main:app", "--host", "0.0.0.0", "--port", "8000"]
```

**Dockerfile** (PyTorch Backend):
```dockerfile
FROM python:3.11-slim

WORKDIR /app

# Install PyTorch (CPU version for cost efficiency)
RUN pip install --no-cache-dir \
    fastapi==0.104.1 \
    uvicorn==0.24.0 \
    torch==2.1.0 \
    numpy==1.26.0 \
    pydantic==2.5.0

# Copy backend code
COPY main.py /app/
COPY problems/ /app/problems/

ENV FRAMEWORK=pytorch

EXPOSE 8000

CMD ["uvicorn", "main:app", "--host", "0.0.0.0", "--port", "8000"]
```

**Backend Deployment** (docker-compose.yml):
```yaml
version: '3.8'

services:
  numpy-backend:
    build:
      context: ./backend
      dockerfile: Dockerfile.numpy
    ports:
      - "8001:8000"
    environment:
      - FRAMEWORK=numpy
    volumes:
      - ./ai-problems:/app/problems:ro
    restart: unless-stopped
    deploy:
      resources:
        limits:
          cpus: '2'
          memory: 2G

  pytorch-backend:
    build:
      context: ./backend
      dockerfile: Dockerfile.pytorch
    ports:
      - "8002:8000"
    environment:
      - FRAMEWORK=pytorch
    volumes:
      - ./ai-problems:/app/problems:ro
    restart: unless-stopped
    deploy:
      resources:
        limits:
          cpus: '4'
          memory: 4G
```

---

### 7. Test Runner

**Purpose**: Loads test cases, executes user code, compares outputs with numerical tolerance.

**Test Case Format** (`test_cases.json`):
```json
[
  {
    "test_number": 1,
    "input": {
      "A": [[3, 0], [4, 5]]
    },
    "expected_output": {
      "U": [[0.0, 1.0], [1.0, 0.0]],
      "Sigma": [6.4031, 3.0],
      "V_T": [[0.6247, 0.7809], [-0.7809, 0.6247]]
    },
    "timeout_ms": 30000
  },
  {
    "test_number": 2,
    "input": {
      "A": [[1, 2], [3, 4]]
    },
    "expected_output": {
      "U": [[-0.4046, -0.9145], [-0.9145, 0.4046]],
      "Sigma": [5.4650, 0.3660],
      "V_T": [[-0.5760, -0.8174], [0.8174, -0.5760]]
    },
    "timeout_ms": 30000
  }
]
```

**Comparison Logic**:
The test runner uses `np.allclose()` with tolerances:
- Relative tolerance (rtol): 1e-5
- Absolute tolerance (atol): 1e-8

This handles floating-point precision issues common in ML/numerical computing.

**Test Execution Flow**:
1. Load user code
2. Load test_cases.json
3. For each test case:
   - Inject input parameters
   - Execute user function
   - Capture output
   - Compare with expected output using np.allclose()
   - Record execution time
   - Catch exceptions (RE verdict)
   - Enforce timeout (TLE verdict)
4. Aggregate results
5. Determine overall verdict

---

### 8. Results Storage

**Purpose**: Persist execution results, individual test outcomes, and error messages.

**Database Tables** (already defined in section 3):
- `submissions`: Overall submission metadata and verdict
- `test_results`: Individual test case results

**Storage Process**:
1. Cloudflare Worker receives response from backend
2. Worker stores each test result individually
3. Worker updates submission record with aggregate data:
   - verdict (ACCEPTED/WRONG_ANSWER/TLE/RE/CE)
   - tests_passed, tests_failed
   - total_execution_time_ms
   - completed_at timestamp
4. Database trigger fires, sending notification via pg_notify

**API Endpoint** (for storing results):
```javascript
// resultsController.js
async function storeTestResult(req, res) {
  const { submission_id, test_number, passed, expected_output, actual_output, execution_time_ms, error_message } = req.body;

  await TestResult.create({
    submission_id,
    test_number,
    passed,
    expected_output: JSON.stringify(expected_output),
    actual_output: JSON.stringify(actual_output),
    execution_time_ms,
    error_message
  });

  return res.status(201).json({ message: 'Test result stored' });
}

async function updateSubmissionVerdict(req, res) {
  const { submission_id } = req.params;
  const { verdict, tests_passed, tests_failed, execution_time_ms } = req.body;

  await Submission.update(
    {
      status: 'COMPLETED',
      verdict,
      tests_passed,
      tests_failed,
      execution_time_ms,
      completed_at: new Date()
    },
    { where: { id: submission_id } }
  );

  return res.status(200).json({ message: 'Verdict updated' });
}
```

---

### 9. WebSocket Server (Real-time Updates)

**Purpose**: Push real-time submission status updates to connected users.

**Technology**: Socket.io for WebSocket communication

**Implementation**:
```javascript
// websocketServer.js
const { Server } = require('socket.io');
const { Client } = require('pg');

function initializeWebSocket(httpServer) {
  const io = new Server(httpServer, {
    cors: {
      origin: process.env.FRONTEND_URL,
      methods: ['GET', 'POST']
    }
  });

  // PostgreSQL listener for database notifications
  const pgClient = new Client({
    host: process.env.DB_HOST,
    database: process.env.DB_NAME,
    user: process.env.DB_USER,
    password: process.env.DB_PASSWORD
  });

  pgClient.connect();
  pgClient.query('LISTEN submission_updates');

  pgClient.on('notification', (msg) => {
    if (msg.channel === 'submission_updates') {
      const data = JSON.parse(msg.payload);

      // Emit to specific user's room
      io.to(`submission:${data.submission_id}`).emit('submission_update', {
        submission_id: data.submission_id,
        status: data.status,
        verdict: data.verdict,
        tests_passed: data.tests_passed,
        tests_failed: data.tests_failed,
        timestamp: new Date().toISOString()
      });

      console.log(`WebSocket update sent for submission ${data.submission_id}`);
    }
  });

  // Socket.io connection handling
  io.on('connection', (socket) => {
    console.log(`Client connected: ${socket.id}`);

    // Join room for specific submission
    socket.on('subscribe_submission', (submission_id) => {
      socket.join(`submission:${submission_id}`);
      console.log(`Client ${socket.id} subscribed to submission ${submission_id}`);
    });

    // Leave room
    socket.on('unsubscribe_submission', (submission_id) => {
      socket.leave(`submission:${submission_id}`);
      console.log(`Client ${socket.id} unsubscribed from submission ${submission_id}`);
    });

    socket.on('disconnect', () => {
      console.log(`Client disconnected: ${socket.id}`);
    });
  });

  return io;
}

module.exports = { initializeWebSocket };
```

**Frontend Integration** (React Hook):
```javascript
// useSubmission.js
import { useEffect, useState } from 'react';
import { io } from 'socket.io-client';

export function useSubmission(submissionId) {
  const [status, setStatus] = useState('QUEUED');
  const [verdict, setVerdict] = useState(null);
  const [testsPassed, setTestsPassed] = useState(0);
  const [testsFailed, setTestsFailed] = useState(0);

  useEffect(() => {
    if (!submissionId) return;

    const socket = io(process.env.REACT_APP_WEBSOCKET_URL);

    socket.on('connect', () => {
      console.log('WebSocket connected');
      socket.emit('subscribe_submission', submissionId);
    });

    socket.on('submission_update', (data) => {
      setStatus(data.status);
      setVerdict(data.verdict);
      setTestsPassed(data.tests_passed);
      setTestsFailed(data.tests_failed);
    });

    return () => {
      socket.emit('unsubscribe_submission', submissionId);
      socket.disconnect();
    };
  }, [submissionId]);

  return { status, verdict, testsPassed, testsFailed };
}
```

**Status Update Flow**:
1. Backend/Cloudflare Worker updates submission in PostgreSQL
2. Database trigger executes, sends pg_notify
3. WebSocket server receives notification
4. Server emits Socket.io event to subscribed clients
5. Frontend receives update, re-renders UI

---

## Data Flow: Complete User Journey

### 1. Problem Selection & Framework Choice
```
User → Frontend → GET /api/problems/12_singular-value-decomposition-svd/starter-code?framework=numpy
Frontend ← Backend ← Returns starter code template
```

### 2. Code Submission
```
User clicks "Submit"
Frontend → POST /api/submissions { code, framework, problemName, userId }
Frontend ← HTTP 202 { submission_id, status: "QUEUED" }
Frontend → WebSocket subscribe to submission_id
```

### 3. Job Enqueuing
```
Submission Service → Create submission record in PostgreSQL (status: QUEUED)
Submission Service → Enqueue job in Redis queue
```

### 4. Job Processing
```
Cloudflare Worker (cron: every 10s) → Poll Redis queue
Cloudflare Worker → Dequeue job
Cloudflare Worker → Update submission status to "RUNNING"
Cloudflare Worker → Forward to backend (numpy-backend or pytorch-backend)
```

### 5. Code Execution
```
Backend → Load test_cases.json
Backend → Execute user code for each test
Backend → Compare outputs with np.allclose()
Backend → Determine verdict (ACCEPTED/WRONG_ANSWER/TLE/RE/CE)
Backend → Return results to Cloudflare Worker
```

### 6. Results Storage
```
Cloudflare Worker → Store test results in test_results table
Cloudflare Worker → Update submission with verdict (status: COMPLETED)
PostgreSQL Trigger → pg_notify('submission_updates', data)
```

### 7. Real-time Notification
```
WebSocket Server ← Receives pg_notify
WebSocket Server → Emit to frontend via Socket.io
Frontend ← Receives { submission_id, status: "COMPLETED", verdict: "ACCEPTED", ... }
Frontend → Display results in UI
```

---

## Verdict System

### Verdict Types

| Verdict | Meaning | Trigger Condition |
|---------|---------|-------------------|
| **ACCEPTED** | All tests passed | tests_passed == total_tests |
| **WRONG_ANSWER** | Incorrect output | One or more tests failed comparison |
| **TLE** | Time Limit Exceeded | Execution time > timeout_ms |
| **RE** | Runtime Error | Exception raised during execution |
| **CE** | Compilation Error | Syntax error, import failure |

### Verdict Determination Logic
```python
if tests_passed == len(test_cases):
    verdict = 'ACCEPTED'
elif any(r.error and 'Time limit exceeded' in r.error for r in results):
    verdict = 'TLE'
elif any(r.error and 'Error' in r.error for r in results):
    verdict = 'RE'
elif any(r.error and 'SyntaxError' in r.error or 'ImportError' in r.error for r in results):
    verdict = 'CE'
else:
    verdict = 'WRONG_ANSWER'
```

---

## System Configuration

### Environment Variables

**Problem Submission Service** (.env):
```
DATABASE_URL=postgresql://user:password@localhost:5432/tensorcode
REDIS_URL=redis://localhost:6379
WEBSOCKET_URL=http://localhost:3001
PORT=3000
```

**Cloudflare Worker** (wrangler.toml secrets):
```
NUMPY_BACKEND_URL=https://numpy-backend.example.com
PYTORCH_BACKEND_URL=https://pytorch-backend.example.com
REDIS_API_URL=https://redis-api.example.com
DATABASE_API_URL=https://database-api.example.com
REDIS_API_TOKEN=<secret>
BACKEND_API_KEY=<secret>
DATABASE_API_TOKEN=<secret>
```

**Backend Executors** (.env):
```
FRAMEWORK=numpy  # or pytorch
PORT=8000
```

**WebSocket Server** (.env):
```
DB_HOST=localhost
DB_NAME=tensorcode
DB_USER=postgres
DB_PASSWORD=<secret>
FRONTEND_URL=http://localhost:3000
PORT=3001
```

---

## Performance Considerations

### Scalability
- **Queue-based design**: Decouples submission from execution
- **Horizontal scaling**: Deploy multiple Cloudflare Workers
- **Backend autoscaling**: Scale NumPy/PyTorch containers based on queue depth
- **Database connection pooling**: pg-pool for PostgreSQL

### Optimization
- **Job prioritization**: PyTorch jobs get higher priority (priority: 5 vs 10)
- **Timeout enforcement**: 30 seconds per test, 5 minutes per job
- **Result caching**: Cache test results for identical submissions
- **WebSocket rooms**: Only notify subscribed users, not broadcast

### Monitoring
- Queue depth metrics (Redis)
- Job processing time (avg, p95, p99)
- Backend response times
- Verdict distribution (ACCEPTED vs failures)
- WebSocket connection count
- Database query performance

---

## Security

### Code Execution Isolation
- Docker containers for backend executors
- Resource limits (CPU: 2-4 cores, Memory: 2-4GB)
- Network isolation (no outbound internet access)
- Temporary file cleanup after execution

### API Security
- API keys for Cloudflare Worker ↔ Backend communication
- Database API tokens
- CORS configuration for WebSocket
- Input validation (code length limits, framework whitelist)

### Data Protection
- User code stored encrypted at rest
- Submission IDs are UUIDs (non-guessable)
- WebSocket authentication via JWT
- Rate limiting on submission endpoint

---

## Error Handling

### System Errors
- **Queue connection failure**: Retry with exponential backoff
- **Backend unavailable**: Mark job as failed, retry up to 3 times
- **Database timeout**: Use connection pooling, increase timeout
- **WebSocket disconnect**: Auto-reconnect on frontend

### User Errors
- **Syntax errors**: Return CE verdict with error message
- **Import errors**: Return CE with missing module info
- **Runtime exceptions**: Return RE with stack trace
- **Timeout**: Return TLE with execution time

---

## Deployment Architecture

```
Frontend (React)
  ↓ HTTPS
Load Balancer
  ↓
Problem Submission Service (Node.js)
  ↓ PostgreSQL
Database (RDS/Cloud SQL)
  ↓ Redis
Queue (ElastiCache/Cloud Memorystore)
  ↓ Cron Trigger
Cloudflare Worker
  ↓ HTTPS
Backend Load Balancer
  ↓
NumPy Backend (Docker/ECS)  |  PyTorch Backend (Docker/ECS)
```

### Recommended Infrastructure
- **Frontend**: Vercel, Netlify
- **API Server**: AWS ECS, Google Cloud Run
- **Database**: AWS RDS PostgreSQL, Google Cloud SQL
- **Queue**: AWS ElastiCache Redis, Google Cloud Memorystore
- **Workers**: Cloudflare Workers
- **Backends**: AWS ECS (Fargate), Google Cloud Run
- **WebSocket**: Separate Node.js service on ECS

---

## Implementation Phases

### Phase 1: Core Infrastructure (Week 1-2)
- PostgreSQL database setup with tables and triggers
- Redis queue setup
- Problem Submission Service API
- Basic frontend with framework selector

### Phase 2: Backend Executors (Week 2-3)
- NumPy backend with FastAPI
- PyTorch backend with FastAPI
- Test runner implementation
- Docker containerization

### Phase 3: Queue Processing (Week 3-4)
- Cloudflare Worker setup
- Job polling and routing logic
- Error handling and retries

### Phase 4: Real-time Updates (Week 4-5)
- WebSocket server with Socket.io
- PostgreSQL triggers for notifications
- Frontend WebSocket integration

### Phase 5: Testing & Optimization (Week 5-6)
- End-to-end testing
- Performance optimization
- Monitoring and logging
- Security hardening

---

## Monitoring & Observability

### Key Metrics
- **Submissions per minute**
- **Queue depth** (jobs waiting)
- **Average processing time** (submission → verdict)
- **Backend response time** (p50, p95, p99)
- **Verdict distribution** (% ACCEPTED, % WA, % TLE, etc.)
- **Error rate** (system errors vs user errors)
- **WebSocket connection count**

### Logging
- Structured JSON logs
- Correlation IDs (submission_id traces entire flow)
- Log aggregation (CloudWatch, Stackdriver, Datadog)

### Alerting
- Queue depth > 100 jobs
- Backend error rate > 5%
- Database connection pool exhausted
- WebSocket server disconnects

---

## Conclusion

This queue-based architecture provides:
- **Asynchronous execution**: Non-blocking submissions
- **Scalability**: Independent scaling of each component
- **Real-time feedback**: WebSocket updates
- **Reliability**: Retry logic, error handling
- **Multi-framework support**: NumPy and PyTorch backends
- **Accurate verdicts**: Numerical comparison with tolerance

The system handles the complete user journey from code submission to verdict delivery, with proper isolation, security, and observability at every layer.
