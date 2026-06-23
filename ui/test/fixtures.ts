import type { Workflow, WorkflowStep, RegisteredWorkflow } from '~/composables/useApi'

// Canned API responses shared across page tests — mirrors the shape the
// sqlite-demo backend returns (epoch-ms timestamp strings, JSON-string
// input/output).

export const registeredWorkflows: RegisteredWorkflow[] = [
  { name: 'greet' },
  { name: 'checkout' },
  { name: 'flaky_task' },
  { name: '__journio_internal_debouncer_workflow' }, // must be filtered out by the UI
]

export const workflows: Workflow[] = [
  {
    workflow_uuid: 'wf-success-1',
    status: 'SUCCESS',
    workflow_name: 'checkout',
    authenticated_user: null,
    assumed_role: null,
    authenticated_roles: null,
    output: '{"order_id":"o1","total":3998,"status":"completed"}',
    executor_id: 'local',
    application_version: 'v1',
    application_id: null,
    attempts: 1,
    queue_name: 'orders',
    timeout: null,
    deduplication_id: null,
    priority: 0,
    queue_partition_key: null,
    input: '{"item":"Widget","quantity":2,"customer":"alice"}',
    error: '',
    created_at: String(Date.now() - 60_000),
    updated_at: String(Date.now() - 50_000),
    workflow_deadline_epoch_ms: null,
    started_at: String(Date.now() - 55_000),
  },
  {
    workflow_uuid: 'wf-error-1',
    status: 'ERROR',
    workflow_name: 'flaky_task',
    authenticated_user: null,
    assumed_role: null,
    authenticated_roles: null,
    output: '',
    executor_id: 'local',
    application_version: 'v1',
    application_id: null,
    attempts: 1,
    queue_name: null,
    timeout: null,
    deduplication_id: null,
    priority: 0,
    queue_partition_key: null,
    input: '3',
    error: 'flaky task failed for seed 3 (odd)',
    created_at: String(Date.now() - 120_000),
    updated_at: String(Date.now() - 110_000),
    workflow_deadline_epoch_ms: null,
    started_at: String(Date.now() - 115_000),
  },
  {
    workflow_uuid: 'wf-pending-1',
    status: 'PENDING',
    workflow_name: 'long_running',
    authenticated_user: null,
    assumed_role: null,
    authenticated_roles: null,
    output: '',
    executor_id: 'local',
    application_version: 'v1',
    application_id: null,
    attempts: 1,
    queue_name: null,
    timeout: null,
    deduplication_id: null,
    priority: 0,
    queue_partition_key: null,
    input: '5',
    error: '',
    created_at: String(Date.now() - 5_000),
    updated_at: String(Date.now() - 4_000),
    workflow_deadline_epoch_ms: null,
    started_at: String(Date.now() - 4_000),
  },
]

export const checkoutSteps: WorkflowStep[] = [
  {
    function_id: 1,
    function_name: 'validate_order',
    output: '"validated 2x Widget"',
    error: null,
    child_workflow_id: null,
  },
  {
    function_id: 2,
    function_name: 'charge_card',
    output: '"charged alice $39.98"',
    error: null,
    child_workflow_id: null,
  },
  {
    function_id: 3,
    function_name: 'ship_order',
    output: '"shipped Widget"',
    error: null,
    child_workflow_id: null,
  },
]

export const failedFlakyWorkflow: Workflow = {
  ...workflows[1],
}

export const failedFlakySteps: WorkflowStep[] = [
  {
    function_id: 1,
    function_name: 'risky_step',
    output: '',
    error: 'flaky task failed for seed 3 (odd)',
    child_workflow_id: null,
  },
]
