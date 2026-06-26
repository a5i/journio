use std::collections::HashMap;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use async_trait::async_trait;
use journio_core::error::JournioResult;
use journio_core::{
    Config, EnqueueOptions, ForkWorkflowOptions, JournioContext, JournioError, JournioErrorCode,
    ListWorkflowsFilter, QueueOptions, ReadStreamOptions, Step, Workflow, WorkflowContext,
};
use napi::bindgen_prelude::*;
use napi::threadsafe_function::{
    ErrorStrategy, ThreadSafeCallContext, ThreadsafeFunction, ThreadsafeFunctionCallMode,
};
use napi_derive::napi;
use once_cell::sync::Lazy;
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};
use tokio::sync::oneshot;

type Dispatcher = ThreadsafeFunction<Value, ErrorStrategy::Fatal>;
type PendingSender = oneshot::Sender<std::result::Result<Value, String>>;

static STATE: Lazy<Mutex<NativeState>> = Lazy::new(|| Mutex::new(NativeState::default()));
static PENDING: Lazy<Mutex<HashMap<String, PendingSender>>> =
    Lazy::new(|| Mutex::new(HashMap::new()));
static WORKFLOW_CONTEXTS: Lazy<Mutex<HashMap<String, WorkflowContext>>> =
    Lazy::new(|| Mutex::new(HashMap::new()));

#[derive(Default)]
struct NativeState {
    context: Option<Arc<JournioContext>>,
    dispatcher: Option<Dispatcher>,
}

#[derive(Debug, Clone, Deserialize)]
#[serde(rename_all = "camelCase")]
struct NativeConfig {
    name: String,
    system_database_url: String,
    system_database_schema_name: Option<String>,
    application_version: Option<String>,
    #[serde(alias = "executorID")]
    executor_id: Option<String>,
    run_admin_server: Option<bool>,
    admin_port: Option<u16>,
    listen_queues: Option<Vec<String>>,
}

#[derive(Debug, Clone, Deserialize, Default)]
#[serde(rename_all = "camelCase")]
struct NativeStartWorkflowParams {
    #[serde(alias = "workflowID")]
    workflow_id: Option<String>,
    queue_name: Option<String>,
    timeout_ms: Option<u64>,
    enqueue_options: Option<NativeEnqueueOptions>,
}

#[derive(Debug, Clone, Deserialize, Default)]
#[serde(rename_all = "camelCase")]
struct NativeEnqueueOptions {
    #[serde(alias = "deduplicationID")]
    deduplication_id: Option<String>,
    priority: Option<i32>,
    queue_partition_key: Option<String>,
}

#[derive(Debug, Clone, Deserialize, Default)]
#[serde(rename_all = "camelCase")]
struct NativeQueueOptions {
    concurrency: Option<i32>,
    worker_concurrency: Option<i32>,
    rate_limit: Option<NativeRateLimit>,
    priority_enabled: Option<bool>,
    partition_queue: Option<bool>,
}

#[derive(Debug, Clone, Deserialize)]
#[serde(rename_all = "camelCase")]
struct NativeRateLimit {
    limit_per_period: i32,
    period_sec: f64,
}

#[derive(Debug, Clone, Deserialize, Default)]
#[serde(rename_all = "camelCase")]
struct NativeListWorkflowsFilter {
    #[serde(alias = "workflowIDs")]
    workflow_ids: Option<Vec<String>>,
    workflow_id_prefix: Option<String>,
    workflow_name: Option<String>,
    status: Option<String>,
    queue_name: Option<String>,
    queues_only: Option<bool>,
    limit: Option<i64>,
    offset: Option<i64>,
    sort_desc: Option<bool>,
}

#[derive(Debug, Clone, Deserialize, Default)]
#[serde(rename_all = "camelCase")]
struct NativeForkOptions {
    #[serde(alias = "newWorkflowID")]
    new_workflow_id: Option<String>,
    start_step: Option<u32>,
    application_version: Option<String>,
    queue_name: Option<String>,
    queue_partition_key: Option<String>,
}

#[derive(Debug, Clone, Serialize)]
#[serde(rename_all = "camelCase")]
struct NativeRuntimeInfo {
    application_version: String,
    executor_id: String,
}

struct JsWorkflow {
    name: String,
    callback_id: String,
}

#[async_trait]
impl Workflow for JsWorkflow {
    fn name(&self) -> &str {
        &self.name
    }

    async fn run(&self, ctx: &WorkflowContext, input: Value) -> JournioResult<Value> {
        WORKFLOW_CONTEXTS
            .lock()
            .expect("workflow contexts")
            .insert(ctx.workflow_id().to_string(), ctx.clone());

        let result = dispatch_and_wait(json!({
            "kind": "workflow",
            "callbackID": self.callback_id,
            "workflowID": ctx.workflow_id(),
            "stepID": ctx.current_step_id(),
            "applicationVersion": ctx.application_version()?,
            "executorID": ctx.executor_id()?,
            "input": input,
        }))
        .await;

        WORKFLOW_CONTEXTS
            .lock()
            .expect("workflow contexts")
            .remove(ctx.workflow_id());

        result
    }
}

struct JsStep {
    name: String,
    callback_id: String,
}

#[async_trait]
impl Step for JsStep {
    fn name(&self) -> &str {
        &self.name
    }

    async fn run(&self, ctx: &WorkflowContext) -> JournioResult<Value> {
        dispatch_and_wait(json!({
            "kind": "step",
            "callbackID": self.callback_id,
            "workflowID": ctx.workflow_id(),
            "stepID": ctx.current_step_id(),
            "applicationVersion": ctx.application_version()?,
            "executorID": ctx.executor_id()?,
            "input": Value::Null,
        }))
        .await
    }
}

#[napi]
pub fn native_register_dispatcher(env: Env, dispatcher: JsFunction) -> Result<()> {
    let mut tsfn: Dispatcher = dispatcher
        .create_threadsafe_function(0, |ctx: ThreadSafeCallContext<Value>| Ok(vec![ctx.value]))?;
    tsfn.unref(&env)?;
    STATE.lock().expect("state").dispatcher = Some(tsfn);
    Ok(())
}

#[napi]
pub async fn native_set_config(config: Value) -> Result<()> {
    let config: NativeConfig = serde_json::from_value(config).map_err(to_napi_error)?;
    let context = create_context(config).await.map_err(to_napi_error)?;
    STATE.lock().expect("state").context = Some(context);
    Ok(())
}

#[napi]
pub async fn native_launch() -> Result<()> {
    context()?.launch().await.map_err(to_napi_error)
}

#[napi]
pub async fn native_shutdown(timeout_ms: Option<u32>) -> Result<()> {
    let timeout = Duration::from_millis(timeout_ms.unwrap_or(1_000) as u64);
    context()?.shutdown(timeout).await.map_err(to_napi_error)?;
    STATE.lock().expect("state").context = None;
    Ok(())
}

#[napi]
pub fn native_runtime_info() -> Result<Value> {
    let ctx = context()?;
    serde_json::to_value(NativeRuntimeInfo {
        application_version: ctx.application_version(),
        executor_id: ctx.executor_id(),
    })
    .map_err(to_napi_error)
}

#[napi]
pub fn native_register_workflow(name: String, callback_id: String) -> Result<()> {
    context()?
        .register_workflow(Arc::new(JsWorkflow { name, callback_id }))
        .map_err(to_napi_error)
}

#[napi]
pub async fn native_run_workflow(name: String, args: Value) -> Result<String> {
    let handle = context()?
        .run_workflow(&name, args)
        .await
        .map_err(to_napi_error)?;
    Ok(handle.workflow_id().to_string())
}

#[napi]
pub async fn native_start_workflow(
    name: String,
    args: Value,
    params: Option<Value>,
) -> Result<String> {
    let params = decode_or_default::<NativeStartWorkflowParams>(params)?;
    let ctx = context()?;
    let handle = if let Some(queue_name) = params.queue_name.clone() {
        ctx.enqueue_workflow(&queue_name, &name, args, enqueue_options(params))
            .await
    } else {
        ctx.start_workflow_background(&name, args, enqueue_options(params))
            .await
    }
    .map_err(to_napi_error)?;
    Ok(handle.workflow_id().to_string())
}

#[napi]
pub async fn native_run_step(
    workflow_id: String,
    name: String,
    callback_id: String,
) -> Result<Value> {
    let workflow_ctx = WORKFLOW_CONTEXTS
        .lock()
        .expect("workflow contexts")
        .get(&workflow_id)
        .cloned()
        .ok_or_else(|| Error::from_reason("Journio.runStep requires an active workflow context"))?;
    workflow_ctx
        .run_as_step(Arc::new(JsStep { name, callback_id }))
        .await
        .map_err(to_napi_error)
}

#[napi]
pub async fn native_get_result(workflow_id: String, timeout_ms: Option<u32>) -> Result<Value> {
    context()?
        .workflow_handle(workflow_id)
        .get_result(timeout_ms.map(|ms| Duration::from_millis(ms as u64)))
        .await
        .map_err(to_napi_error)
}

#[napi]
pub async fn native_get_status(workflow_id: String) -> Result<Value> {
    let status = context()?
        .workflow_handle(workflow_id)
        .get_status()
        .await
        .map_err(to_napi_error)?;
    serde_json::to_value(status).map_err(to_napi_error)
}

#[napi]
pub async fn native_cancel_workflow(workflow_id: String) -> Result<bool> {
    context()?
        .cancel_workflow(&workflow_id)
        .await
        .map_err(to_napi_error)
}

#[napi]
pub async fn native_resume_workflow(
    workflow_id: String,
    queue_name: Option<String>,
) -> Result<bool> {
    context()?
        .resume_workflow(&workflow_id, queue_name.as_deref())
        .await
        .map_err(to_napi_error)
}

#[napi]
pub async fn native_fork_workflow(workflow_id: String, options: Option<Value>) -> Result<String> {
    let options = decode_or_default::<NativeForkOptions>(options)?;
    let handle = context()?
        .fork_workflow(
            &workflow_id,
            ForkWorkflowOptions {
                workflow_id: options.new_workflow_id,
                start_step: options.start_step.unwrap_or(0),
                application_version: options.application_version,
                queue_name: options.queue_name,
                queue_partition_key: options.queue_partition_key,
            },
        )
        .await
        .map_err(to_napi_error)?;
    Ok(handle.workflow_id().to_string())
}

#[napi]
pub async fn native_sleep(workflow_id: String, duration_ms: u32) -> Result<u32> {
    let workflow_ctx = active_context(&workflow_id)?;
    let slept = workflow_ctx
        .sleep(Duration::from_millis(duration_ms as u64))
        .await
        .map_err(to_napi_error)?;
    Ok(slept.as_millis().try_into().unwrap_or(u32::MAX))
}

#[napi]
pub async fn native_send(
    workflow_id: Option<String>,
    destination_id: String,
    message: Value,
    topic: Option<String>,
) -> Result<()> {
    let topic = topic.unwrap_or_default();
    if let Some(workflow_id) = workflow_id {
        active_context(&workflow_id)?
            .send(&destination_id, message, &topic)
            .await
            .map_err(to_napi_error)
    } else {
        context()?
            .send(&destination_id, message, &topic)
            .await
            .map_err(to_napi_error)
    }
}

#[napi]
pub async fn native_recv(
    workflow_id: String,
    topic: Option<String>,
    timeout_ms: Option<u32>,
) -> Result<Value> {
    let timeout = Duration::from_millis(timeout_ms.unwrap_or(60_000) as u64);
    match active_context(&workflow_id)?
        .recv(&topic.unwrap_or_default(), timeout)
        .await
    {
        Ok(value) => Ok(value),
        Err(err) if err.code == JournioErrorCode::TimeoutError => Ok(Value::Null),
        Err(err) => Err(to_napi_error(err)),
    }
}

#[napi]
pub async fn native_set_event(workflow_id: String, key: String, value: Value) -> Result<()> {
    active_context(&workflow_id)?
        .set_event(&key, value)
        .await
        .map_err(to_napi_error)
}

#[napi]
pub async fn native_get_event(
    workflow_id: Option<String>,
    target_workflow_id: String,
    key: String,
    timeout_ms: Option<u32>,
) -> Result<Value> {
    let timeout = Duration::from_millis(timeout_ms.unwrap_or(60_000) as u64);
    if let Some(workflow_id) = workflow_id {
        active_context(&workflow_id)?
            .get_event(&target_workflow_id, &key, timeout)
            .await
            .map_err(to_napi_error)
    } else {
        context()?
            .get_event(&target_workflow_id, &key, timeout)
            .await
            .map_err(to_napi_error)
    }
}

#[napi]
pub async fn native_write_stream(workflow_id: String, key: String, value: Value) -> Result<()> {
    active_context(&workflow_id)?
        .write_stream(&key, value)
        .await
        .map_err(to_napi_error)
}

#[napi]
pub async fn native_close_stream(workflow_id: String, key: String) -> Result<()> {
    active_context(&workflow_id)?
        .close_stream(&key)
        .await
        .map_err(to_napi_error)
}

#[napi]
pub async fn native_read_stream(
    workflow_id: String,
    key: String,
    snapshot: Option<bool>,
    from_offset: Option<i64>,
) -> Result<Value> {
    let (values, closed) = context()?
        .read_stream(
            &workflow_id,
            &key,
            ReadStreamOptions {
                snapshot: snapshot.unwrap_or(false),
                from_offset: from_offset.unwrap_or(0),
            },
        )
        .await
        .map_err(to_napi_error)?;
    Ok(json!({ "values": values, "closed": closed }))
}

#[napi]
pub async fn native_register_queue(name: String, options: Option<Value>) -> Result<Value> {
    let options = decode_or_default::<NativeQueueOptions>(options)?;
    let queue = context()?
        .register_queue(
            &name,
            QueueOptions {
                concurrency: options.concurrency,
                worker_concurrency: options.worker_concurrency,
                rate_limit_max: options.rate_limit.as_ref().map(|r| r.limit_per_period),
                rate_limit_period: options
                    .rate_limit
                    .as_ref()
                    .map(|r| Duration::from_secs_f64(r.period_sec)),
                priority_enabled: options.priority_enabled.unwrap_or(false),
                partition_queue: options.partition_queue.unwrap_or(false),
                polling_interval: None,
            },
        )
        .await
        .map_err(to_napi_error)?;
    serde_json::to_value(queue).map_err(to_napi_error)
}

#[napi]
pub async fn native_list_workflows(filter: Option<Value>) -> Result<Value> {
    let filter = list_filter(decode_or_default::<NativeListWorkflowsFilter>(filter)?);
    let workflows = context()?
        .list_workflows_filtered(&filter)
        .await
        .map_err(to_napi_error)?;
    serde_json::to_value(workflows).map_err(to_napi_error)
}

#[napi]
pub async fn native_list_workflow_steps(workflow_id: String) -> Result<Value> {
    let steps = context()?
        .get_workflow_steps(&workflow_id)
        .await
        .map_err(to_napi_error)?;
    serde_json::to_value(steps).map_err(to_napi_error)
}

#[napi]
pub async fn native_patch(workflow_id: String, patch_name: String) -> Result<bool> {
    active_context(&workflow_id)?
        .patch(&patch_name)
        .await
        .map_err(to_napi_error)
}

#[napi]
pub async fn native_deprecate_patch(workflow_id: String, patch_name: String) -> Result<bool> {
    active_context(&workflow_id)?
        .deprecate_patch(&patch_name)
        .await
        .map_err(to_napi_error)?;
    Ok(true)
}

#[napi]
pub fn native_complete_callback(
    request_id: String,
    ok: bool,
    value: Option<Value>,
    error: Option<String>,
) -> Result<()> {
    let Some(sender) = PENDING.lock().expect("pending").remove(&request_id) else {
        return Err(Error::from_reason(format!(
            "unknown Journio callback request: {request_id}"
        )));
    };
    let result = if ok {
        Ok(value.unwrap_or(Value::Null))
    } else {
        Err(error.unwrap_or_else(|| "JavaScript callback failed".to_string()))
    };
    let _ = sender.send(result);
    Ok(())
}

async fn create_context(config: NativeConfig) -> JournioResult<Arc<JournioContext>> {
    let schema = config
        .system_database_schema_name
        .clone()
        .unwrap_or_else(|| "journio".to_string());
    let system_db: Arc<dyn journio_core::SystemDatabase> = if config
        .system_database_url
        .starts_with("sqlite:")
    {
        Arc::new(journio_sqlite::SqliteSystemDatabase::connect(&config.system_database_url).await?)
    } else if config.system_database_url.starts_with("postgres:")
        || config.system_database_url.starts_with("postgresql:")
    {
        Arc::new(journio_postgres::PostgresSystemDatabase::connect(
            &config.system_database_url,
            &schema,
        )?)
    } else {
        return Err(JournioError::new(
            JournioErrorCode::InitializationError,
            format!(
                "unsupported systemDatabaseUrl: {}",
                config.system_database_url
            ),
        ));
    };

    JournioContext::new(Config {
        app_name: config.name,
        system_db: Some(system_db),
        database_schema: Some(schema),
        admin_server: config.run_admin_server.unwrap_or(false),
        admin_server_port: config.admin_port,
        application_version: config.application_version,
        executor_id: config.executor_id,
        listen_queues: config.listen_queues,
        ..Default::default()
    })
    .await
}

async fn dispatch_and_wait(mut payload: Value) -> JournioResult<Value> {
    let request_id = uuid::Uuid::new_v4().to_string();
    payload["requestID"] = Value::String(request_id.clone());
    let (sender, receiver) = oneshot::channel();
    PENDING
        .lock()
        .expect("pending")
        .insert(request_id.clone(), sender);

    let dispatcher = STATE
        .lock()
        .expect("state")
        .dispatcher
        .clone()
        .ok_or_else(|| {
            JournioError::new(
                JournioErrorCode::InitializationError,
                "Journio JavaScript dispatcher is not registered",
            )
        })?;

    dispatcher.call(payload, ThreadsafeFunctionCallMode::NonBlocking);
    match receiver.await {
        Ok(Ok(value)) => Ok(value),
        Ok(Err(message)) => Err(JournioError::new(
            JournioErrorCode::WorkflowExecutionError,
            message,
        )),
        Err(_) => Err(JournioError::new(
            JournioErrorCode::WorkflowExecutionError,
            format!("JavaScript callback request {request_id} was dropped"),
        )),
    }
}

fn context() -> Result<Arc<JournioContext>> {
    STATE
        .lock()
        .expect("state")
        .context
        .clone()
        .ok_or_else(|| Error::from_reason("Journio.setConfig must be called before this method"))
}

fn active_context(workflow_id: &str) -> Result<WorkflowContext> {
    WORKFLOW_CONTEXTS
        .lock()
        .expect("workflow contexts")
        .get(workflow_id)
        .cloned()
        .ok_or_else(|| Error::from_reason("method requires an active workflow context"))
}

fn enqueue_options(params: NativeStartWorkflowParams) -> EnqueueOptions {
    let enqueue = params.enqueue_options.unwrap_or_default();
    EnqueueOptions {
        workflow_id: params.workflow_id,
        deduplication_id: enqueue.deduplication_id,
        priority: enqueue.priority.unwrap_or(0),
        queue_partition_key: enqueue.queue_partition_key,
        timeout: params.timeout_ms.map(Duration::from_millis),
        ..Default::default()
    }
}

fn list_filter(input: NativeListWorkflowsFilter) -> ListWorkflowsFilter {
    let mut filter = ListWorkflowsFilter {
        workflow_ids: input.workflow_ids.unwrap_or_default(),
        queues_only: input.queues_only.unwrap_or(false),
        queue_names: input.queue_name.into_iter().collect(),
        names: input.workflow_name.into_iter().collect(),
        limit: input.limit,
        offset: input.offset,
        sort_desc: input.sort_desc.unwrap_or(false),
        ..Default::default()
    };
    if let Some(prefix) = input.workflow_id_prefix {
        filter.workflow_id_prefixes.push(prefix);
    }
    if let Some(status) = input.status {
        if let Some(status) = workflow_status_type(&status) {
            filter.statuses.push(status);
        }
    }
    filter
}

fn workflow_status_type(status: &str) -> Option<journio_core::WorkflowStatusType> {
    match status {
        "PENDING" => Some(journio_core::WorkflowStatusType::Pending),
        "ENQUEUED" => Some(journio_core::WorkflowStatusType::Enqueued),
        "DELAYED" => Some(journio_core::WorkflowStatusType::Delayed),
        "SUCCESS" => Some(journio_core::WorkflowStatusType::Success),
        "ERROR" => Some(journio_core::WorkflowStatusType::Error),
        "CANCELLED" => Some(journio_core::WorkflowStatusType::Cancelled),
        "MAX_RECOVERY_ATTEMPTS_EXCEEDED" | "RETRIES_EXCEEDED" => {
            Some(journio_core::WorkflowStatusType::MaxRecoveryAttemptsExceeded)
        }
        _ => None,
    }
}

fn decode_or_default<T>(value: Option<Value>) -> Result<T>
where
    T: for<'de> Deserialize<'de> + Default,
{
    match value {
        Some(value) => serde_json::from_value(value).map_err(to_napi_error),
        None => Ok(T::default()),
    }
}

fn to_napi_error<E: std::fmt::Display>(err: E) -> Error {
    Error::from_reason(err.to_string())
}
