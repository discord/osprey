mod backoff_utils;
mod cached_futures;
mod consumer;
mod coordinator_metrics;
mod discovery;
mod engine_pool;
mod etcd;
mod etcd_config;
mod etcd_watcherd;
mod future_utils;
mod gcloud;
mod metrics;
mod osprey_bidirectional_stream;
mod pigeon;
mod priority_queue;
mod proto;
mod pub_sub_streaming_pull;
mod shutdown_handler;
mod signals;
mod snowflake_client;
mod sync_action_rpc;
mod tokio_utils;
#[cfg(test)]
mod tonic_mock;
use anyhow::Result;
use clap::Parser;
use proto::osprey_coordinator_sync_action::osprey_coordinator_sync_action_service_server::OspreyCoordinatorSyncActionServiceServer;
use std::sync::atomic::AtomicBool;
use std::sync::Arc;
use std::time::Duration;

use crate::coordinator_metrics::OspreyCoordinatorMetrics;
use crate::snowflake_client::SnowflakeClient;

use crate::metrics::emit_worker::SpawnEmitWorker;
use crate::metrics::new_client;

use consumer::{start_kafka_consumer, start_pubsub_subscriber};
use engine_pool::{spawn_engine_routing_watcher, EngineRoutingFlags};
use priority_queue::{
    create_engine_queues, spawn_priority_queue_metrics_worker, spawn_stranded_action_rescuer,
};
use tokio::join;

use crate::osprey_bidirectional_stream::OspreyCoordinatorServer;
use crate::proto::osprey_coordinator_service_server::OspreyCoordinatorServiceServer;

#[derive(Debug, Parser)]
struct CliOptions {
    #[arg(
        short,
        long,
        default_value = "19950",
        env = "OSPREY_COORDINATOR_BIDI_STREAM_PORT"
    )]
    bidi_stream_port: u16,
    #[arg(
        long,
        default_value = "19951",
        env = "OSPREY_COORDINATOR_SYNC_ACTION_PORT"
    )]
    sync_action_port: u16,
    #[arg(
        long,
        default_value = "http://localhost:19952",
        env = "SNOWFLAKE_API_ENDPOINT"
    )]
    snowflake_api_endpoint: String,
    #[arg(
        long,
        default_value = "osprey_coordinator",
        env = "OSPREY_COORDINATOR_SERVICE_NAME"
    )]
    service_name: String,
    /// etcd directory holding the engine routing flags `rust_bps_async`, `rust_bps_sync`, `shadow_bps_async` and
    /// `shadow_bps_sync`, in basis points. A missing key is 0.
    #[arg(
        long,
        default_value = "/config/osprey_coordinator/engine_routing",
        env = "OSPREY_COORDINATOR_ENGINE_ROUTING_ETCD_ROOT"
    )]
    engine_routing_etcd_root: String,
    /// Capacity of each shadow queue (sync and async); a copy that finds it full is dropped.
    #[arg(
        long,
        default_value = "1000",
        env = "OSPREY_COORDINATOR_SHADOW_QUEUE_CAPACITY"
    )]
    shadow_queue_capacity: usize,
}

#[tokio::main]
async fn main() -> Result<()> {
    tracing_subscriber::fmt::init();
    let opts = CliOptions::parse();

    tracing::info!("starting Osprey Coordinator");

    tracing::info!("creating osprey-snowflake client");
    let snowflake_client = Arc::new(SnowflakeClient::new(opts.snowflake_api_endpoint));

    let metrics = OspreyCoordinatorMetrics::new();
    let engine_routing_flags = Arc::new(EngineRoutingFlags::default());
    spawn_engine_routing_watcher(
        opts.engine_routing_etcd_root.clone(),
        engine_routing_flags.clone(),
    );
    let (priority_queue_sender, priority_queue_receivers) = create_engine_queues(
        engine_routing_flags,
        metrics.clone(),
        opts.shadow_queue_capacity,
    );
    tracing::info!("starting grpc metrics worker");
    let _worker_guard = metrics
        .clone()
        .spawn_emit_worker(new_client("osprey_coordinator").unwrap());

    let osprey_coordinator_grpc_bidi_stream_service =
        OspreyCoordinatorServiceServer::new(OspreyCoordinatorServer::new(
            priority_queue_sender.clone(),
            priority_queue_receivers.clone(),
            metrics.clone(),
        ));

    let is_shutting_down = Arc::new(AtomicBool::new(false));

    let osprey_coordinator_sync_action_service =
        OspreyCoordinatorSyncActionServiceServer::new(sync_action_rpc::SyncActionServer::new(
            snowflake_client.clone(),
            priority_queue_sender.clone(),
            metrics.clone(),
            is_shutting_down.clone(),
        ));

    let consumer_type = std::env::var("OSPREY_COORDINATOR_CONSUMER_TYPE").ok();

    let consumer_fut = match consumer_type.as_deref() {
        Some("kafka") => {
            tracing::info!("starting Kafka consumer");
            Box::pin(start_kafka_consumer(
                snowflake_client.clone(),
                priority_queue_sender.clone(),
                metrics.clone(),
            ))
                as std::pin::Pin<Box<dyn std::future::Future<Output = Result<()>> + Send>>
        }
        Some("pubsub") => {
            tracing::info!("starting PubSub subscriber");
            Box::pin(start_pubsub_subscriber(
                snowflake_client.clone(),
                priority_queue_sender.clone(),
                metrics.clone(),
            ))
                as std::pin::Pin<Box<dyn std::future::Future<Output = Result<()>> + Send>>
        }
        Some(invalid) => {
            anyhow::bail!(
                "invalid OSPREY_COORDINATOR_CONSUMER_TYPE '{}', must be 'kafka' or 'pubsub'",
                invalid
            );
        }
        None => {
            tracing::info!(
                "OSPREY_COORDINATOR_CONSUMER_TYPE not set, defaulting to Kafka consumer"
            );
            Box::pin(start_kafka_consumer(
                snowflake_client.clone(),
                priority_queue_sender.clone(),
                metrics.clone(),
            ))
                as std::pin::Pin<Box<dyn std::future::Future<Output = Result<()>> + Send>>
        }
    };

    let bidi_service_name = opts.service_name.clone();
    let sync_action_service_name = format!("{}_sync_action", opts.service_name);
    tracing::info!(
        bidi_service_name = %bidi_service_name,
        sync_action_service_name = %sync_action_service_name,
        "registering coordinator services in etcd"
    );

    let grpc_bidi_stream_service_fut = pigeon::serve(
        osprey_coordinator_grpc_bidi_stream_service,
        &bidi_service_name,
        opts.bidi_stream_port,
        Duration::from_secs(30),
    );
    let sync_action_service_fut = pigeon::serve(
        osprey_coordinator_sync_action_service,
        &sync_action_service_name,
        opts.sync_action_port,
        Duration::from_secs(60),
    );

    tracing::info!("starting priority queue metrics worker");
    let _drop_guard =
        spawn_priority_queue_metrics_worker(priority_queue_sender.clone(), metrics.clone());
    let _rescuer_guard = spawn_stranded_action_rescuer(priority_queue_sender.clone());

    shutdown_handler::spawn_shutdown_handler(
        priority_queue_sender.clone(),
        priority_queue_receivers.clone(),
        is_shutting_down.clone(),
    );

    tracing::info!("starting consumer/bidi stream/sync classification rpc");
    let (consumer_result, grpc_bidi_stream_service_result, sync_action_service_result) = join!(
        consumer_fut,
        grpc_bidi_stream_service_fut,
        sync_action_service_fut
    );
    tracing::info!({
        consumer_result=?consumer_result,
        bidi_stream_result=?grpc_bidi_stream_service_result,
        sync_action_result=?sync_action_service_result},
        "osprey coordinator terminated");

    Ok(())
}
