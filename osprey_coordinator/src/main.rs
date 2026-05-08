mod backoff_utils;
mod cached_futures;
mod consumer;
mod coordinator_metrics;
mod discovery;
mod etcd;
mod etcd_config;
mod etcd_watcherd;
mod future_utils;
mod gcloud;
mod hashring;
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
use std::sync::atomic::Ordering;
use std::sync::Arc;
use std::time::Duration;

use crate::coordinator_metrics::OspreyCoordinatorMetrics;
use crate::snowflake_client::SnowflakeClient;

use crate::metrics::emit_worker::SpawnEmitWorker;
use crate::metrics::new_client;

use consumer::{start_kafka_consumer, start_pubsub_subscriber};
use priority_queue::{create_ackable_action_priority_queue, spawn_priority_queue_metrics_worker};
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
}

#[tokio::main]
async fn main() -> Result<()> {
    tracing_subscriber::fmt::init();
    let opts = CliOptions::parse();

    tracing::info!("starting Osprey Coordinator");

    tracing::info!("creating osprey-snowflake client");
    let snowflake_client = Arc::new(SnowflakeClient::new(opts.snowflake_api_endpoint));

    let (priority_queue_sender, priority_queue_receiver) = create_ackable_action_priority_queue();
    let metrics = OspreyCoordinatorMetrics::new();
    tracing::info!("starting grpc metrics worker");
    let _worker_guard = metrics
        .clone()
        .spawn_emit_worker(new_client("osprey_coordinator").unwrap());

    let bidi_inner = OspreyCoordinatorServer::new(
        priority_queue_sender.clone(),
        priority_queue_receiver.clone(),
        metrics.clone(),
    );
    let connected_workers = bidi_inner.connected_workers();
    let osprey_coordinator_grpc_bidi_stream_service =
        OspreyCoordinatorServiceServer::new(bidi_inner);

    let osprey_coordinator_sync_action_service =
        OspreyCoordinatorSyncActionServiceServer::new(sync_action_rpc::SyncActionServer::new(
            snowflake_client.clone(),
            priority_queue_sender.clone(),
            metrics.clone(),
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
    // Sync action server has a custom HealthChecker so the K8s readiness probe
    // (when targeted at this service's name in `grpc.health.v1.Health/Check`)
    // only reports SERVING once at least one async worker has connected via
    // bidi. Without this, new coord pods accept discord_api → coord
    // process_action calls before any worker is dialed in to dispatch them,
    // producing the DEADLINE_EXCEEDED bursts seen on every rolling deploy.
    let workers_for_health = connected_workers.clone();
    let sync_action_service_fut = pigeon::Server::new(
        osprey_coordinator_sync_action_service,
        &sync_action_service_name,
        opts.sync_action_port,
    )
    .with_standard_registration(
        &sync_action_service_name,
        std::env::var("POD_IP")
            .expect("`POD_IP needs to be set")
            .into(),
    )
    .with_encoded_file_descriptor_set(crate::proto::PB_DESCRIPTOR_BYTES)
    .with_announce_delay(Some(Duration::from_secs(60)))
    .with_shutdown(signals::exit_signal())
    .with_health_checker(move || workers_for_health.load(Ordering::Relaxed) > 0)
    .serve();

    tracing::info!("starting priority queue metrics worker");
    let _drop_guard =
        spawn_priority_queue_metrics_worker(priority_queue_sender.clone(), metrics.clone());

    shutdown_handler::spawn_shutdown_handler(
        priority_queue_sender.clone(),
        priority_queue_receiver.clone(),
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
