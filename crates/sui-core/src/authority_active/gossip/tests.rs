// Copyright (c) 2022, Mysten Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

use super::*;
use crate::authority_active::gossip::configurable_batch_action_client::{
    init_configurable_authorities, BatchAction, ConfigurableBatchActionClient,
};
use crate::authority_active::MAX_RETRY_DELAY_MS;
use crate::gateway_state::GatewayMetrics;
use std::collections::BTreeMap;
use std::time::Duration;
use tokio::task::JoinHandle;

#[tokio::test(flavor = "current_thread", start_paused = true)]
pub async fn test_gossip_plain() {
    let action_sequence = vec![
        BatchAction::EmitUpdateItem(),
        BatchAction::EmitUpdateItem(),
        BatchAction::EmitUpdateItem(),
    ];

    let (clients, states, digests) = init_configurable_authorities(action_sequence).await;

    let _active_authorities = start_gossip_process(states.clone(), clients.clone()).await;
    tokio::time::sleep(Duration::from_secs(20)).await;

    // Expected outcome of gossip: each digest's tx signature and cert is now on every authority.
    let clients_final: Vec<_> = clients.values().collect();
    for client in clients_final.iter() {
        for digest in &digests {
            let result1 = client
                .handle_transaction_info_request(TransactionInfoRequest {
                    transaction_digest: digest.transaction,
                })
                .await;

            assert!(result1.is_ok());
            let result = result1.unwrap();
            let found_cert = result.certified_transaction.is_some();
            assert!(found_cert);
        }
    }
}

#[tokio::test(flavor = "current_thread", start_paused = true)]
pub async fn test_gossip_error() {
    let action_sequence = vec![BatchAction::EmitError(), BatchAction::EmitUpdateItem()];

    let (clients, states, digests) = init_configurable_authorities(action_sequence).await;

    let _active_authorities = start_gossip_process(states.clone(), clients.clone()).await;
    // failure back-offs were set from the errors
    tokio::time::sleep(Duration::from_millis(MAX_RETRY_DELAY_MS)).await;

    let clients_final: Vec<_> = clients.values().collect();
    for client in clients_final.iter() {
        for digest in &digests {
            let result1 = client
                .handle_transaction_info_request(TransactionInfoRequest {
                    transaction_digest: digest.transaction,
                })
                .await;

            assert!(result1.is_ok());
            let result = result1.unwrap();
            let found_cert = result.certified_transaction.is_some();
            assert!(found_cert);
        }
    }
}

#[tokio::test(flavor = "current_thread", start_paused = true)]
pub async fn test_gossip_after_revert() {
    let action_sequence = vec![BatchAction::EmitUpdateItem(), BatchAction::EmitUpdateItem()];
    let (clients, states, digests) = init_configurable_authorities(action_sequence).await;

    // There are 2 transactions:
    // 1. For the first transaction, only one validator reverts it.
    // 2. For the second transaction, all validators revert it.
    for state in &states {
        if state.get_transaction(digests[0].transaction).await.is_ok() {
            state
                .database
                .revert_state_update(&digests[0].transaction)
                .unwrap();
            break;
        }
    }
    for state in &states {
        if state.get_transaction(digests[1].transaction).await.is_ok() {
            state
                .database
                .revert_state_update(&digests[1].transaction)
                .unwrap();
        }
    }

    let _active_authorities = start_gossip_process(states.clone(), clients.clone()).await;
    tokio::time::sleep(Duration::from_secs(20)).await;

    let clients_final: Vec<_> = clients.values().collect();
    for client in clients_final.iter() {
        let result = client
            .handle_transaction_info_request(TransactionInfoRequest {
                transaction_digest: digests[0].transaction,
            })
            .await
            .unwrap();
        assert!(result.certified_transaction.is_some());
        let result = client
            .handle_transaction_info_request(TransactionInfoRequest {
                transaction_digest: digests[1].transaction,
            })
            .await
            .unwrap();
        assert!(result.certified_transaction.is_none());
    }
}

async fn start_gossip_process(
    states: Vec<Arc<AuthorityState>>,
    clients: BTreeMap<AuthorityName, ConfigurableBatchActionClient>,
) -> Vec<JoinHandle<()>> {
    let mut active_authorities = Vec::new();

    // Start active processes.
    for state in states {
        let inner_clients = clients.clone();

        let handle = tokio::task::spawn(async move {
            let active_state = Arc::new(
                ActiveAuthority::new_with_ephemeral_follower_store(
                    state,
                    inner_clients,
                    GatewayMetrics::new_for_tests(),
                )
                .unwrap(),
            );
            active_state.spawn_gossip_process(3).await;
        });
        active_authorities.push(handle);
    }

    active_authorities
}
