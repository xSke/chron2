use std::{collections::HashSet, pin::Pin};

use anyhow::anyhow;
use base64::Engine;
use flate2::bufread::GzDecoder;
use futures::{
    channel::mpsc::{unbounded, UnboundedSender},
    FutureExt, Sink, SinkExt, Stream, StreamExt,
};
use serde::{Deserialize, Serialize};
use time::OffsetDateTime;
use tokio_tungstenite::{connect_async, tungstenite::Message};
use tracing::{error, info};

#[derive(Deserialize, Debug)]
pub struct SerializedPusherMessage {
    pub channel: Option<String>,
    pub event: String,
    pub data: String,
}

#[derive(Debug)]
pub struct PusherMessage {
    pub timestamp: OffsetDateTime,
    pub channel: String,
    pub event: String,
    pub data: String,
    pub payload: Option<serde_json::Value>,
}

#[derive(Serialize, Debug)]
struct PusherSubscribeCommand {
    auth: String,
    channel: String,
}

#[derive(Serialize, Debug)]
struct PusherCommand<T> {
    event: String,
    data: T,
}

fn subscribe_command(channel: String) -> Message {
    let cmd = PusherCommand {
        event: "pusher:subscribe".to_string(),
        data: PusherSubscribeCommand {
            auth: "".to_string(),
            channel: channel,
        },
    };
    let json = serde_json::to_string(&cmd).unwrap();
    Message::Text(json)
}

fn parse_pusher_msg(data: &str) -> anyhow::Result<SerializedPusherMessage> {
    Ok(serde_json::from_str::<SerializedPusherMessage>(&data)?)
}

#[derive(Deserialize)]
struct WrappedPayload {
    message: String,
}

pub fn decode_payload(payload: &str) -> anyhow::Result<serde_json::Value> {
    let inner = serde_json::from_str::<WrappedPayload>(payload)?;

    if let Ok(decoded) = base64::engine::general_purpose::STANDARD.decode(&inner.message) {
        let gzip = GzDecoder::new(&decoded[..]);
        return Ok(serde_json::from_reader(gzip)?);
    } else if let Ok(value) = serde_json::from_str::<serde_json::Value>(&inner.message) {
        Ok(value)
    } else {
        Ok(serde_json::from_str::<serde_json::Value>(payload)?)
    }
}

async fn single_ws_worker(
    pusher_key: &str,
    incoming: Pin<&mut impl Sink<(OffsetDateTime, Message)>>,
    mut receiver: Pin<&mut impl Stream<Item = String>>,
) -> anyhow::Result<()> {
    info!("connecting to pusher...");
    let url = format!("wss://ws-us3.pusher.com/app/{}?protocol=7", pusher_key);
    let (ws, _) = connect_async(&url).await?;

    let (mut ws_tx, mut ws_rx) = ws.split();

    // wait until we're ready to subscribe
    while let Some(Ok(Message::Text(msg))) = ws_rx.next().await {
        if msg.contains("pusher:connection_established") {
            break;
        }
    }

    let mut ws_rx = Box::pin(ws_rx);
    let mut incoming = Box::pin(incoming);

    let mut subscribed_channels = HashSet::new();

    loop {
        let mut rx_fut = ws_rx.next().fuse();

        futures::select! {
            msg = rx_fut => {
                let timestamp = OffsetDateTime::now_utc();
                if let Some(Ok(msg)) = msg {
                    // we wanna defer parsing this to the other end of the queue to maximize recv throughput
                    incoming.send((timestamp, msg)).await.map_err(|_| anyhow!("could not send"))?;
                } else {
                    break;
                }
            },
            msg = receiver.next().fuse() => {
                if let Some(channel) = msg {
                    if subscribed_channels.insert(channel.clone()) {
                        info!("subscribing to {}", &channel);
                        ws_tx.send(subscribe_command(channel)).await?;
                    }
                }
            }
        }
    }

    Ok(())
}

#[derive(Clone)]
pub struct PusherHandle {
    subscribe_tx: UnboundedSender<String>,
}

impl PusherHandle {
    pub async fn subscribe(&mut self, channel: String) -> anyhow::Result<()> {
        Ok(self.subscribe_tx.send(channel).await?)
    }
}

pub async fn pusher_connect(
    pusher_key: String,
) -> anyhow::Result<(PusherHandle, impl Stream<Item = PusherMessage>)> {
    let (sub_tx, sub_rx) = unbounded();
    let (recv_tx, recv_rx) = unbounded();

    let mut send_rx = Box::pin(sub_rx);
    let mut recv_tx = Box::pin(recv_tx);

    let mut sub_tx_2 = sub_tx.clone();

    tokio::spawn(async move {
        loop {
            // queue up subscriptions for when it reconnects
            sub_tx_2.send("ticker".to_string()).await.unwrap();
            sub_tx_2.send("sim-data".to_string()).await.unwrap();
            sub_tx_2.send("temporal".to_string()).await.unwrap();

            if let Err(e) = single_ws_worker(&pusher_key, recv_tx.as_mut(), send_rx.as_mut()).await
            {
                error!("error in pusher worker: {}", e);
            }
        }
    });

    let out_stream = recv_rx.filter_map(|(timestamp, msg)| async move {
        match msg {
            Message::Text(text) => parse_pusher_msg(&text).ok().map(|x| {
                // dbg!(&x);
                let payload = decode_payload(&x.data);
                PusherMessage {
                    timestamp,
                    channel: x.channel.unwrap_or_default(),
                    event: x.event,
                    data: x.data,
                    payload: payload.ok(),
                }
            }),
            _ => None,
        }
    });

    Ok((
        PusherHandle {
            subscribe_tx: sub_tx,
        },
        out_stream,
    ))
}
