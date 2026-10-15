#![cfg(not(oneshot_loom))]

use oneshot::{Receiver, Sender, channel};

#[test]
fn test_raw_sender() {
    let (sender, receiver) = channel::<u32>();
    let raw = sender.into_raw();
    // SAFETY: `raw` comes from `Sender::into_raw` for the same message type.
    let recreated = unsafe { Sender::<u32>::from_raw(raw) };
    recreated
        .send(100)
        .unwrap_or_else(|e| panic!("error sending after into_raw/from_raw roundtrip: {e}"));
    assert_eq!(receiver.try_recv(), Ok(100))
}

#[test]
fn test_raw_receiver() {
    let (sender, receiver) = channel::<u32>();
    let raw = receiver.into_raw();
    sender.send(100).unwrap();
    // SAFETY: `raw` comes from `Sender::into_raw` for the same message type.
    let recreated = unsafe { Receiver::<u32>::from_raw(raw) };
    assert_eq!(
        recreated
            .try_recv()
            .unwrap_or_else(|e| panic!("error receiving after into_raw/from_raw roundtrip: {e}")),
        100
    )
}

#[test]
fn test_raw_sender_and_receiver() {
    let (sender, receiver) = channel::<u32>();
    let raw_receiver = receiver.into_raw();
    let raw_sender = sender.into_raw();

    // SAFETY: `raw_sender` comes from `Sender::into_raw` for the same message type.
    let recreated_sender = unsafe { Sender::<u32>::from_raw(raw_sender) };
    recreated_sender.send(100).unwrap();

    // SAFETY: `raw_receiver` comes from `Receiver::into_raw` for the same message type.
    let recreated_receiver = unsafe { Receiver::<u32>::from_raw(raw_receiver) };
    assert_eq!(
        recreated_receiver
            .try_recv()
            .unwrap_or_else(|e| panic!("error receiving after into_raw/from_raw roundtrip: {e}")),
        100
    )
}
