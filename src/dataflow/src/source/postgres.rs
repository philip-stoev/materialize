// Copyright Materialize, Inc. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::convert::TryInto;
use std::error::Error;
use std::io::SeekFrom;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use anyhow::{anyhow, bail};
use async_trait::async_trait;
use futures::StreamExt;
use lazy_static::lazy_static;

use postgres_protocol::message::backend::{
    LogicalReplicationMessage, ReplicationMessage, Tuple, TupleData,
};
use tokio::fs::File;
use tokio::io::{AsyncBufReadExt, AsyncSeekExt, AsyncWriteExt, BufReader, BufWriter};
use tokio_postgres::config::{Config, ReplicationMode};
use tokio_postgres::error::{DbError, Severity, SqlState};
use tokio_postgres::replication::LogicalReplicationStream;
use tokio_postgres::types::PgLsn;
use tokio_postgres::{Client, SimpleQueryMessage};

use crate::source::{SimpleSource, SourceError, SourceTransaction, Timestamper};
use dataflow_types::{PostgresSourceConnector, SourceErrorDetails};
use repr::{Datum, Row};

lazy_static! {
    /// Postgres epoch is 2000-01-01T00:00:00Z
    static ref PG_EPOCH: SystemTime = UNIX_EPOCH + Duration::from_secs(946_684_800);
}

/// Information required to sync data from Postgres
pub struct PostgresSourceReader {
    /// Used to produce useful error messages
    source_name: String,
    connector: PostgresSourceConnector,
    /// Our cursor into the WAL
    lsn: PgLsn,
}

enum ReplicationError {
    Recoverable(anyhow::Error),
    Fatal(anyhow::Error),
}

macro_rules! try_fatal {
    ($expr:expr $(,)?) => {
        match $expr {
            Ok(val) => val,
            Err(err) => return Err(ReplicationError::Fatal(err.into())),
        }
    };
}
macro_rules! try_recoverable {
    ($expr:expr $(,)?) => {
        match $expr {
            Ok(val) => val,
            Err(err) => return Err(ReplicationError::Recoverable(err.into())),
        }
    };
}

impl PostgresSourceReader {
    /// Constructs a new instance
    pub fn new(source_name: String, connector: PostgresSourceConnector) -> Self {
        Self {
            source_name,
            connector,
            lsn: 0.into(),
        }
    }

    /// Starts a replication connection to the upstream database
    async fn connect_replication(&self) -> Result<Client, anyhow::Error> {
        let mut config: Config = self.connector.conn.parse()?;
        let tls = postgres_util::make_tls(&config)?;
        let (client, conn) = config
            .replication_mode(ReplicationMode::Logical)
            .connect(tls)
            .await?;
        tokio::spawn(conn);
        Ok(client)
    }

    /// Creates the replication slot and produces the initial snapshot of the data
    ///
    /// After the initial snapshot has been produced it returns the name of the created slot and
    /// the LSN at which we should start the replication stream at.
    async fn produce_snapshot(
        &mut self,
        snapshot_tx: &mut SourceTransaction<'_>,
        buffer: &mut BufReader<BufWriter<File>>,
    ) -> Result<(), ReplicationError> {
        let client = try_recoverable!(self.connect_replication().await);

        // We're initialising this source so any previously existing slot must be removed and
        // re-created. Once we have data persistence we will be able to reuse slots across restarts
        let _ = client
            .simple_query(&format!(
                "DROP_REPLICATION_SLOT {:?}",
                &self.connector.slot_name
            ))
            .await;

        // Get all the relevant tables for this publication
        let publication_tables = try_recoverable!(
            postgres_util::publication_info(&self.connector.conn, &self.connector.publication)
                .await
        );

        // Start a transaction and immediatelly create a replication slot with the USE SNAPSHOT
        // directive. This makes the starting point of the slot and the snapshot of the transaction
        // identical.
        try_recoverable!(
            client
                .simple_query("BEGIN READ ONLY ISOLATION LEVEL REPEATABLE READ;")
                .await
        );

        let slot_query = format!(
            r#"CREATE_REPLICATION_SLOT {:?} LOGICAL "pgoutput" USE_SNAPSHOT"#,
            &self.connector.slot_name
        );
        let slot_row = try_recoverable!(client.simple_query(&slot_query).await)
            .into_iter()
            .next()
            .and_then(|msg| match msg {
                SimpleQueryMessage::Row(row) => Some(row),
                _ => None,
            })
            .ok_or_else(|| {
                ReplicationError::Recoverable(anyhow!(
                    "empty result after creating replication slot"
                ))
            })?;

        // Store the lsn at which we will need to start the replication stream from
        let consistent_point = try_recoverable!(slot_row
            .get("consistent_point")
            .ok_or_else(|| anyhow!("missing expected column: `consistent_point`")));
        self.lsn = try_fatal!(consistent_point
            .parse()
            .or_else(|_| Err(anyhow!("invalid lsn"))));

        for info in publication_tables {
            // TODO(petrosagg): use a COPY statement here for more efficient network transfer
            let data = try_recoverable!(
                client
                    .simple_query(&format!(
                        "SELECT * FROM {:?}.{:?}",
                        info.namespace, info.name
                    ))
                    .await
            );
            for msg in data {
                if let SimpleQueryMessage::Row(row) = msg {
                    let mut mz_row = Row::default();
                    let rel_id: Datum = (info.rel_id as i32).into();
                    mz_row.push(rel_id);
                    mz_row.push_list((0..row.len()).map(|n| {
                        let a: Datum = row.get(n).into();
                        a
                    }));
                    try_recoverable!(snapshot_tx.insert(mz_row.clone()).await);
                    try_fatal!(buffer.write(&try_fatal!(bincode::serialize(&mz_row))).await);
                    try_fatal!(buffer.write(&[b'\n']).await);
                }
            }
        }
        try_recoverable!(client.simple_query("COMMIT;").await);
        Ok(())
    }

    /// Reverts a failed snapshot by deleting any processed rows from the dataflow.
    async fn revert_snapshot(
        &self,
        snapshot_tx: &mut SourceTransaction<'_>,
        buffer: &mut BufReader<BufWriter<File>>,
    ) -> Result<(), anyhow::Error> {
        buffer.seek(SeekFrom::Start(0)).await?;
        loop {
            let mut bytes = Vec::new();
            match buffer.read_until(b'\n', &mut bytes).await? {
                0 => break,
                _ => {
                    let mz_row: Row = bincode::deserialize(&bytes)?;
                    snapshot_tx.delete(mz_row).await?;
                }
            }
        }

        Ok(())
    }

    /// Converts a Tuple received in the replication stream into a Row instance. The logical
    /// replication protocol doesn't use the binary encoding for column values so contrary to the
    /// initial snapshot here we need to parse the textual form of each column.
    fn row_from_tuple(&mut self, rel_id: u32, tuple: &Tuple) -> Result<Row, anyhow::Error> {
        let mut row = Row::default();

        let rel_id: Datum = (rel_id as i32).into();
        row.push(rel_id);
        row.push_list_with(|packer| {
            for val in tuple.tuple_data() {
                let datum = match val {
                    TupleData::Null => Datum::Null,
                    TupleData::UnchangedToast => bail!("Unsupported TOAST value"),
                    TupleData::Text(b) => std::str::from_utf8(&b)?.into(),
                };
                packer.push(datum);
            }
            Ok(())
        })?;

        Ok(row)
    }

    async fn produce_replication(
        &mut self,
        timestamper: &Timestamper,
    ) -> Result<(), ReplicationError> {
        use ReplicationError::*;

        let client = try_recoverable!(self.connect_replication().await);

        let query = format!(
            r#"START_REPLICATION SLOT "{name}" LOGICAL {lsn}
              ("proto_version" '1', "publication_names" '{publication}')"#,
            name = &self.connector.slot_name,
            lsn = self.lsn.to_string(),
            publication = self.connector.publication
        );
        let copy_stream = try_recoverable!(client.copy_both_simple(&query).await);

        let stream = LogicalReplicationStream::new(copy_stream);
        tokio::pin!(stream);

        let mut last_keepalive = Instant::now();
        let mut inserts = vec![];
        let mut deletes = vec![];
        while let Some(item) = stream.next().await {
            use ReplicationMessage::*;
            // The upstream will periodically request keepalive responses by setting the reply field
            // to 1. However, we cannot rely on these messages arriving on time. For example, when
            // the upstream is sending a big transaction its keepalive messages are queued and can
            // be delayed arbitrarily.  Therefore, we also make sure to send a proactive keepalive
            // every 30 seconds.
            //
            // See: https://www.postgresql.org/message-id/CAMsr+YE2dSfHVr7iEv1GSPZihitWX-PMkD9QALEGcTYa+sdsgg@mail.gmail.com
            if matches!(item, Ok(PrimaryKeepAlive(ref k)) if k.reply() == 1)
                || last_keepalive.elapsed() > Duration::from_secs(30)
            {
                let ts: i64 = PG_EPOCH
                    .elapsed()
                    .expect("system clock set earlier than year 2000!")
                    .as_micros()
                    .try_into()
                    .expect("software more than 200k years old, consider updating");

                try_recoverable!(
                    stream
                        .as_mut()
                        .standby_status_update(self.lsn, self.lsn, self.lsn, ts, 0)
                        .await
                );
                last_keepalive = Instant::now();
            }
            match item {
                Ok(XLogData(xlog_data)) => {
                    use LogicalReplicationMessage::*;

                    match xlog_data.data() {
                        Begin(_) => {
                            if !inserts.is_empty() || !deletes.is_empty() {
                                return Err(Fatal(anyhow!(
                                    "got BEGIN statement after uncommitted data"
                                )));
                            }
                        }
                        Insert(insert) => {
                            let rel_id = insert.rel_id();
                            let row = try_fatal!(self.row_from_tuple(rel_id, insert.tuple()));
                            inserts.push(row);
                        }
                        Update(update) => {
                            let rel_id = update.rel_id();
                            let old_tuple = try_fatal!(update
                                .old_tuple()
                                .ok_or_else(|| anyhow!("full row missing from update")));
                            let old_row = try_fatal!(self.row_from_tuple(rel_id, old_tuple));
                            deletes.push(old_row);

                            let new_row =
                                try_fatal!(self.row_from_tuple(rel_id, update.new_tuple()));
                            inserts.push(new_row);
                        }
                        Delete(delete) => {
                            let rel_id = delete.rel_id();
                            let old_tuple = try_fatal!(delete
                                .old_tuple()
                                .ok_or_else(|| anyhow!("full row missing from delete")));
                            let row = try_fatal!(self.row_from_tuple(rel_id, old_tuple));
                            deletes.push(row);
                        }
                        Commit(commit) => {
                            self.lsn = commit.end_lsn().into();

                            let tx = timestamper.start_tx().await;

                            for row in deletes.drain(..) {
                                try_fatal!(tx.delete(row).await);
                            }
                            for row in inserts.drain(..) {
                                try_fatal!(tx.insert(row).await);
                            }
                        }
                        Origin(_) | Relation(_) | Type(_) => (),
                        Truncate(_) => return Err(Fatal(anyhow!("source table got truncated"))),
                        // The enum is marked as non_exaustive. Better to be conservative here in
                        // case a new message is relevant to the semantics of our source
                        _ => return Err(Fatal(anyhow!("unexpected logical replication message"))),
                    }
                }
                // Handled above
                Ok(PrimaryKeepAlive(_)) => {}
                Err(err) => {
                    let recoverable = match err.source() {
                        Some(err) => {
                            match err.downcast_ref::<DbError>() {
                                Some(db_err) => {
                                    use Severity::*;
                                    // Connection and non-fatal errors
                                    db_err.code() == &SqlState::CONNECTION_EXCEPTION
                                        || db_err.code() == &SqlState::CONNECTION_DOES_NOT_EXIST
                                        || db_err.code() == &SqlState::CONNECTION_FAILURE
                                        || !matches!(
                                            db_err.parsed_severity(),
                                            Some(Error) | Some(Fatal) | Some(Panic)
                                        )
                                }
                                // IO errors
                                None => err.is::<std::io::Error>(),
                            }
                        }
                        // We have no information about what happened, it might be a fatal error or
                        // it might not. Unexpected errors can happen if the upstream crashes for
                        // example in which case we should retry.
                        //
                        // Therefore, we adopt a "recoverable unless proven otherwise" policy and
                        // keep retrying in the event of unexpected errors.
                        None => true,
                    };

                    if recoverable {
                        return Err(Recoverable(err.into()));
                    } else {
                        return Err(Fatal(err.into()));
                    }
                }
                // The enum is marked non_exaustive, better be conservative
                _ => return Err(Fatal(anyhow!("Unexpected replication message"))),
            }
        }
        Err(Recoverable(anyhow!("replication stream ended")))
    }
}

#[async_trait]
impl SimpleSource for PostgresSourceReader {
    /// The top-level control of the state machine and retry logic
    async fn start(mut self, timestamper: &Timestamper) -> Result<(), SourceError> {
        // Buffer rows from snapshot to retract and retry, if initial snapshot fails.
        // Postgres sources cannot proceed without a successful snapshot.
        {
            let mut snapshot_tx = timestamper.start_tx().await;
            loop {
                let file = File::from_std(tempfile::tempfile().map_err(|e| SourceError {
                    source_name: self.source_name.clone(),
                    error: SourceErrorDetails::FileIO(e.to_string()),
                })?);
                let mut buffer = BufReader::new(BufWriter::new(file));
                match self.produce_snapshot(&mut snapshot_tx, &mut buffer).await {
                    Ok(_) => {
                        log::info!(
                            "replication snapshot for source {} succeeded",
                            &self.source_name
                        );
                        break;
                    }
                    Err(ReplicationError::Recoverable(e)) => {
                        log::warn!(
                            "replication snapshot for source {} failed, retrying: {}",
                            &self.source_name,
                            e
                        );
                        self.revert_snapshot(&mut snapshot_tx, &mut buffer)
                            .await
                            .map_err(|e| SourceError {
                                source_name: self.source_name.clone(),
                                error: SourceErrorDetails::FileIO(e.to_string()),
                            })?;
                    }
                    Err(ReplicationError::Fatal(e)) => {
                        return Err(SourceError {
                            source_name: self.source_name,
                            error: SourceErrorDetails::Initialization(e.to_string()),
                        })
                    }
                }

                // TODO(petrosagg): implement exponential back-off
                tokio::time::sleep(Duration::from_secs(3)).await;
            }
        }

        loop {
            match self.produce_replication(timestamper).await {
                Ok(_) => log::info!("replication interrupted with no error"),
                Err(ReplicationError::Recoverable(e)) => {
                    log::info!("replication interrupted: {}", e)
                }
                Err(ReplicationError::Fatal(e)) => {
                    return Err(SourceError {
                        source_name: self.source_name,
                        error: SourceErrorDetails::FileIO(e.to_string()),
                    })
                }
            }

            // TODO(petrosagg): implement exponential back-off
            tokio::time::sleep(Duration::from_secs(3)).await;
        }
    }
}
