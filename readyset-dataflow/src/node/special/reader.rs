use std::collections::HashSet;
use std::time::SystemTime;

use dataflow_expression::ReaderProcessing;
use failpoint_macros::failpoint;
use metrics::histogram;
use readyset_client::metrics::recorded;
use readyset_client::{KeyColumnIdx, KeyComparison, ViewPlaceholder};
#[cfg(feature = "failure_injection")]
use readyset_util::failpoints;
use serde::{Deserialize, Serialize};
use tracing::{debug, trace, warn};

use crate::backlog;
use crate::prelude::*;
use crate::redis_notifier;

#[derive(Serialize, Deserialize)]
pub struct Reader {
    for_node: NodeIndex,
    index: Option<Index>,

    /// Operations to perform on the result set after the rows are returned from the lookup
    reader_processing: ReaderProcessing,

    /// Vector of (placeholder_number, key_column_index). The placeholder_number corresponds to
    /// where the placeholder appears in the SQL query and the key_column_index corresponds to the
    /// key column index in the reader state.
    ///
    /// The data is stored in this manner instead of in a Hashmap to support ordered iteration.
    placeholder_map: Vec<(ViewPlaceholder, KeyColumnIdx)>,
}

impl Clone for Reader {
    fn clone(&self) -> Self {
        Reader {
            for_node: self.for_node,
            reader_processing: self.reader_processing.clone(),
            index: self.index.clone(),
            placeholder_map: self.placeholder_map.clone(),
        }
    }
}

impl Reader {
    pub fn new(for_node: NodeIndex, reader_processing: ReaderProcessing) -> Self {
        Reader {
            for_node,
            reader_processing,
            index: None,
            placeholder_map: Default::default(),
        }
    }

    pub fn shard(&mut self, _: usize) {}

    pub fn is_for(&self) -> NodeIndex {
        self.for_node
    }

    pub(in crate::node) fn take(&mut self) -> Self {
        Self {
            for_node: self.for_node,
            reader_processing: self.reader_processing.clone(),
            index: self.index.clone(),
            placeholder_map: self.placeholder_map.clone(),
        }
    }

    pub fn is_materialized(&self) -> bool {
        self.index.is_some()
    }

    pub fn index(&self) -> Option<&Index> {
        self.index.as_ref()
    }

    pub fn key(&self) -> Option<&[usize]> {
        self.index.as_ref().map(|s| &s.columns[..])
    }

    pub fn index_type(&self) -> Option<IndexType> {
        self.index.as_ref().map(|index| index.index_type)
    }

    #[must_use]
    pub fn with_index(mut self, index: &Index) -> Self {
        self.set_index(index);
        self
    }

    pub fn set_index(&mut self, index: &Index) {
        if let Some(ref m_index) = self.index {
            debug_assert_eq!(m_index, index);
        } else {
            self.index = Some(index.clone());
        }
    }

    /// Sets the placeholder to column mapping if it is not already set.
    ///
    /// We do not currently support multiple mappings from placeholders to key columns. That would
    /// require a method for resolving which mapping should be used for each query.
    ///
    /// This method will need to be implemented before using the same reader for functionally
    /// identical queries with different parameter orderings (e.g., 'SELECT * FROM t WHERE a = ?
    /// AND b = ?' and 'SELECT * FROM t WHERE b = ? AND a = ?')
    pub fn set_mapping(&mut self, mapping: Vec<(ViewPlaceholder, KeyColumnIdx)>) {
        if !self.placeholder_map.is_empty() {
            debug_assert_eq!(self.placeholder_map, mapping);
        } else {
            self.placeholder_map = mapping
        }
    }

    /// Returns the mapping from placeholder to reader key column. There is exactly one value for
    /// each reader key column in the map
    pub fn mapping(&self) -> &[(ViewPlaceholder, KeyColumnIdx)] {
        self.placeholder_map.as_ref()
    }

    #[failpoint(failpoints::READER_HANDLE_PACKET)]
    pub(in crate::node) fn process(
        &mut self,
        m: &mut Option<Packet>,
        publish: bool,
        state: &mut backlog::WriteHandle,
        cache_name: Option<&str>,
    ) {
        let m = m.as_mut().unwrap();
        m.handle_trace(
            |trace| match SystemTime::now().duration_since(trace.start) {
                Ok(d) => {
                    histogram!(recorded::PACKET_WRITE_PROPAGATION_TIME)
                        .record(d.as_micros() as f64);
                }
                Err(e) => {
                    warn!(error = %e, "Write latency trace failed");
                }
            },
        );
        // make sure we don't fill a partial materialization
        // hole with incomplete (i.e., non-replay) state.
        if let Packet::Update(m) = m {
            let data = m.data_mut();
            trace!(?data, "reader received regular message");
            if state.is_partial() {
                data.retain(|row| {
                    match state.contains_record(&row[..]) {
                        Ok(false) => {
                            // row would miss in partial state.
                            // leave it blank so later lookup triggers replay.
                            trace!(?row, "dropping row that hit partial hole");
                            false
                        }
                        Ok(true) => {
                            // state is already present,
                            // so we can safely keep it up to date.
                            true
                        }
                        Err(reader_map::Error::NotPublished) => {
                            // If we got here it means we got a `NotReady` error type. This is
                            // impossible, because when readers are instantiated we issue a
                            // commit to the underlying map, which makes it Ready.
                            unreachable!(
                                "somehow found a NotReady reader even though we've
                                    already initialized it with a commit"
                            )
                        }
                        Err(reader_map::Error::Destroyed) => {
                            unreachable!(
                                "somehow map was destroyed but we hold a mutable reference"
                            )
                        }
                    }
                });
            }
        } else if state.is_partial() {
            // it *can* happen that multiple readers miss (and thus request replay for) the
            // same hole at the same time. we need to make sure that we ignore any such
            // duplicated replay.
            let data = m.data_mut();
            trace!(?data, "reader received replay");
            data.retain(|row| {
                match state.contains_record(&row[..]) {
                    Ok(false) => {
                        // filling a hole with replay -- ok
                        true
                    }
                    Ok(true) => {
                        trace!(?row, "reader dropping row that hit already-filled hole");
                        // a given key should only be replayed to once!
                        false
                    }
                    Err(_) => {
                        // state has not yet been published, which means it's new,
                        // which means there are no readers, which means no
                        // requests for replays have been issued by readers, which
                        // means no duplicates can be received.
                        true
                    }
                }
            });
        }

        // Extract key column values BEFORE take_data() consumes them.
        // These represent the specific parameter values that were affected
        // (e.g., the exact conversation_id that changed), enabling per-parameter
        // granular invalidation instead of invalidating the entire parameterized cache.
        let key_values = if publish && cache_name.is_some() && !self.placeholder_map.is_empty() {
            self.extract_key_column_values(m)
        } else {
            vec![]
        };

        state.add(m.take_data());

        if publish {
            // TODO: skip if we didn't modify anything (inc. ts)
            state.publish();

            // Tier 1: Notify Redis that this reader's cached data has changed
            if let Some(name) = cache_name {
                redis_notifier::notify_invalidation(name, key_values);
            }
        }
    }

    /// Extract unique key column values from the packet's data.
    ///
    /// For a parameterized query like `WHERE conversation_id = $1`, the placeholder_map
    /// tells us which column index holds `conversation_id`. We extract those values from
    /// each record, deduplicate, and return them as pipe-delimited strings (one per unique
    /// key combination). This enables the Redis bridge to invalidate only the specific
    /// parameter values that were affected, not the entire parameterized cache.
    fn extract_key_column_values(&self, packet: &mut Packet) -> Vec<String> {
        let data = packet.data_mut();

        // Collect key column indices sorted by placeholder index.
        // For `WHERE a = $1 AND b = $2`, this gives us columns for $1 then $2 in order.
        let mut key_indices: Vec<(usize, usize)> = Vec::new(); // (placeholder_idx, column_idx)
        for (placeholder, col_idx) in &self.placeholder_map {
            match placeholder {
                ViewPlaceholder::OneToOne(placeholder_idx, _) => {
                    key_indices.push((*placeholder_idx as usize, *col_idx));
                }
                _ => {} // Skip Generated, Between, PageNumber for now
            }
        }

        if key_indices.is_empty() {
            return vec![];
        }

        key_indices.sort_by_key(|&(pi, _)| pi);
        let col_indices: Vec<usize> = key_indices.iter().map(|&(_, ci)| ci).collect();

        // Extract unique key value combinations from the records
        let mut seen: HashSet<String> = HashSet::new();
        let mut result: Vec<String> = Vec::new();

        for record in data.iter() {
            let row = record.row();
            let values: Vec<String> = col_indices
                .iter()
                .filter_map(|&i| row.get(i).map(|v| format!("{}", v)))
                .collect();

            if values.len() == col_indices.len() {
                let key = values.join("|");
                if !seen.contains(&key) {
                    seen.insert(key.clone());
                    result.push(key);
                }
            }
        }

        if !result.is_empty() {
            debug!(
                count = result.len(),
                sample = ?result.first(),
                "Extracted key column values for granular invalidation"
            );
        }

        result
    }

    /// Map evicted reader keys into the same pipe-delimited, placeholder-ordered
    /// strings that [`Self::extract_key_column_values`] produces for the Update
    /// path, so eviction-sourced invalidations hit the same per-param dep sets
    /// (`rs:pdeps:{cache}:{param_key}`).
    ///
    /// `key_columns` is the reader's partial index columns (the order in which
    /// values appear inside each `KeyComparison`). Any shape we cannot map
    /// faithfully (range evictions, non-OneToOne placeholders, column
    /// mismatches) returns an empty vec, which the notifier treats as a broad
    /// (whole-cache) invalidation — strictly safe, just less granular.
    pub(in crate::node) fn eviction_key_values(
        &self,
        key_columns: &[usize],
        keys: &[KeyComparison],
    ) -> Vec<String> {
        // (placeholder_idx, position of the value inside the key tuple)
        let mut mapped: Vec<(usize, usize)> = Vec::new();
        for (placeholder, col_idx) in &self.placeholder_map {
            match placeholder {
                ViewPlaceholder::OneToOne(placeholder_idx, _) => {
                    match key_columns.iter().position(|c| c == col_idx) {
                        Some(pos) => mapped.push((*placeholder_idx as usize, pos)),
                        None => return vec![],
                    }
                }
                _ => return vec![], // Generated, Between, PageNumber -> broad
            }
        }
        if mapped.is_empty() {
            return vec![];
        }
        mapped.sort_by_key(|&(pi, _)| pi);

        let mut seen: HashSet<String> = HashSet::new();
        let mut result: Vec<String> = Vec::new();
        for key in keys {
            match key {
                KeyComparison::Equal(values) => {
                    let vs: Vec<String> = mapped
                        .iter()
                        .filter_map(|&(_, pos)| values.get(pos).map(|v| format!("{}", v)))
                        .collect();
                    if vs.len() != mapped.len() {
                        return vec![];
                    }
                    let joined = vs.join("|");
                    if seen.insert(joined.clone()) {
                        result.push(joined);
                    }
                }
                KeyComparison::Range(_) => return vec![],
            }
        }
        result
    }

    /// Get a reference to the reader's post lookup.
    pub fn reader_processing(&self) -> &ReaderProcessing {
        &self.reader_processing
    }
}

#[cfg(test)]
mod tests {
    use readyset_sql::ast::BinaryOperator;
    use vec1::vec1;

    use super::*;

    fn reader_with_mapping(mapping: Vec<(ViewPlaceholder, KeyColumnIdx)>) -> Reader {
        let mut r = Reader::new(NodeIndex::new(0), ReaderProcessing::default());
        r.set_mapping(mapping);
        r
    }

    #[test]
    fn eviction_key_values_single_column() {
        let r = reader_with_mapping(vec![(ViewPlaceholder::OneToOne(1, BinaryOperator::Equal), 3)]);
        let keys = [KeyComparison::Equal(vec1![DfValue::from("abc")])];
        assert_eq!(r.eviction_key_values(&[3], &keys), vec!["abc".to_string()]);
    }

    #[test]
    fn eviction_key_values_composite_reordered() {
        // $1 -> row col 7, $2 -> row col 2; reader index columns are [2, 7],
        // so key tuples arrive as (col2_val, col7_val) and must be emitted in
        // placeholder order "col7_val|col2_val".
        let r = reader_with_mapping(vec![
            (ViewPlaceholder::OneToOne(1, BinaryOperator::Equal), 7),
            (ViewPlaceholder::OneToOne(2, BinaryOperator::Equal), 2),
        ]);
        let keys = [KeyComparison::Equal(vec1![
            DfValue::from(0),
            DfValue::from("not_sent")
        ])];
        assert_eq!(
            r.eviction_key_values(&[2, 7], &keys),
            vec!["not_sent|0".to_string()]
        );
    }

    #[test]
    fn eviction_key_values_dedups() {
        let r = reader_with_mapping(vec![(ViewPlaceholder::OneToOne(1, BinaryOperator::Equal), 0)]);
        let keys = [
            KeyComparison::Equal(vec1![DfValue::from("x")]),
            KeyComparison::Equal(vec1![DfValue::from("x")]),
            KeyComparison::Equal(vec1![DfValue::from("y")]),
        ];
        assert_eq!(
            r.eviction_key_values(&[0], &keys),
            vec!["x".to_string(), "y".to_string()]
        );
    }

    #[test]
    fn eviction_key_values_range_falls_back_to_broad() {
        let r = reader_with_mapping(vec![(ViewPlaceholder::OneToOne(1, BinaryOperator::Equal), 0)]);
        let keys = [KeyComparison::from_range(
            &(vec1![DfValue::from(1)]..=vec1![DfValue::from(5)]),
        )];
        assert!(r.eviction_key_values(&[0], &keys).is_empty());
    }

    #[test]
    fn eviction_key_values_unmappable_column_falls_back_to_broad() {
        // Placeholder maps to a column that is not part of the evicted index.
        let r = reader_with_mapping(vec![(ViewPlaceholder::OneToOne(1, BinaryOperator::Equal), 9)]);
        let keys = [KeyComparison::Equal(vec1![DfValue::from("abc")])];
        assert!(r.eviction_key_values(&[3], &keys).is_empty());
    }

    #[test]
    fn eviction_key_values_no_placeholders_is_broad() {
        let r = reader_with_mapping(vec![]);
        let keys = [KeyComparison::Equal(vec1![DfValue::from("abc")])];
        assert!(r.eviction_key_values(&[0], &keys).is_empty());
    }
}
