use crate::proto::raftpb::LogEntry;

#[derive(Message, Clone)]
pub struct Log {
    #[prost(message, repeated, tag = "1")]
    pub entries: Vec<LogEntry>,
    #[prost(uint64, tag = "2")]
    pub offset:  u64,
}

impl Log {
    pub fn new() -> Self {
        Self {
            entries: vec![LogEntry::default()],
            offset:  0,
        }
    }
    pub fn first(&self) -> &LogEntry {
        &self.entries[0]
    }
    pub fn last(&self) -> &LogEntry {
        self.entries.last().unwrap()
    }
    pub fn len(&self) -> u64 {
        self.entries.len() as u64 + self.offset
    }
    pub fn get(&self, index: u64) -> Option<&LogEntry> {
        self.entries
            .get(index.checked_sub(self.offset)? as usize)
    }
    pub fn push(&mut self, entry: LogEntry) {
        assert!(entry.term >= self.last().term);
        self.entries.push(entry);
    }
    pub fn append(&mut self, other: &mut Vec<LogEntry>) {
        if other.is_empty() {
            return;
        }

        assert!(other[0].term >= self.last().term);
        self.entries.append(other)
    }
    pub fn truncate(&mut self, len: u64) {
        assert!(len > self.offset);
        self.entries
            .truncate((len - self.offset) as usize);
    }
    pub fn tail(&self, from: u64) -> impl Iterator<Item = &LogEntry> {
        assert!(from > self.offset);
        self.entries
            .iter()
            .skip((from - self.offset) as usize)
    }
    pub fn back(&self, with: u64) -> Option<u64> {
        let next = self
            .entries
            .binary_search_by_key(&(with + 1), |entry| entry.term)
            .unwrap_or_else(|index| index);

        if next <= 1 {
            return None;
        }

        let index = next - 1;
        (self.entries[index].term == with).then(|| index as u64 + self.offset)
    }
    pub fn offset(&mut self, to: u64) {
        assert!((self.offset + 1..self.len()).contains(&to));
        self.entries
            .drain(0..(to - self.offset) as usize);

        self.entries[0].data.clear();
        self.offset = to;
    }
    pub fn reset(&mut self, term: u64, index: u64) {
        assert!(term >= self.first().term);
        self.entries = vec![LogEntry::new(Vec::new(), term)];

        assert!(index > self.offset);
        self.offset = index;
    }
}
